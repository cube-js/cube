use std::time::{Duration, Instant};

use anyhow::{bail, Result};

use crate::client;

/// How many CONSECUTIVE transient failures to absorb before giving up.
///
/// A 30-minute wait at the default interval is on the order of 360 requests, so a
/// single 502 from a proxy would otherwise fail a CI gate for a sync that is
/// perfectly healthy. Consecutive, not cumulative: a blip every few minutes over a
/// long wait is normal, while five in a row means the API is actually down.
/// Anything the server answers definitively — a 401, a 404, a verdict the poll
/// closure itself reached — is not retried at all (see `client::is_transient`).
const MAX_TRANSIENT_FAILURES: u32 = 5;

/// What one poll attempt learned.
pub enum Progress<T> {
    /// Still running. The string is a short label — a stage name, a status — used
    /// to report movement; repeats of the same label are not reported twice.
    Waiting(String),
    /// Reached a terminal state, successful or not. The caller decides which.
    Done(T),
}

/// Poll `attempt` on a fixed interval until it reports `Done`, or the deadline
/// passes.
///
/// Progress goes to **stderr**, not stdout: `--json` output has to stay a single
/// parseable document, and a CI log wants the movement interleaved with everything
/// else it prints. Only label CHANGES are written, so a fifteen-minute wait
/// produces a handful of lines rather than one per poll.
pub async fn poll<T, F, Fut>(
    what: &str,
    timeout: Duration,
    interval: Duration,
    mut attempt: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<Progress<T>>>,
{
    let started = Instant::now();
    let mut last_label: Option<String> = None;
    let mut transient_failures = 0;

    loop {
        match attempt().await {
            Ok(Progress::Done(value)) => return Ok(value),
            Ok(Progress::Waiting(label)) => {
                transient_failures = 0;
                if last_label.as_deref() != Some(label.as_str()) {
                    eprintln!("{what}: {label}");
                    last_label = Some(label);
                }
            }
            Err(err)
                if client::is_transient(&err) && transient_failures < MAX_TRANSIENT_FAILURES =>
            {
                transient_failures += 1;
                eprintln!(
                    "{what}: {err:#} (retrying, {transient_failures}/{MAX_TRANSIENT_FAILURES})"
                );
                // The label is deliberately left alone, so recovering doesn't
                // reprint the stage the wait was already sitting on.
            }
            Err(err) => return Err(err),
        }

        // Sleep only as far as the deadline, so a `--poll` larger than the
        // remaining time still gets one last look instead of ending the wait
        // early. `Instant::elapsed` is monotonic, so a clock change can't skew it.
        let remaining = timeout.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            bail!(
                "timed out after {}s waiting for {what}. It may still be running — \
                 re-run the status command to check, or raise --timeout",
                started.elapsed().as_secs()
            );
        }

        tokio::time::sleep(interval.min(remaining)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    /// A `Progress` the tests can compare.
    fn done<T>(value: T) -> Result<Progress<T>> {
        Ok(Progress::Done(value))
    }

    fn waiting<T>(label: &str) -> Result<Progress<T>> {
        Ok(Progress::Waiting(label.to_string()))
    }

    fn transient() -> anyhow::Error {
        anyhow::Error::new(client::TransportError {
            url: "https://tenant.example/api".to_string(),
            source: "connection reset".to_string(),
        })
    }

    #[tokio::test]
    async fn returns_the_first_terminal_value() {
        let value = poll(
            "thing",
            Duration::from_secs(5),
            Duration::from_millis(1),
            || std::future::ready(done(7)),
        )
        .await
        .unwrap();

        assert_eq!(value, 7);
    }

    #[tokio::test]
    async fn absorbs_transient_failures_and_then_succeeds() {
        let calls = Cell::new(0);
        let value = poll(
            "thing",
            Duration::from_secs(5),
            Duration::from_millis(1),
            || {
                calls.set(calls.get() + 1);
                std::future::ready(if calls.get() <= MAX_TRANSIENT_FAILURES {
                    Err(transient())
                } else {
                    done(7)
                })
            },
        )
        .await
        .unwrap();

        assert_eq!(value, 7);
        assert_eq!(calls.get(), MAX_TRANSIENT_FAILURES + 1);
    }

    #[tokio::test]
    async fn gives_up_once_transient_failures_stop_being_occasional() {
        let calls = Cell::new(0);
        let err = poll(
            "thing",
            Duration::from_secs(5),
            Duration::from_millis(1),
            || {
                calls.set(calls.get() + 1);
                std::future::ready(Err::<Progress<()>, _>(transient()))
            },
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("connection reset"));
        // The failing attempt itself is not counted as one of the absorbed ones.
        assert_eq!(calls.get(), MAX_TRANSIENT_FAILURES + 1);
    }

    #[tokio::test]
    async fn never_retries_a_verdict_the_attempt_reached() {
        // The whole reason retrying is a whitelist: a closure that has decided the
        // sync does not exist must not be asked again until the timeout.
        let calls = Cell::new(0);
        let err = poll(
            "thing",
            Duration::from_secs(5),
            Duration::from_millis(1),
            || {
                calls.set(calls.get() + 1);
                std::future::ready(Err::<Progress<()>, _>(anyhow::anyhow!("no such sync")))
            },
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("no such sync"));
        assert_eq!(calls.get(), 1);
    }

    #[tokio::test]
    async fn times_out_reporting_the_time_actually_spent() {
        // An interval longer than the timeout still gets one look, and the message
        // reports the elapsed time rather than the configured timeout.
        let calls = Cell::new(0);
        let err = poll(
            "thing",
            Duration::from_millis(20),
            Duration::from_secs(30),
            || {
                calls.set(calls.get() + 1);
                std::future::ready(waiting::<()>("working"))
            },
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("timed out after"));
        assert!(calls.get() >= 1);
    }
}
