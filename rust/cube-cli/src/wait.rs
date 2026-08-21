use std::time::{Duration, Instant};

use anyhow::Result;

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

/// Ceiling on the backed-off sleep between retries, so a long `--poll` plus
/// doubling can't overshoot the wait it belongs to.
const BACKOFF_CAP: Duration = Duration::from_secs(60);

/// The advice appended to a timeout when the caller doesn't say otherwise: true
/// whenever the budget is the user's own `--timeout`.
const DEFAULT_TIMEOUT_ADVICE: &str =
    "It may still be running — re-run the status command to check, or raise --timeout";

/// How a wait is configured.
///
/// A struct rather than positional arguments because two of the three are
/// `Duration`s: `poll(what, timeout, interval, …)` compiles just as happily with
/// those two swapped, and a wait that polls every 30 minutes for 5 seconds fails in
/// a way that looks like a server problem. Named fields make that unwritable.
pub struct Wait {
    /// What is being waited for; prefixes each progress line.
    pub what: String,
    pub timeout: Duration,
    pub interval: Duration,
    /// Appended to the timeout error. Callers whose budget is NOT the user's
    /// `--timeout` should replace it — advising someone to raise a flag that cannot
    /// move the deadline sends them off to do something useless.
    ///
    /// Empty means "nothing to add": it only ever reaches the reader on the timeout
    /// branch, so a caller that explains the failure and its recovery for EVERY
    /// outcome (via `anyhow` context) would otherwise say it twice on that one
    /// branch. See [`Wait::advising_nothing`].
    pub on_timeout: String,
}

impl Wait {
    pub fn new(what: impl Into<String>, timeout: Duration, interval: Duration) -> Self {
        Self {
            what: what.into(),
            timeout,
            interval,
            on_timeout: DEFAULT_TIMEOUT_ADVICE.to_string(),
        }
    }

    /// Replace the timeout advice — see [`Wait::on_timeout`].
    pub fn advising(mut self, advice: impl Into<String>) -> Self {
        self.on_timeout = advice.into();
        self
    }

    /// Add nothing to the timeout message, for a caller whose own error context
    /// already carries the recovery on every outcome — see [`Wait::on_timeout`].
    pub fn advising_nothing(self) -> Self {
        self.advising("")
    }
}

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
pub async fn poll<T, F, Fut>(wait: Wait, mut attempt: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<Progress<T>>>,
{
    let Wait {
        what,
        timeout,
        interval,
        on_timeout,
    } = wait;
    let started = Instant::now();
    let mut last_label: Option<String> = None;
    // The CURRENT failure streak: how many consecutive transient failures, and the
    // most recent one's message. One variable rather than two, because the count and
    // the message are only ever meaningful together — and keeping a message past the
    // streak that produced it is how a blip recovered from at minute 1 ends up blamed
    // for a timeout at minute 30. `None` between streaks says exactly that.
    let mut streak: Option<(u32, String)> = None;

    // Same message whether the deadline lands between attempts or inside one.
    let timed_out =
        |elapsed: Duration, last_label: &Option<String>, streak: &Option<(u32, String)>| {
            anyhow::anyhow!(
                "timed out after {}s waiting for {what}{}{}{}",
                elapsed.as_secs(),
                match last_label {
                    Some(label) => format!(" (last seen: {label})"),
                    None => String::new(),
                },
                if on_timeout.is_empty() {
                    String::new()
                } else {
                    format!(". {on_timeout}")
                },
                match streak {
                    Some((_, err)) => format!(". Last error: {err}"),
                    None => String::new(),
                }
            )
        };

    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        let failures = streak.as_ref().map_or(0, |(count, _)| *count);

        let outcome = match tokio::time::timeout_at(deadline, attempt()).await {
            Ok(outcome) => outcome,
            // The request itself outlived the budget. Nothing arriving later can be
            // trusted as "within --timeout", so this ends the wait rather than
            // looping back to check the clock.
            Err(_) => return Err(timed_out(started.elapsed(), &last_label, &streak)),
        };

        match outcome {
            Ok(Progress::Done(value)) => return Ok(value),
            Ok(Progress::Waiting(label)) => {
                streak = None;
                if last_label.as_deref() != Some(label.as_str()) {
                    eprintln!("{what}: {label}");
                    last_label = Some(label);
                }
            }
            Err(err) if client::is_transient(&err) && failures < MAX_TRANSIENT_FAILURES => {
                let count = failures + 1;
                eprintln!("{what}: {err:#} (retrying, {count}/{MAX_TRANSIENT_FAILURES})");
                streak = Some((count, format!("{err:#}")));
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
            return Err(timed_out(started.elapsed(), &last_label, &streak));
        }

        let consecutive_failures = streak.as_ref().map_or(0, |(count, _)| *count);
        tokio::time::sleep(backoff(interval, consecutive_failures).min(remaining)).await;
    }
}

/// How long to wait before the next attempt: the caller's interval normally, and
/// double it per consecutive failure while retrying.
///
/// Retrying at the failing cadence is the wrong response to the one status that
/// says the cadence itself is the problem — a 429 answered five more times a poll
/// apart just postpones the failure. (`Retry-After` would be better still, but the
/// header isn't carried on the error; the backoff is the part that helps without
/// plumbing headers through.) Progress resets it, so a recovered wait goes straight
/// back to the interval the user asked for.
fn backoff(interval: Duration, consecutive_failures: u32) -> Duration {
    if consecutive_failures == 0 {
        return interval;
    }

    let factor = 1u32 << (consecutive_failures - 1).min(16);

    interval
        .checked_mul(factor)
        .unwrap_or(BACKOFF_CAP)
        .min(BACKOFF_CAP)
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

    /// A wait with a 1ms interval, for tests that only care about the outcome.
    fn short_wait(what: &str, timeout: Duration) -> Wait {
        Wait::new(what, timeout, Duration::from_millis(1))
    }

    fn transient() -> anyhow::Error {
        anyhow::Error::new(client::TransportError {
            url: "https://tenant.example/api".to_string(),
            source: "connection reset".to_string(),
        })
    }

    /// The budget has to bound the attempt, not just the gap between attempts. Before
    /// `timeout_at`, a request that never answered outlived `--timeout` entirely and
    /// hung the caller — in CI, until the job's own timeout hours later.
    #[tokio::test]
    async fn a_never_answering_attempt_still_times_out() {
        let started = Instant::now();
        let err = poll(
            Wait::new(
                "a hung request",
                Duration::from_millis(80),
                Duration::from_millis(10),
            ),
            || async {
                std::future::pending::<()>().await;
                #[allow(unreachable_code)]
                Ok::<Progress<serde_json::Value>, anyhow::Error>(unreachable!(
                    "the deadline must fire first"
                ))
            },
        )
        .await
        .expect_err("must not wait past the deadline");

        assert!(err.to_string().contains("timed out"), "{err}");
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "returned after {:?} — the deadline did not bound the attempt",
            started.elapsed()
        );
    }

    /// A response that arrives after the deadline is not "within --timeout" either.
    #[tokio::test]
    async fn an_answer_after_the_deadline_is_not_accepted() {
        let err = poll(
            Wait::new(
                "a late answer",
                Duration::from_millis(50),
                Duration::from_millis(10),
            ),
            || async {
                tokio::time::sleep(Duration::from_millis(400)).await;
                Ok(Progress::Done(serde_json::json!({"late": true})))
            },
        )
        .await
        .expect_err("a late success must not count");

        assert!(err.to_string().contains("timed out"), "{err}");
    }

    #[tokio::test]
    async fn returns_the_first_terminal_value() {
        let value = poll(short_wait("thing", Duration::from_secs(5)), || {
            std::future::ready(done(7))
        })
        .await
        .unwrap();

        assert_eq!(value, 7);
    }

    #[tokio::test]
    async fn absorbs_transient_failures_and_then_succeeds() {
        let calls = Cell::new(0);
        let value = poll(short_wait("thing", Duration::from_secs(5)), || {
            calls.set(calls.get() + 1);
            std::future::ready(if calls.get() <= MAX_TRANSIENT_FAILURES {
                Err(transient())
            } else {
                done(7)
            })
        })
        .await
        .unwrap();

        assert_eq!(value, 7);
        assert_eq!(calls.get(), MAX_TRANSIENT_FAILURES + 1);
    }

    #[tokio::test]
    async fn gives_up_once_transient_failures_stop_being_occasional() {
        let calls = Cell::new(0);
        let err = poll(short_wait("thing", Duration::from_secs(5)), || {
            calls.set(calls.get() + 1);
            std::future::ready(Err::<Progress<()>, _>(transient()))
        })
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
        let err = poll(short_wait("thing", Duration::from_secs(5)), || {
            calls.set(calls.get() + 1);
            std::future::ready(Err::<Progress<()>, _>(anyhow::anyhow!("no such sync")))
        })
        .await
        .unwrap_err();

        assert!(err.to_string().contains("no such sync"));
        assert_eq!(calls.get(), 1);
    }

    #[test]
    fn backoff_doubles_while_failing_and_resets_on_progress() {
        let interval = Duration::from_secs(5);
        assert_eq!(backoff(interval, 0), interval);
        assert_eq!(backoff(interval, 1), Duration::from_secs(5));
        assert_eq!(backoff(interval, 2), Duration::from_secs(10));
        assert_eq!(backoff(interval, 3), Duration::from_secs(20));
        // Capped, and an absurd interval can't overflow into a tiny sleep.
        assert_eq!(backoff(Duration::from_secs(3600), 5), BACKOFF_CAP);
        assert_eq!(backoff(Duration::MAX, 4), BACKOFF_CAP);
    }

    #[tokio::test]
    async fn a_timeout_while_absorbing_failures_reports_the_last_one() {
        // Deliberately NOT alternating error/progress: which of the two landed last
        // before the deadline would be a race. Failing throughout, with a timeout
        // short enough to expire before the failure limit is reached, puts the wait
        // on the deadline branch every time.
        let err = poll(
            Wait::new("thing", Duration::from_millis(5), Duration::from_millis(2)),
            || std::future::ready(Err::<Progress<()>, _>(transient())),
        )
        .await
        .unwrap_err();

        let message = err.to_string();
        assert!(message.contains("timed out after"), "got: {message}");
        assert!(message.contains("Last error: request to"), "got: {message}");
    }

    #[tokio::test]
    async fn a_recovered_blip_is_not_blamed_for_a_later_timeout() {
        // The failure is absorbed, the wait then runs healthily to its deadline, and
        // the timeout must not point at a blip that is long past.
        let calls = Cell::new(0);
        // 300ms, not 30: the assertion holds only once a `waiting` attempt has
        // landed, so a single scheduler stall right after the failing first attempt
        // — ordinary on a loaded CI runner — would otherwise flip it.
        let err = poll(short_wait("thing", Duration::from_millis(300)), || {
            calls.set(calls.get() + 1);
            std::future::ready(if calls.get() == 1 {
                Err(transient())
            } else {
                waiting::<()>("working")
            })
        })
        .await
        .unwrap_err();

        let message = err.to_string();
        assert!(message.contains("timed out after"), "got: {message}");
        assert!(!message.contains("Last error"), "got: {message}");
        // …and it still says what the wait was looking at when it gave up.
        assert!(message.contains("(last seen: working)"), "got: {message}");
    }

    #[tokio::test]
    async fn timeout_advice_is_the_default_unless_replaced() {
        let default = poll(short_wait("thing", Duration::from_millis(5)), || {
            std::future::ready(waiting::<()>("working"))
        })
        .await
        .unwrap_err()
        .to_string();
        assert!(default.contains("raise --timeout"), "got: {default}");

        // Silent: nothing appended, and no dangling separator where it would have
        // gone — the caller's own context says it instead.
        let silent = poll(
            short_wait("thing", Duration::from_millis(5)).advising_nothing(),
            || std::future::ready(waiting::<()>("working")),
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(
            silent.ends_with("waiting for thing (last seen: working)"),
            "got: {silent}"
        );
    }

    #[tokio::test]
    async fn times_out_reporting_the_time_actually_spent() {
        // An interval longer than the timeout still gets one look, and the message
        // reports the elapsed time rather than the configured timeout.
        let calls = Cell::new(0);
        let err = poll(
            Wait::new("thing", Duration::from_millis(20), Duration::from_secs(30)),
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
