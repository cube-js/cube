use std::time::{Duration, Instant};

use anyhow::{bail, Result};

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
/// else it prints. Only label CHANGES are written, so a fifteen-minute sync
/// produces a handful of lines rather than one per poll.
///
/// The deadline is checked before sleeping as well as after, so a timeout is
/// reported promptly instead of after one last idle interval.
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

    loop {
        match attempt().await? {
            Progress::Done(value) => return Ok(value),
            Progress::Waiting(label) => {
                if last_label.as_deref() != Some(label.as_str()) {
                    eprintln!("{what}: {label}");
                    last_label = Some(label);
                }
            }
        }

        let elapsed = started.elapsed();
        if elapsed + interval >= timeout {
            bail!(
                "timed out after {}s waiting for {what}. It may still be running — \
                 re-run the status command to check, or raise --timeout",
                timeout.as_secs()
            );
        }

        tokio::time::sleep(interval).await;
    }
}
