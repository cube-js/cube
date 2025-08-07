use async_std::future::Future;
use std::time::{Duration, SystemTime};

/// The returned object will [log::warn] if it is alive longer than [timeout].
/// Be cautious when interpreting results in async code, this function looks at wall time. So future
/// that is not running will add to the time.
pub fn warn_long(name: &str, timeout: Duration) -> ShortSpan<'_> {
    ShortSpan {
        name,
        timeout,
        start: SystemTime::now(),
    }
}

pub async fn warn_long_fut<F: Future>(name: &str, timeout: Duration, f: F) -> F::Output {
    let _s = warn_long(name, timeout);
    f.await
}

pub struct ShortSpan<'r> {
    name: &'r str,
    timeout: Duration,
    start: SystemTime,
}

impl<'r> Drop for ShortSpan<'r> {
    fn drop(&mut self) {
        // We won't report anything in case of error.
        let elapsed = self.start.elapsed().unwrap_or(Duration::from_secs(0));
        if self.timeout < elapsed {
            log::warn!(
                "Long operation. '{}' took {}ms, expected less than {}ms",
                self.name,
                elapsed.as_millis(),
                self.timeout.as_millis()
            )
        }
    }
}
