//! Admission control for logical planning: planning runs before the result cache and nothing
//! else bounds how many queries enter it at once.

use crate::app_metrics;
use crate::CubeError;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

pub struct PlanningThrottle {
    permits: Option<Arc<Semaphore>>,
    max_queued: usize,
    max_wait: Duration,
    /// Reported as gauges, so both are approximate: the read and the report are separate steps
    /// and concurrent releases can report out of order.
    in_flight: AtomicUsize,
    queued: AtomicUsize,
}

/// Held for the whole planning of one query, released on drop.
pub struct PlanningPermit {
    throttle: Arc<PlanningThrottle>,
    _permit: OwnedSemaphorePermit,
}

impl PlanningThrottle {
    /// `max_concurrent` or `max_queued` of `0` disables the respective limit.
    pub fn new(
        max_concurrent: usize,
        max_queued: usize,
        max_wait: Duration,
    ) -> Arc<PlanningThrottle> {
        Arc::new(PlanningThrottle {
            permits: if max_concurrent == 0 {
                None
            } else {
                Some(Arc::new(Semaphore::new(max_concurrent)))
            },
            max_queued,
            max_wait,
            in_flight: AtomicUsize::new(0),
            queued: AtomicUsize::new(0),
        })
    }

    pub async fn acquire(self: &Arc<Self>) -> Result<Option<PlanningPermit>, CubeError> {
        let permits = match &self.permits {
            None => return Ok(None),
            Some(p) => p.clone(),
        };

        let permit = match permits.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(TryAcquireError::Closed) => {
                return Err(CubeError::internal(
                    "Query planning throttle is closed".to_string(),
                ))
            }
            Err(TryAcquireError::NoPermits) => {
                let _waiting = self.start_waiting()?;
                let wait_start = Instant::now();
                let permit = tokio::time::timeout(self.max_wait, permits.acquire_owned())
                    .await
                    .map_err(|_| self.waited_too_long())?;
                app_metrics::QUERY_PLANNING_THROTTLE_WAIT_US
                    .report(wait_start.elapsed().as_micros() as i64);
                permit?
            }
        };

        app_metrics::QUERY_PLANNING_THROTTLE_IN_FLIGHT
            .report(self.in_flight.fetch_add(1, Ordering::SeqCst) as i64 + 1);
        Ok(Some(PlanningPermit {
            throttle: self.clone(),
            _permit: permit,
        }))
    }

    fn waited_too_long(&self) -> CubeError {
        app_metrics::QUERY_PLANNING_THROTTLE_REJECTED.increment();
        CubeError::user(format!(
            "Waited longer than {} s for a query planning slot. Raise \
             CUBESTORE_MAX_CONCURRENT_QUERY_PLANS to plan more queries at once.",
            self.max_wait.as_secs()
        ))
    }

    fn start_waiting(self: &Arc<Self>) -> Result<WaitingGuard, CubeError> {
        let queued = self.queued.fetch_add(1, Ordering::SeqCst) + 1;
        let guard = WaitingGuard {
            throttle: self.clone(),
        };
        if self.max_queued != 0 && queued > self.max_queued {
            app_metrics::QUERY_PLANNING_THROTTLE_REJECTED.increment();
            return Err(CubeError::user(format!(
                "Too many queries are waiting to be planned: {} queued, the limit is {}. \
                 Raise CUBESTORE_MAX_QUEUED_QUERY_PLANS or CUBESTORE_MAX_CONCURRENT_QUERY_PLANS \
                 to accept more.",
                queued, self.max_queued
            )));
        }
        app_metrics::QUERY_PLANNING_THROTTLE_QUEUED.report(queued as i64);
        Ok(guard)
    }
}

struct WaitingGuard {
    throttle: Arc<PlanningThrottle>,
}

impl Drop for WaitingGuard {
    fn drop(&mut self) {
        let queued = self.throttle.queued.fetch_sub(1, Ordering::SeqCst) - 1;
        app_metrics::QUERY_PLANNING_THROTTLE_QUEUED.report(queued as i64);
    }
}

impl Drop for PlanningPermit {
    fn drop(&mut self) {
        let in_flight = self.throttle.in_flight.fetch_sub(1, Ordering::SeqCst) - 1;
        app_metrics::QUERY_PLANNING_THROTTLE_IN_FLIGHT.report(in_flight as i64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::future::join_all;
    use std::sync::atomic::AtomicBool;

    #[tokio::test]
    async fn disabled_throttle_hands_out_no_permits() {
        let throttle = PlanningThrottle::new(0, 1, Duration::from_secs(30));
        let permits = join_all((0..100).map(|_| throttle.acquire()))
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(permits.iter().all(|p| p.is_none()));
        assert_eq!(throttle.in_flight.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn concurrency_is_capped_and_permits_are_returned() {
        let throttle = PlanningThrottle::new(2, 0, Duration::from_secs(30));
        let first = throttle.acquire().await.unwrap();
        let second = throttle.acquire().await.unwrap();
        assert_eq!(throttle.in_flight.load(Ordering::SeqCst), 2);

        let throttle_to_move = throttle.clone();
        let acquired = Arc::new(AtomicBool::new(false));
        let acquired_to_move = acquired.clone();
        let third = tokio::spawn(async move {
            let permit = throttle_to_move.acquire().await.unwrap();
            acquired_to_move.store(true, Ordering::SeqCst);
            permit
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!acquired.load(Ordering::SeqCst));

        drop(first);
        third.await.unwrap();
        assert!(acquired.load(Ordering::SeqCst));

        drop(second);
        assert_eq!(throttle.in_flight.load(Ordering::SeqCst), 0);
        assert_eq!(throttle.queued.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn waiting_longer_than_the_budget_is_rejected() {
        let throttle = PlanningThrottle::new(1, 0, Duration::from_millis(50));
        let _held = throttle.acquire().await.unwrap();

        let rejected = throttle.acquire().await;
        assert!(rejected
            .err()
            .unwrap()
            .message
            .contains("Waited longer than"));
        assert_eq!(throttle.queued.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn queue_depth_over_the_limit_is_rejected() {
        let throttle = PlanningThrottle::new(1, 1, Duration::from_secs(30));
        let _held = throttle.acquire().await.unwrap();

        let throttle_to_move = throttle.clone();
        let waiting = tokio::spawn(async move { throttle_to_move.acquire().await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(throttle.queued.load(Ordering::SeqCst), 1);

        let rejected = throttle.acquire().await;
        assert!(rejected.is_err());
        assert!(rejected
            .err()
            .unwrap()
            .message
            .contains("waiting to be planned"));

        drop(_held);
        waiting.await.unwrap().unwrap();
        assert_eq!(throttle.queued.load(Ordering::SeqCst), 0);
    }
}
