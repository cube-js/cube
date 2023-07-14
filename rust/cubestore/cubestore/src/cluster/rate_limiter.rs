use crate::config::injection::DIService;
use crate::util::WorkerLoop;
use crate::CubeError;
use async_trait::async_trait;
use futures_timer::Delay;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{Notify, RwLock};
use tokio_util::sync::CancellationToken;

#[async_trait]
pub trait ProcessRateLimiter: DIService + Send + Sync {
    async fn commit_task_usage(&self, size: i64);

    async fn current_budget(&self) -> i64;

    async fn current_budget_f64(&self) -> f64;

    async fn wait_for_allow(&self, timeout: Option<Duration>) -> Result<(), CubeError>;

    async fn wait_processing_loop(self: Arc<Self>);

    async fn pending_size(&self) -> usize;

    fn stop_processing_loops(&self);
}

crate::di_service!(ProcessRateLimiterImpl, [ProcessRateLimiter]);

const MS_MUL: i64 = 1000;
const MAX_PENDING_ITEMS: usize = 100000;

struct PendingItem {
    notify: Notify,
    timeout_at: SystemTime,
}

impl PendingItem {
    pub fn new(timeout: Duration) -> Arc<Self> {
        Arc::new(Self {
            notify: Notify::new(),
            timeout_at: SystemTime::now() + timeout,
        })
    }

    pub fn notify(&self) {
        self.notify.notify_one()
    }

    pub async fn wait(&self) {
        self.notify.notified().await
    }

    pub fn is_timeout(&self) -> bool {
        self.timeout_at <= SystemTime::now()
    }
}

struct Budget {
    rate: i64,
    burst: i64,
    deposit: i64,
    value: i64,
    last_refill: SystemTime,
    pending: VecDeque<Arc<PendingItem>>,
}

impl Budget {
    pub fn new(rate: i64, burst: i64, deposit: i64) -> Self {
        Self {
            rate,
            burst,
            deposit,
            value: burst,
            last_refill: SystemTime::now(),
            pending: VecDeque::with_capacity(10000),
        }
    }

    pub fn value(&self) -> i64 {
        self.value
    }

    pub fn commit_task_usage(&mut self, value: i64) {
        self.value -= value - self.deposit;
    }

    pub fn refill(&mut self) {
        let now = SystemTime::now();
        let res = now.duration_since(self.last_refill);
        let duration = if let Ok(dur) = res {
            dur.as_millis()
        } else {
            0
        };
        if duration > 0 {
            self.value = (self.value + duration as i64 * self.rate).min(self.burst);
            self.last_refill = now;
        }
        self.process_pending();
    }

    pub fn try_allow(
        &mut self,
        timeout: Option<Duration>,
    ) -> Result<Option<Arc<PendingItem>>, CubeError> {
        self.refill();
        if self.pending.is_empty() && self.value >= self.deposit {
            self.value -= self.deposit;
            Ok(None)
        } else if let Some(timeout) = timeout {
            if self.pending_size() >= MAX_PENDING_ITEMS {
                Err(CubeError::internal("Too many pending items".to_string()))
            } else {
                let pending_item = PendingItem::new(timeout);
                self.pending.push_back(pending_item.clone());
                Ok(Some(pending_item))
            }
        } else {
            Err(CubeError::internal(
                "Process can not be started due to rate limit".to_string(),
            ))
        }
    }

    fn process_pending(&mut self) {
        if self.pending.is_empty() {
            return;
        }

        loop {
            if let Some(item) = self.pending.front() {
                if item.is_timeout() {
                    item.notify();
                    self.pending.pop_front();
                } else if self.value >= self.deposit {
                    self.value -= self.deposit;
                    item.notify();
                    self.pending.pop_front();
                } else {
                    break;
                }
            } else {
                break;
            }
        }
    }

    pub fn pending_size(&self) -> usize {
        self.pending.len()
    }
}

pub struct ProcessRateLimiterImpl {
    budget: RwLock<Budget>,
    cancel_token: CancellationToken,
    pending_process_loop: WorkerLoop,
}

impl ProcessRateLimiterImpl {
    /// Crates new limitter for rate of data processing
    /// per_second - the amount of available for processing data renewable per second   
    /// burst - the maximum amount of available for processing data
    /// deposit_size - the fixed amount substracted form available stock at start of task processing
    pub fn new(per_second: i64, burst: i64, deposit_size: i64) -> Arc<Self> {
        Arc::new(Self {
            budget: RwLock::new(Budget::new(
                per_second,
                burst * MS_MUL,
                deposit_size * MS_MUL,
            )),
            cancel_token: CancellationToken::new(),
            pending_process_loop: WorkerLoop::new("RateLimiterPendingProcessing"),
        })
    }

    async fn refill_budget(&self) {
        self.budget.write().await.refill();
    }
}

#[async_trait]
impl ProcessRateLimiter for ProcessRateLimiterImpl {
    async fn commit_task_usage(&self, size: i64) {
        self.budget.write().await.commit_task_usage(size * MS_MUL);
    }

    async fn current_budget(&self) -> i64 {
        self.budget.read().await.value() / MS_MUL
    }

    async fn current_budget_f64(&self) -> f64 {
        self.budget.read().await.value() as f64 / MS_MUL as f64
    }

    async fn wait_for_allow(&self, timeout: Option<Duration>) -> Result<(), CubeError> {
        let result = self.budget.write().await.try_allow(timeout);
        match result {
            Ok(None) => Ok(()),
            Ok(Some(pending)) => {
                let timeout = if let Some(t) = timeout {
                    t
                } else {
                    Duration::from_millis(0)
                };
                tokio::select! {
                    _ = self.cancel_token.cancelled() => {
                        Ok(())
                    }
                    _ = pending.wait() => {
                        if pending.is_timeout() {
                            Err(CubeError::internal("Process can not be started due aaa to rate limit".to_string()))
                        } else {
                            Ok(())
                        }
                    }
                    _ = Delay::new(timeout) => {
                        Err(CubeError::internal("Process can not be started due !!! to rate limit".to_string()))
                    }
                }
            }
            Err(e) => Err(e),
        }
    }

    async fn wait_processing_loop(self: Arc<Self>) {
        let scheduler = self.clone();
        scheduler
            .pending_process_loop
            .process(
                scheduler.clone(),
                async move |_| Ok(Delay::new(Duration::from_millis(10)).await),
                async move |s, _| {
                    s.refill_budget().await;
                    Ok(())
                },
            )
            .await;
    }

    fn stop_processing_loops(&self) {
        self.cancel_token.cancel();
        self.pending_process_loop.stop();
    }

    async fn pending_size(&self) -> usize {
        self.budget.read().await.pending_size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::cube_ext;
    use futures_util::future::join_all;
    use tokio::time::sleep;

    #[tokio::test]
    async fn rate_limiter_without_refill_test() {
        let rate_limiter = ProcessRateLimiterImpl::new(0, 100, 10);
        let r = rate_limiter.wait_for_allow(None).await;
        assert!(r.is_ok());
        assert_eq!(rate_limiter.current_budget().await, 90);
        rate_limiter.commit_task_usage(50).await;
        assert_eq!(rate_limiter.current_budget().await, 50);

        let r = rate_limiter.wait_for_allow(None).await;
        assert!(r.is_ok());
        assert_eq!(rate_limiter.current_budget().await, 40);
        rate_limiter.commit_task_usage(45).await;
        assert_eq!(rate_limiter.current_budget().await, 5);

        assert!(rate_limiter.wait_for_allow(None).await.is_err());

        rate_limiter.commit_task_usage(20).await;
        assert_eq!(rate_limiter.current_budget().await, -5);

        assert!(rate_limiter.wait_for_allow(None).await.is_err());
    }

    #[tokio::test]
    async fn rate_limiter_base_refill_test() {
        let rate_limiter = ProcessRateLimiterImpl::new(10, 10, 0);
        rate_limiter.commit_task_usage(3).await;
        assert_eq!(rate_limiter.current_budget().await, 7);
        sleep(Duration::from_millis(200)).await;
        let r = rate_limiter.wait_for_allow(None).await;
        assert!(r.is_ok());
        assert_eq!(rate_limiter.current_budget().await, 9);
        sleep(Duration::from_millis(300)).await;
        let r = rate_limiter.wait_for_allow(None).await;
        assert!(r.is_ok());
        assert_eq!(rate_limiter.current_budget().await, 10);

        rate_limiter.commit_task_usage(12).await;
        let r = rate_limiter.wait_for_allow(None).await;
        assert!(r.is_err());
        sleep(Duration::from_millis(200)).await;
        let r = rate_limiter.wait_for_allow(None).await;
        assert!(r.is_ok());
    }
    #[tokio::test]
    async fn rate_limiter_pending_test() {
        let rate_limiter = ProcessRateLimiterImpl::new(10, 10, 2);
        let rl = rate_limiter.clone();
        let proc = cube_ext::spawn(async move { rl.wait_processing_loop().await });
        let mut futures = Vec::new();
        for _ in 0..10 {
            let now = SystemTime::now();
            let limiter_to_move = rate_limiter.clone();
            futures.push(cube_ext::spawn(async move {
                let res = limiter_to_move
                    .wait_for_allow(Some(Duration::from_millis(1100)))
                    .await;
                match res {
                    Ok(_) => Some(now.elapsed().unwrap().as_millis() / 100),
                    Err(_) => None,
                }
            }));
            //Delay::new(Duration::from_millis(5)).await;
        }
        let r = join_all(futures)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            r,
            vec![
                Some(0),
                Some(0),
                Some(0),
                Some(0),
                Some(0),
                Some(2),
                Some(4),
                Some(6),
                Some(8),
                Some(10)
            ]
        );
        assert_eq!(rate_limiter.pending_size().await, 0);

        Delay::new(Duration::from_millis(1000)).await;

        let mut futures = Vec::new();
        for _ in 0..10 {
            let now = SystemTime::now();
            let limiter_to_move = rate_limiter.clone();
            futures.push(cube_ext::spawn(async move {
                let res = limiter_to_move
                    .wait_for_allow(Some(Duration::from_millis(500)))
                    .await;
                match res {
                    Ok(_) => Some(now.elapsed().unwrap().as_millis() / 100),
                    Err(_) => None,
                }
            }));
            //Delay::new(Duration::from_millis(5)).await;
        }
        let r = join_all(futures)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            r,
            vec![
                Some(0),
                Some(0),
                Some(0),
                Some(0),
                Some(0),
                Some(2),
                Some(4),
                None,
                None,
                None
            ]
        );

        Delay::new(Duration::from_millis(1050)).await;
        assert_eq!(rate_limiter.current_budget().await, 10);
        rate_limiter.commit_task_usage(12).await;
        assert_eq!(rate_limiter.current_budget().await, 0);

        let mut futures = Vec::new();
        for _ in 0..2 {
            let now = SystemTime::now();
            let limiter_to_move = rate_limiter.clone();
            futures.push(cube_ext::spawn(async move {
                let res = limiter_to_move
                    .wait_for_allow(Some(Duration::from_millis(100)))
                    .await;
                match res {
                    Ok(_) => Some(now.elapsed().unwrap().as_millis() / 100),
                    Err(_) => None,
                }
            }));
            Delay::new(Duration::from_millis(300)).await;
        }
        let r = join_all(futures)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(r, vec![None, Some(0)]);

        Delay::new(Duration::from_millis(15)).await;
        assert_eq!(rate_limiter.pending_size().await, 0);

        rate_limiter.stop_processing_loops();
        proc.await.unwrap();
    }
}
