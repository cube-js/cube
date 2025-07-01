use crate::CubeError;
use futures_timer::Delay;
use log::error;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet};
use std::fmt::Debug;
use std::future::Future;
use std::hash::Hash;
use std::sync::Arc;
use tokio::sync::{Notify, RwLock};
use tokio::time::Duration;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Eq, PartialEq)]
pub struct TimedTask<T: Debug + PartialEq + Eq> {
    pub deadline: Instant,
    pub task: T,
}

impl<T: Debug + PartialEq + Eq> PartialOrd for TimedTask<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // Reverse order to have min heap
        other.deadline.partial_cmp(&self.deadline)
    }
}

impl<T: Debug + PartialEq + Eq> Ord for TimedTask<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse order to have min heap
        other.deadline.cmp(&self.deadline)
    }
}

/// Cleans up deactivated partitions and chunks on remote fs.
/// Ensures enough time has passed that queries over those files finish.
pub struct DeadlineQueue<T: Debug + PartialEq + Eq + Hash + Clone> {
    stop: CancellationToken,
    task_notify: Notify,
    pending: RwLock<(BinaryHeap<TimedTask<T>>, HashSet<T>)>,
    gc_loop_interval: u64,
}

impl<T: Debug + PartialEq + Eq + Hash + Clone> DeadlineQueue<T> {
    pub fn new(gc_loop_interval: u64, stop: CancellationToken) -> Self {
        Self {
            gc_loop_interval,
            stop,
            task_notify: Notify::new(),
            pending: RwLock::new((BinaryHeap::new(), HashSet::new())),
        }
    }

    pub async fn send(&self, task: T, deadline: Instant) -> Result<(), CubeError> {
        if self.pending.read().await.1.get(&task).is_none() {
            let mut pending_lock = self.pending.write().await;
            // Double-checked locking
            if pending_lock.1.get(&task).is_none() {
                log::trace!(
                    "Posting GCTask {}: {:?}",
                    deadline
                        .checked_duration_since(Instant::now())
                        .map(|d| format!("in {:?}", d))
                        .unwrap_or("now".to_string()),
                    task
                );
                pending_lock.1.insert(task.clone());
                pending_lock.0.push(TimedTask { task, deadline });
                self.task_notify.notify_waiters();
            }
        }

        Ok(())
    }

    pub async fn run<S, F>(
        &self,
        service: Arc<S>,
        loop_fn: impl Fn(Arc<S>, T) -> F + Send + Sync + 'static,
    ) where
        S: Send + Sync + 'static,
        F: Future<Output = Result<(), CubeError>> + Send + 'static,
    {
        loop {
            tokio::select! {
                _ = self.stop.cancelled() => {
                    return;
                }
                _ = Delay::new(Duration::from_secs(self.gc_loop_interval)) => {}
                _ = self.task_notify.notified() => {}
            };

            while self
                .pending
                .read()
                .await
                .0
                .peek()
                .map(|current| current.deadline <= Instant::now())
                .unwrap_or(false)
            {
                let task = {
                    let mut pending_lock = self.pending.write().await;
                    // Double-checked locking
                    if pending_lock
                        .0
                        .peek()
                        .map(|current| current.deadline <= Instant::now())
                        .unwrap_or(false)
                    {
                        let task = pending_lock.0.pop().unwrap();
                        pending_lock.1.remove(&task.task);
                        task.task
                    } else {
                        continue;
                    }
                };

                if let Err(e) = loop_fn(service.clone(), task).await {
                    error!("Error while processing deadline queue: {}", e);
                };
            }
        }
    }

    pub async fn run_batching<S, F>(
        &self,
        service: Arc<S>,
        loop_fn: impl Fn(Arc<S>, Vec<T>) -> F + Send + Sync + 'static,
    ) where
        S: Send + Sync + 'static,
        F: Future<Output = Result<(), CubeError>> + Send + 'static,
    {
        loop {
            tokio::select! {
                _ = self.stop.cancelled() => {
                    return;
                }
                _ = Delay::new(Duration::from_secs(self.gc_loop_interval)) => {}
                _ = self.task_notify.notified() => {}
            };

            let mut pending_tasks = vec![];

            while self
                .pending
                .read()
                .await
                .0
                .peek()
                .map(|current| current.deadline <= Instant::now())
                .unwrap_or(false)
            {
                let task = {
                    let mut pending_lock = self.pending.write().await;
                    // Double-checked locking
                    if pending_lock
                        .0
                        .peek()
                        .map(|current| current.deadline <= Instant::now())
                        .unwrap_or(false)
                    {
                        let task = pending_lock.0.pop().unwrap();
                        pending_lock.1.remove(&task.task);
                        task.task
                    } else {
                        continue;
                    }
                };

                pending_tasks.push(task);
            }

            if let Err(e) = loop_fn(service.clone(), pending_tasks).await {
                error!("Error while processing deadline queue: {}", e);
            };
        }
    }
}
