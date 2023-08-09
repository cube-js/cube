use crate::config::injection::DIService;
use crate::CubeError;
use async_trait::async_trait;
use std::cmp::PartialEq;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

#[derive(Eq, PartialEq, Hash, Clone)]
pub enum TaskType {
    Select,
    Job,
}

#[async_trait]
pub trait ProcessRateLimiter: DIService + Send + Sync {
    async fn commit_task_usage(&self, task_type: TaskType, size: i64);

    async fn current_budget(&self, task_type: TaskType) -> Option<i64>;

    async fn current_budget_f64(&self, task_type: TaskType) -> Option<f64>;

    async fn wait_for_allow(
        &self,
        task_type: TaskType,
        timeout: Option<Duration>,
    ) -> Result<(), CubeError>;

    async fn spawn_processing_loop(self: Arc<Self>) -> Vec<JoinHandle<()>>;

    async fn pending_size(&self, task_type: TaskType) -> Option<usize>;

    fn stop_processing_loops(&self);
}

crate::di_service!(BasicProcessRateLimiter, [ProcessRateLimiter]);

pub struct BasicProcessRateLimiter;

impl BasicProcessRateLimiter {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {})
    }
}

#[async_trait]
impl ProcessRateLimiter for BasicProcessRateLimiter {
    async fn commit_task_usage(&self, _task_type: TaskType, _size: i64) {}

    async fn current_budget(&self, _task_type: TaskType) -> Option<i64> {
        None
    }

    async fn current_budget_f64(&self, _task_type: TaskType) -> Option<f64> {
        None
    }

    async fn wait_for_allow(
        &self,
        _task_type: TaskType,
        _timeout: Option<Duration>,
    ) -> Result<(), CubeError> {
        Ok(())
    }

    async fn spawn_processing_loop(self: Arc<Self>) -> Vec<JoinHandle<()>> {
        vec![]
    }

    async fn pending_size(&self, _task_type: TaskType) -> Option<usize> {
        None
    }

    fn stop_processing_loops(&self) {}
}
