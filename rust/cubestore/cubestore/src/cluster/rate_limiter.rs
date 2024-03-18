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
    Cache,
    Queue,
}

impl TaskType {
    pub fn name(&self) -> String {
        match self {
            Self::Select => "select".to_string(),
            Self::Job => "job".to_string(),
            Self::Cache => "cache".to_string(),
            Self::Queue => "queue".to_string(),
        }
    }
}

#[derive(Clone, Default)]
pub struct TraceIndex {
    pub table_id: Option<u64>,
    pub trace_obj: Option<String>,
}

#[async_trait]
pub trait ProcessRateLimiter: DIService + Send + Sync {
    async fn commit_task_usage(
        &self,
        task_type: TaskType,
        size: i64,
        wait_ms: u64,
        trace_index: TraceIndex,
    );

    async fn current_budget(&self, task_type: TaskType) -> Option<i64>;

    async fn current_budget_f64(&self, task_type: TaskType) -> Option<f64>;

    // Return waiting time in ms
    async fn wait_for_allow(
        &self,
        task_type: TaskType,
        timeout: Option<Duration>,
    ) -> Result<u64, CubeError>;

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
    async fn commit_task_usage(
        &self,
        _task_type: TaskType,
        _size: i64,
        _wait_ms: u64,
        _trace_index: TraceIndex,
    ) {
    }

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
    ) -> Result<u64, CubeError> {
        Ok(0)
    }

    async fn spawn_processing_loop(self: Arc<Self>) -> Vec<JoinHandle<()>> {
        vec![]
    }

    async fn pending_size(&self, _task_type: TaskType) -> Option<usize> {
        None
    }

    fn stop_processing_loops(&self) {}
}
