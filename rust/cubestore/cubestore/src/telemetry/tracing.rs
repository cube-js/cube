use crate::config::injection::DIService;
use crate::CubeError;
use std::sync::Arc;

pub trait TracingHelper: DIService + Send + Sync {
    fn trace_and_span_id(&self) -> Option<(u64, u64)>;
}

pub struct TracingHelperImpl;

impl TracingHelper for TracingHelperImpl {
    fn trace_and_span_id(&self) -> Option<(u64, u64)> {
        None
    }
}

impl TracingHelperImpl {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {})
    }
}

crate::di_service!(TracingHelperImpl, [TracingHelper]);
