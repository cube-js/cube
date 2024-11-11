use crate::config::injection::DIService;
use crate::CubeError;
use std::sync::Arc;
use tracing::Span;

pub type TraceIdAndSpanId = (u128, u64);

pub trait TracingHelper: DIService + Send + Sync {
    fn trace_and_span_id(&self) -> Option<TraceIdAndSpanId>;
    fn span_from_existing_trace(
        &self,
        trace_id_and_span_id: Option<TraceIdAndSpanId>,
    ) -> Option<Span>;
}

pub struct TracingHelperImpl;

impl TracingHelper for TracingHelperImpl {
    fn trace_and_span_id(&self) -> Option<TraceIdAndSpanId> {
        None
    }

    fn span_from_existing_trace(
        &self,
        _trace_id_and_span_id: Option<TraceIdAndSpanId>,
    ) -> Option<Span> {
        None
    }
}

impl TracingHelperImpl {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {})
    }
}

crate::di_service!(TracingHelperImpl, [TracingHelper]);
