use crate::config::injection::DIService;
use crate::util::memory::MemoryHandler;
use crate::CubeError;
use serde::{Deserialize, Serialize};
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

const ERROR_DESCRIPTION_MAX_CHARS: usize = 512;

/// Span of one unit of work. Only such spans start a trace, so every one declares the fields
/// the helpers in this module fill in.
#[macro_export]
macro_rules! work_span {
    ($name:expr $(, $($fields:tt)*)?) => {
        ::tracing::info_span!(
            $name,
            "otel.status_code" = ::tracing::field::Empty,
            "otel.status_description" = ::tracing::field::Empty,
            "error.type" = ::tracing::field::Empty,
            "cubestore.request_id" = ::tracing::field::Empty,
            "cubestore.instance" = ::tracing::field::Empty,
            "cubestore.data_loaded_bytes" = ::tracing::field::Empty,
            "cubestore.memory.baseline_bytes" = ::tracing::field::Empty,
            "cubestore.memory.peak_bytes" = ::tracing::field::Empty,
            $($($fields)*)?
        )
    };
}

pub fn record_error(span: &Span, error: &CubeError) {
    span.record("otel.status_code", "ERROR");
    span.record(
        "otel.status_description",
        truncated(&error.message, ERROR_DESCRIPTION_MAX_CHARS),
    );
    span.record("error.type", tracing::field::debug(&error.cause));
}

pub fn record_result<T>(span: &Span, result: &Result<T, CubeError>) {
    if let Err(error) = result {
        record_error(span, error);
    }
}

/// Copies the identifiers Cube sends along with a query, so a span can be joined to the request
/// that caused it.
pub fn record_trace_obj(span: &Span, trace_obj: Option<&str>) {
    let Some(serde_json::Value::Object(trace_obj)) =
        trace_obj.and_then(|json| serde_json::from_str(json).ok())
    else {
        return;
    };
    for (key, field) in [
        ("requestId", "cubestore.request_id"),
        ("instance", "cubestore.instance"),
    ] {
        if let Some(value) = trace_obj.get(key).and_then(|value| value.as_str()) {
            span.record(field, value);
        }
    }
}

/// Process heap figures of a unit of work that had the process to itself.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProcessMemoryUsage {
    baseline_bytes: Option<u64>,
    peak_bytes: Option<u64>,
}

impl ProcessMemoryUsage {
    pub fn record(&self, span: &Span) {
        if let Some(baseline_bytes) = self.baseline_bytes {
            span.record("cubestore.memory.baseline_bytes", baseline_bytes);
        }
        if let Some(peak_bytes) = self.peak_bytes {
            span.record("cubestore.memory.peak_bytes", peak_bytes);
        }
    }
}

pub struct ProcessMemoryWindow {
    memory_handler: Arc<dyn MemoryHandler>,
    baseline_bytes: Option<u64>,
}

impl ProcessMemoryWindow {
    pub fn start(memory_handler: Arc<dyn MemoryHandler>) -> Self {
        memory_handler.reset_peak();
        let baseline_bytes = memory_handler.allocated_bytes();
        Self {
            memory_handler,
            baseline_bytes,
        }
    }

    pub fn usage(&self) -> ProcessMemoryUsage {
        ProcessMemoryUsage {
            baseline_bytes: self.baseline_bytes,
            peak_bytes: self.memory_handler.peak_allocated_bytes(),
        }
    }
}

fn truncated(text: &str, max_chars: usize) -> &str {
    match text.char_indices().nth(max_chars) {
        Some((end, _)) => &text[..end],
        None => text,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::fmt::Debug;
    use std::sync::Mutex;
    use tracing::field::{Field, Visit};
    use tracing::span::{Attributes, Id, Record};
    use tracing::{Event, Metadata, Subscriber};

    #[derive(Default)]
    struct RecordedFields(Mutex<HashMap<String, String>>);

    impl Visit for &RecordedFields {
        fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
            self.record_str(field, &format!("{:?}", value));
        }

        fn record_str(&mut self, field: &Field, value: &str) {
            self.0
                .lock()
                .unwrap()
                .insert(field.name().to_string(), value.to_string());
        }
    }

    struct SingleSpanSubscriber(Arc<RecordedFields>);

    impl Subscriber for SingleSpanSubscriber {
        fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, attributes: &Attributes<'_>) -> Id {
            attributes.record(&mut &*self.0);
            Id::from_u64(1)
        }

        fn record(&self, _span: &Id, values: &Record<'_>) {
            values.record(&mut &*self.0);
        }

        fn record_follows_from(&self, _span: &Id, _follows: &Id) {}
        fn event(&self, _event: &Event<'_>) {}
        fn enter(&self, _span: &Id) {}
        fn exit(&self, _span: &Id) {}
    }

    fn recorded_fields(fill: impl FnOnce(&Span)) -> HashMap<String, String> {
        let fields = Arc::new(RecordedFields::default());
        tracing::subscriber::with_default(SingleSpanSubscriber(fields.clone()), || {
            fill(&crate::work_span!("unit of work"));
        });
        let fields = fields.0.lock().unwrap().clone();
        fields
    }

    #[test]
    fn error_sets_status_description_and_type() {
        let fields =
            recorded_fields(|span| record_error(span, &CubeError::corrupt_data("é".repeat(600))));

        assert_eq!(fields["otel.status_code"], "ERROR");
        assert_eq!(fields["error.type"], "CorruptData");
        assert_eq!(
            fields["otel.status_description"],
            "é".repeat(ERROR_DESCRIPTION_MAX_CHARS)
        );
    }

    #[test]
    fn successful_result_leaves_the_span_without_status() {
        let fields = recorded_fields(|span| record_result(span, &Ok(())));

        assert!(fields.is_empty());
    }

    #[test]
    fn trace_obj_identifiers_are_copied() {
        let fields = recorded_fields(|span| {
            record_trace_obj(
                span,
                Some(r#"{"requestId":"req-1-span-2","instance":"api-0","buildRangeEnd":"x"}"#),
            )
        });

        assert_eq!(
            fields,
            HashMap::from([
                (
                    "cubestore.request_id".to_string(),
                    "req-1-span-2".to_string()
                ),
                ("cubestore.instance".to_string(), "api-0".to_string()),
            ])
        );
    }

    #[test]
    fn unusable_trace_obj_records_nothing() {
        for trace_obj in [
            None,
            Some("not json"),
            Some("[1]"),
            Some(r#"{"requestId":7}"#),
        ] {
            assert!(recorded_fields(|span| record_trace_obj(span, trace_obj)).is_empty());
        }
    }

    #[derive(Debug)]
    struct FixedMemoryHandler;

    impl MemoryHandler for FixedMemoryHandler {
        fn check_memory(&self) -> Result<(), CubeError> {
            Ok(())
        }

        fn allocated_bytes(&self) -> Option<u64> {
            Some(100)
        }

        fn peak_allocated_bytes(&self) -> Option<u64> {
            Some(250)
        }
    }

    crate::di_service!(FixedMemoryHandler, [MemoryHandler]);

    #[test]
    fn memory_window_records_baseline_and_peak() {
        let fields = recorded_fields(|span| {
            ProcessMemoryWindow::start(Arc::new(FixedMemoryHandler))
                .usage()
                .record(span)
        });

        assert_eq!(fields["cubestore.memory.baseline_bytes"], "100");
        assert_eq!(fields["cubestore.memory.peak_bytes"], "250");
    }

    #[test]
    fn untracked_allocator_records_no_memory() {
        let fields = recorded_fields(|span| {
            ProcessMemoryWindow::start(crate::util::memory::MemoryHandlerImpl::new())
                .usage()
                .record(span)
        });

        assert!(fields.is_empty());
    }
}
