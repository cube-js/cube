//! Per-query detailed trace collection for `EXPLAIN ANALYZE DETAILED`.
//!
//! The trace is assembled per process-region (entry node, worker node, select
//! subprocess) into the serializable `*Trace` structs and merged upwards across
//! the network and IPC boundaries. Collection is gated by the presence of a
//! per-query `TraceCtx` in the task-local `TRACE`: when no detailed query is in
//! flight the recording helpers short-circuit before doing any work.

use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex, Weak};
use std::time::Instant;

/// Aggregation axis for timed operations. Stays a small, stable taxonomy: a new
/// probe usually reuses an existing kind and only adds a `label`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OpKind {
    Transport,
    Serialize,
    Deserialize,
    Metastore,
    Planning,
    WarmupIo,
    ChunkLoad,
    Other,
}

/// A single (aggregated) measurement: how long a class of operations took and,
/// optionally, how many bytes it moved. Repeats with the same `(kind, label)`
/// are folded together on insertion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpSample {
    pub kind: OpKind,
    pub label: String,
    pub elapsed_us: u64,
    pub bytes: Option<u64>,
    pub count: u32,
}

/// Trace assembled inside the select subprocess and shipped back over IPC.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SubprocessTrace {
    pub ops: Vec<OpSample>,
    pub exec_memory_peak_bytes: Option<u64>,
    pub physical_plan: Option<String>,
}

/// Trace assembled on a worker node and shipped back over the network.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorkerTrace {
    pub node_name: String,
    pub ops: Vec<OpSample>,
    pub subprocess: Option<SubprocessTrace>,
}

/// Trace of the entry node that received the query.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RouterTrace {
    pub ops: Vec<OpSample>,
}

/// Whole-query trace assembled on the entry node from the per-worker traces.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryTrace {
    pub router: RouterTrace,
    pub workers: Vec<WorkerTrace>,
}

/// Per-query sink for one process-region. Interior-mutable so recording helpers
/// can write through a shared `Arc` from anywhere in the query's task.
pub struct TraceCtx {
    ops: Mutex<Vec<OpSample>>,
}

impl TraceCtx {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            ops: Mutex::new(Vec::new()),
        })
    }

    fn push(&self, sample: OpSample) {
        let mut ops = self.ops.lock().unwrap();
        if let Some(existing) = ops
            .iter_mut()
            .find(|o| o.kind == sample.kind && o.label == sample.label)
        {
            existing.elapsed_us += sample.elapsed_us;
            existing.count += sample.count;
            existing.bytes = match (existing.bytes, sample.bytes) {
                (Some(a), Some(b)) => Some(a + b),
                (a, b) => a.or(b),
            };
        } else {
            ops.push(sample);
        }
    }

    pub fn take_ops(&self) -> Vec<OpSample> {
        std::mem::take(&mut self.ops.lock().unwrap())
    }
}

tokio::task_local! {
    pub static TRACE: Option<Arc<TraceCtx>>;
}

/// The active per-query sink, or `None` when no detailed query is in flight.
pub fn current_trace() -> Option<Arc<TraceCtx>> {
    TRACE.try_with(|t| t.clone()).ok().flatten()
}

/// Guard factory for the `#[cuberpc::service(trace_guard = ...)]`-generated
/// `TracedMetaStore` decorator: one sample per metastore method call.
pub fn metastore_trace_guard(method: &'static str) -> OpGuard {
    OpGuard::start(OpKind::Metastore, method)
}

/// Run `fut` with `ctx` as the active sink for the current process-region.
pub async fn scoped<F>(ctx: Option<Arc<TraceCtx>>, fut: F) -> F::Output
where
    F: std::future::Future,
{
    TRACE.scope(ctx, fut).await
}

/// RAII timer that records an `OpSample` on drop. Captures nothing (not even the
/// start instant) when tracing is off, so leaving these in hot paths is cheap.
pub struct OpGuard {
    kind: OpKind,
    label: &'static str,
    began: Option<Instant>,
    // Captured at `start` so the sample lands in the ctx that was active then,
    // even if the guard outlives the task-local scope. Weak so a dropped ctx
    // just discards the sample instead of keeping it alive.
    ctx: Weak<TraceCtx>,
    bytes: Option<u64>,
}

impl OpGuard {
    pub fn start(kind: OpKind, label: &'static str) -> Self {
        let ctx = current_trace();
        let began = ctx.is_some().then(Instant::now);
        Self {
            kind,
            label,
            began,
            ctx: ctx.as_ref().map(Arc::downgrade).unwrap_or_default(),
            bytes: None,
        }
    }

    pub fn set_bytes(&mut self, bytes: u64) {
        if self.began.is_some() {
            self.bytes = Some(bytes);
        }
    }
}

impl Drop for OpGuard {
    fn drop(&mut self) {
        let Some(began) = self.began else {
            return;
        };
        if let Some(ctx) = self.ctx.upgrade() {
            ctx.push(OpSample {
                kind: self.kind,
                label: self.label.to_string(),
                elapsed_us: began.elapsed().as_micros() as u64,
                bytes: self.bytes,
                count: 1,
            });
        }
    }
}
