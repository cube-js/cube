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
    Execution,
    WarmupIo,
    ChunkLoad,
    Other,
}

/// A single (aggregated) measurement: how long a class of operations took and,
/// optionally, how many bytes it moved / rows it produced. Repeats with the same
/// `(kind, label)` are folded together on insertion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpSample {
    pub kind: OpKind,
    pub label: String,
    pub elapsed_us: u64,
    pub bytes: Option<u64>,
    pub rows: Option<u64>,
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

/// Trace of the entry node that received the query (parse + planning).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RouterTrace {
    pub ops: Vec<OpSample>,
}

/// Trace assembled on the execution main (the worker that runs the router plan):
/// its own ops + the per-worker traces it collected through ClusterSend. Shipped
/// back to the entry node.
///
/// `ops` holds the main's stage guards (`main.router_physical_plan`, `main.execute`)
/// plus per-node DataFusion `elapsed_compute` of the final stages (OpKind::Execution).
/// `exec_memory_peak_bytes` is the peak of operator reservations during execution
/// (sort/aggregate/join buffers — not every allocation).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MainTrace {
    pub node_name: String,
    pub ops: Vec<OpSample>,
    pub exec_memory_peak_bytes: Option<u64>,
    pub physical_plan: Option<String>,
    pub workers: Vec<WorkerTrace>,
}

/// Whole-query trace assembled on the entry node.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryTrace {
    pub router: RouterTrace,
    pub main: Option<MainTrace>,
}

/// Per-query sink for one process-region. Interior-mutable so recording helpers
/// can write through a shared `Arc` from anywhere in the query's task.
pub struct TraceCtx {
    ops: Mutex<Vec<OpSample>>,
    plan_text: Mutex<Option<String>>,
}

impl TraceCtx {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            ops: Mutex::new(Vec::new()),
            plan_text: Mutex::new(None),
        })
    }

    pub fn take_plan_text(&self) -> Option<String> {
        self.plan_text.lock().unwrap().take()
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
            existing.rows = match (existing.rows, sample.rows) {
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

/// Collects per-worker traces on the execution main. Passed to `ClusterSendExec`
/// through the `TaskContext` session config so it survives DataFusion's internal
/// task spawning (a task-local would not).
#[derive(Default)]
pub struct WorkerTraceCollector {
    traces: Mutex<Vec<WorkerTrace>>,
}

impl WorkerTraceCollector {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn push(&self, trace: WorkerTrace) {
        self.traces.lock().unwrap().push(trace);
    }

    pub fn take(&self) -> Vec<WorkerTrace> {
        std::mem::take(&mut self.traces.lock().unwrap())
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

/// Record an already-measured sample into the active trace (no-op when off).
/// For values not timed by an `OpGuard` — e.g. DataFusion node metrics.
pub fn record_op(
    kind: OpKind,
    label: &str,
    elapsed_us: u64,
    bytes: Option<u64>,
    rows: Option<u64>,
    count: u32,
) {
    if let Some(ctx) = current_trace() {
        ctx.push(OpSample {
            kind,
            label: label.to_string(),
            elapsed_us,
            bytes,
            rows,
            count,
        });
    }
}

/// Stash the executed physical plan text into the active trace (no-op when off).
pub fn set_plan_text(text: String) {
    if let Some(ctx) = current_trace() {
        *ctx.plan_text.lock().unwrap() = Some(text);
    }
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
                rows: None,
                count: 1,
            });
        }
    }
}
