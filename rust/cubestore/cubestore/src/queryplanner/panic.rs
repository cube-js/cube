use crate::queryplanner::planning::WorkerExec;
use async_trait::async_trait;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, Extension, LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::cmp::Ordering;
use std::fmt::{Formatter, Pointer};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[derive(Debug, Clone, Hash, Ord, PartialOrd, Eq, PartialEq)]
pub struct PanicWorkerNode {}

impl PanicWorkerNode {
    pub fn into_plan(self) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(self),
        })
    }

    pub fn from_serialized(inputs: &[LogicalPlan], serialized: PanicWorkerSerialized) -> Self {
        assert_eq!(0, inputs.len());
        let PanicWorkerSerialized {} = serialized;
        Self {}
    }

    pub fn to_serialized(&self) -> PanicWorkerSerialized {
        PanicWorkerSerialized {}
    }
}

lazy_static! {
    static ref EMPTY_SCHEMA: DFSchemaRef = Arc::new(DFSchema::empty());
}

impl UserDefinedLogicalNode for PanicWorkerNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "PanicWorker"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &EMPTY_SCHEMA
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain<'a>(&self, f: &mut Formatter<'a>) -> std::fmt::Result {
        write!(f, "Panic")
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::common::Result<Arc<dyn UserDefinedLogicalNode>> {
        assert!(exprs.is_empty());
        assert!(inputs.is_empty());

        Ok(Arc::new(PanicWorkerNode {}))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.hash(&mut s);
    }

    fn dyn_eq(&self, other: &dyn UserDefinedLogicalNode) -> bool {
        other
            .as_any()
            .downcast_ref()
            .map(|o| self.eq(o))
            .unwrap_or(false)
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct PanicWorkerSerialized {}

#[derive(Debug)]
pub struct PanicWorkerExec {
    properties: PlanProperties,
}

impl PanicWorkerExec {
    pub fn new() -> PanicWorkerExec {
        PanicWorkerExec {
            properties: PlanProperties::new(
                EquivalenceProperties::new(Arc::new(Schema::empty())),
                Partitioning::UnknownPartitioning(1),
                ExecutionMode::Bounded,
            ),
        }
    }
}

impl DisplayAs for PanicWorkerExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "PanicWorkerExec")
    }
}

#[async_trait]
impl ExecutionPlan for PanicWorkerExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        assert_eq!(children.len(), 0);
        Ok(Arc::new(PanicWorkerExec::new()))
    }

    fn execute(
        &self,
        partition: usize,
        _: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        assert_eq!(partition, 0);
        panic!("worker panic")
    }

    fn name(&self) -> &str {
        "PanicWorkerExec"
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }
}

pub fn plan_panic_worker() -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    Ok(Arc::new(WorkerExec {
        input: Arc::new(PanicWorkerExec::new()),
        max_batch_rows: 1,
        limit_and_reverse: None,
    }))
}
