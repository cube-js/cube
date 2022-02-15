use async_trait::async_trait;
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;
use arrow::datatypes::{Schema, SchemaRef};
use datafusion::error::DataFusionError;
use datafusion::logical_plan::{DFSchema, DFSchemaRef, Expr, LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::{ExecutionPlan, OptimizerHints, Partitioning, SendableRecordBatchStream};

#[derive(Debug, Clone)]
pub struct PanicNode {
}

impl PanicNode {
    pub fn into_plan(self) -> LogicalPlan {
        LogicalPlan::Extension {
            node: Arc::new(self),
        }
    }
}

lazy_static! {
    static ref EMPTY_SCHEMA: DFSchemaRef = Arc::new(DFSchema::empty());
}

impl UserDefinedLogicalNode for PanicNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef { &EMPTY_SCHEMA }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter<'a>) -> std::fmt::Result {
        write!(f, "Panic")
    }

    fn from_template(
        &self,
        exprs: &[Expr],
        inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode + Send + Sync> {
        assert!(exprs.is_empty());
        assert!(inputs.is_empty());

        Arc::new(PanicNode {})
    }
}

#[derive(Debug)]
pub struct PanicExec {
}

impl PanicExec {
    pub fn new() -> PanicExec {
        PanicExec {}
    }
}

#[async_trait]
impl ExecutionPlan for PanicExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::empty())
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        assert_eq!(children.len(), 0);
        Ok(Arc::new(PanicExec::new()))
    }

    fn output_hints(&self) -> OptimizerHints {
        OptimizerHints::default()
    }

    async fn execute(
        &self,
        partition: usize,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        assert_eq!(partition, 0);
        panic!("worker panic")
    }
}

pub fn plan_panic() -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    Ok(Arc::new(PanicExec::new()))
}