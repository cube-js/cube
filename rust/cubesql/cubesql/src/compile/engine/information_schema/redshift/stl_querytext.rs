use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{Array, ArrayRef, Int32Builder, Int64Builder, StringBuilder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

struct RedshiftStlQuerytextBuilder {
    userids: Int32Builder,
    xids: Int64Builder,
    pids: Int32Builder,
    queries: Int32Builder,
    sequences: Int32Builder,
    texts: StringBuilder,
}

impl RedshiftStlQuerytextBuilder {
    fn new() -> Self {
        let capacity = 1;
        Self {
            userids: Int32Builder::new(capacity),
            xids: Int64Builder::new(capacity),
            pids: Int32Builder::new(capacity),
            queries: Int32Builder::new(capacity),
            sequences: Int32Builder::new(capacity),
            texts: StringBuilder::new(capacity),
        }
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(self.userids.finish()),
            Arc::new(self.xids.finish()),
            Arc::new(self.pids.finish()),
            Arc::new(self.queries.finish()),
            Arc::new(self.sequences.finish()),
            Arc::new(self.texts.finish()),
        ];

        columns
    }
}

pub struct RedshiftStlQuerytextProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl RedshiftStlQuerytextProvider {
    pub fn new() -> Self {
        let builder = RedshiftStlQuerytextBuilder::new();

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for RedshiftStlQuerytextProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("userid", DataType::Int32, false),
            Field::new("xid", DataType::Int64, false),
            Field::new("pid", DataType::Int32, false),
            Field::new("query", DataType::Int32, false),
            Field::new("sequence", DataType::Int32, false),
            Field::new("text", DataType::Utf8, false),
        ]))
    }

    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let batch = RecordBatch::try_new(self.schema(), self.data.to_vec())?;

        Ok(Arc::new(MemoryExec::try_new(
            &[vec![batch]],
            self.schema(),
            projection.clone(),
        )?))
    }

    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> Result<TableProviderFilterPushDown, DataFusionError> {
        Ok(TableProviderFilterPushDown::Unsupported)
    }
}
