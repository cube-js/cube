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

struct RedshiftStlQueryBuilder {
    userids: Int32Builder,
    queries: Int32Builder,
    labels: StringBuilder,
    xids: Int64Builder,
    pids: Int32Builder,
    databases: StringBuilder,
    querytxts: StringBuilder,
    // FIXME: these must be timestamps but are compared directly to strings in Redshift
    starttimes: StringBuilder,
    endtimes: StringBuilder,
    aborted: Int32Builder,
    insert_pristine: Int32Builder,
    concurrency_scaling_statuses: Int32Builder,
}

impl RedshiftStlQueryBuilder {
    fn new() -> Self {
        let capacity = 1;
        Self {
            userids: Int32Builder::new(capacity),
            queries: Int32Builder::new(capacity),
            labels: StringBuilder::new(capacity),
            xids: Int64Builder::new(capacity),
            pids: Int32Builder::new(capacity),
            databases: StringBuilder::new(capacity),
            querytxts: StringBuilder::new(capacity),
            // FIXME: these must be timestamps but are compared directly to strings in Redshift
            starttimes: StringBuilder::new(capacity),
            endtimes: StringBuilder::new(capacity),
            aborted: Int32Builder::new(capacity),
            insert_pristine: Int32Builder::new(capacity),
            concurrency_scaling_statuses: Int32Builder::new(capacity),
        }
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];
        columns.push(Arc::new(self.userids.finish()));
        columns.push(Arc::new(self.queries.finish()));
        columns.push(Arc::new(self.labels.finish()));
        columns.push(Arc::new(self.xids.finish()));
        columns.push(Arc::new(self.pids.finish()));
        columns.push(Arc::new(self.databases.finish()));
        columns.push(Arc::new(self.querytxts.finish()));
        columns.push(Arc::new(self.starttimes.finish()));
        columns.push(Arc::new(self.endtimes.finish()));
        columns.push(Arc::new(self.aborted.finish()));
        columns.push(Arc::new(self.insert_pristine.finish()));
        columns.push(Arc::new(self.concurrency_scaling_statuses.finish()));

        columns
    }
}

pub struct RedshiftStlQueryProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl RedshiftStlQueryProvider {
    pub fn new() -> Self {
        let builder = RedshiftStlQueryBuilder::new();

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for RedshiftStlQueryProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("userid", DataType::Int32, false),
            Field::new("query", DataType::Int32, false),
            Field::new("label", DataType::Utf8, false),
            Field::new("xid", DataType::Int64, false),
            Field::new("pid", DataType::Int32, false),
            Field::new("database", DataType::Utf8, false),
            Field::new("querytxt", DataType::Utf8, false),
            // FIXME: these must be timestamps but are compared directly to strings in Redshift
            Field::new("starttime", DataType::Utf8, false),
            Field::new("endtime", DataType::Utf8, false),
            Field::new("aborted", DataType::Int32, false),
            Field::new("insert_pristine", DataType::Int32, false),
            Field::new("concurrency_scaling_status", DataType::Int32, false),
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
