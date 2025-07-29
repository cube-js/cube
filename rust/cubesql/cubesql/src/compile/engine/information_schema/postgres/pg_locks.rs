use std::{any::Any, sync::Arc};

use async_trait::async_trait;

use datafusion::{
    arrow::{
        array::{
            Array, ArrayRef, BooleanBuilder, Int16Builder, Int32Builder, StringBuilder,
            TimestampNanosecondBuilder, UInt32Builder,
        },
        datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

struct PgCatalogLocksBuilder {
    locktype: StringBuilder,
    database: UInt32Builder,
    relation: UInt32Builder,
    page: Int32Builder,
    tuple: Int16Builder,
    virtualxid: StringBuilder,
    transactionid: UInt32Builder,
    classid: UInt32Builder,
    objid: UInt32Builder,
    objsubid: Int16Builder,
    virtualtransaction: StringBuilder,
    pid: Int32Builder,
    mode: StringBuilder,
    granted: BooleanBuilder,
    fastpath: BooleanBuilder,
    waitstart: TimestampNanosecondBuilder,
}

impl PgCatalogLocksBuilder {
    fn new() -> Self {
        let capacity = 1;

        Self {
            locktype: StringBuilder::new(capacity),
            database: UInt32Builder::new(capacity),
            relation: UInt32Builder::new(capacity),
            page: Int32Builder::new(capacity),
            tuple: Int16Builder::new(capacity),
            virtualxid: StringBuilder::new(capacity),
            transactionid: UInt32Builder::new(capacity),
            classid: UInt32Builder::new(capacity),
            objid: UInt32Builder::new(capacity),
            objsubid: Int16Builder::new(capacity),
            virtualtransaction: StringBuilder::new(capacity),
            pid: Int32Builder::new(capacity),
            mode: StringBuilder::new(capacity),
            granted: BooleanBuilder::new(capacity),
            fastpath: BooleanBuilder::new(capacity),
            waitstart: TimestampNanosecondBuilder::new(capacity),
        }
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(self.locktype.finish()),
            Arc::new(self.database.finish()),
            Arc::new(self.relation.finish()),
            Arc::new(self.page.finish()),
            Arc::new(self.tuple.finish()),
            Arc::new(self.virtualxid.finish()),
            Arc::new(self.transactionid.finish()),
            Arc::new(self.classid.finish()),
            Arc::new(self.objid.finish()),
            Arc::new(self.objsubid.finish()),
            Arc::new(self.virtualtransaction.finish()),
            Arc::new(self.pid.finish()),
            Arc::new(self.mode.finish()),
            Arc::new(self.granted.finish()),
            Arc::new(self.fastpath.finish()),
            Arc::new(self.waitstart.finish()),
        ];

        columns
    }
}

pub struct PgCatalogLocksProvider {
    data: Arc<Vec<ArrayRef>>,
}

// https://www.postgresql.org/docs/14/view-pg-locks.html
impl PgCatalogLocksProvider {
    pub fn new() -> Self {
        let builder = PgCatalogLocksBuilder::new();

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for PgCatalogLocksProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("locktype", DataType::Utf8, true),
            Field::new("database", DataType::UInt32, true),
            Field::new("relation", DataType::UInt32, true),
            Field::new("page", DataType::Int32, true),
            Field::new("tuple", DataType::Int16, true),
            Field::new("virtualxid", DataType::Utf8, true),
            Field::new("transactionid", DataType::UInt32, true),
            Field::new("classid", DataType::UInt32, true),
            Field::new("objid", DataType::UInt32, true),
            Field::new("objsubid", DataType::Int16, true),
            Field::new("virtualtransaction", DataType::Utf8, true),
            Field::new("pid", DataType::Int32, true),
            Field::new("mode", DataType::Utf8, true),
            Field::new("granted", DataType::Boolean, true),
            Field::new("fastpath", DataType::Boolean, true),
            Field::new(
                "waitstart",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
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
