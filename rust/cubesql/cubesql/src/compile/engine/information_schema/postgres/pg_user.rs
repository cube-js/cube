use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{
            Array, ArrayRef, BooleanBuilder, ListBuilder, StringBuilder,
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

struct PgCatalogUserBuilder {
    usenames: StringBuilder,
    usesysids: UInt32Builder,
    usecreatedbs: BooleanBuilder,
    usesupers: BooleanBuilder,
    userepls: BooleanBuilder,
    usebypassrlss: BooleanBuilder,
    passwds: StringBuilder,
    valuntils: TimestampNanosecondBuilder,
    useconfigs: ListBuilder<StringBuilder>,
}

impl PgCatalogUserBuilder {
    fn new(capacity: usize) -> Self {
        Self {
            usenames: StringBuilder::with_capacity(capacity, capacity),
            usesysids: UInt32Builder::with_capacity(capacity),
            usecreatedbs: BooleanBuilder::with_capacity(capacity),
            usesupers: BooleanBuilder::with_capacity(capacity),
            userepls: BooleanBuilder::with_capacity(capacity),
            usebypassrlss: BooleanBuilder::with_capacity(capacity),
            passwds: StringBuilder::with_capacity(capacity, capacity),
            valuntils: TimestampNanosecondBuilder::with_capacity(capacity),
            useconfigs: ListBuilder::new(StringBuilder::with_capacity(capacity, capacity)),
        }
    }

    fn add_user(&mut self, usename: impl AsRef<str>) {
        self.usenames.append_value(usename);
        self.usesysids.append_value(10);
        self.usecreatedbs.append_value(true);
        self.usesupers.append_value(true);
        self.userepls.append_value(false);
        self.usebypassrlss.append_value(true);
        self.passwds.append_value("********");
        self.valuntils.append_null();
        self.useconfigs.append(false);
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(self.usenames.finish()),
            Arc::new(self.usesysids.finish()),
            Arc::new(self.usecreatedbs.finish()),
            Arc::new(self.usesupers.finish()),
            Arc::new(self.userepls.finish()),
            Arc::new(self.usebypassrlss.finish()),
            Arc::new(self.passwds.finish()),
            Arc::new(self.valuntils.finish()),
            Arc::new(self.useconfigs.finish()),
        ];

        columns
    }
}

pub struct PgCatalogUserProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl PgCatalogUserProvider {
    pub fn new(user: impl AsRef<str>) -> Self {
        let mut builder = PgCatalogUserBuilder::new(1);

        builder.add_user(&user);

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for PgCatalogUserProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("usename", DataType::Utf8, false),
            Field::new("usesysid", DataType::UInt32, false),
            Field::new("usecreatedb", DataType::Boolean, false),
            Field::new("usesuper", DataType::Boolean, false),
            Field::new("userepl", DataType::Boolean, false),
            Field::new("usebypassrls", DataType::Boolean, false),
            Field::new("passwd", DataType::Utf8, false),
            Field::new(
                "valuntil",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new(
                "useconfig",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
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
