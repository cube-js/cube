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
            usenames: StringBuilder::new(capacity),
            usesysids: UInt32Builder::new(capacity),
            usecreatedbs: BooleanBuilder::new(capacity),
            usesupers: BooleanBuilder::new(capacity),
            userepls: BooleanBuilder::new(capacity),
            usebypassrlss: BooleanBuilder::new(capacity),
            passwds: StringBuilder::new(capacity),
            valuntils: TimestampNanosecondBuilder::new(capacity),
            useconfigs: ListBuilder::new(StringBuilder::new(capacity)),
        }
    }

    fn add_user(&mut self, usename: impl AsRef<str>) {
        self.usenames.append_value(usename).unwrap();
        self.usesysids.append_value(10).unwrap();
        self.usecreatedbs.append_value(true).unwrap();
        self.usesupers.append_value(true).unwrap();
        self.userepls.append_value(false).unwrap();
        self.usebypassrlss.append_value(true).unwrap();
        self.passwds.append_value("********").unwrap();
        self.valuntils.append_null().unwrap();
        self.useconfigs.append(false).unwrap();
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];
        columns.push(Arc::new(self.usenames.finish()));
        columns.push(Arc::new(self.usesysids.finish()));
        columns.push(Arc::new(self.usecreatedbs.finish()));
        columns.push(Arc::new(self.usesupers.finish()));
        columns.push(Arc::new(self.userepls.finish()));
        columns.push(Arc::new(self.usebypassrlss.finish()));
        columns.push(Arc::new(self.passwds.finish()));
        columns.push(Arc::new(self.valuntils.finish()));
        columns.push(Arc::new(self.useconfigs.finish()));

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
                DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
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
