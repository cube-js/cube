use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{
            Array, ArrayRef, BooleanBuilder, Int32Builder, ListBuilder, StringBuilder,
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

struct PgCatalogRolesBuilder {
    rolname: StringBuilder,
    rolsuper: BooleanBuilder,
    rolinherit: BooleanBuilder,
    rolcreaterole: BooleanBuilder,
    rolcreatedb: BooleanBuilder,
    rolcanlogin: BooleanBuilder,
    rolreplication: BooleanBuilder,
    rolconnlimit: Int32Builder,
    rolpassword: StringBuilder,
    rolvaliduntil: TimestampNanosecondBuilder,
    rolbypassrls: BooleanBuilder,
    rolconfig: ListBuilder<StringBuilder>,
    oid: UInt32Builder,
}

impl PgCatalogRolesBuilder {
    fn new(capacity: usize) -> Self {
        Self {
            rolname: StringBuilder::with_capacity(capacity, capacity),
            rolsuper: BooleanBuilder::with_capacity(capacity),
            rolinherit: BooleanBuilder::with_capacity(capacity),
            rolcreaterole: BooleanBuilder::with_capacity(capacity),
            rolcreatedb: BooleanBuilder::with_capacity(capacity),
            rolcanlogin: BooleanBuilder::with_capacity(capacity),
            rolreplication: BooleanBuilder::with_capacity(capacity),
            rolconnlimit: Int32Builder::with_capacity(capacity),
            rolpassword: StringBuilder::with_capacity(capacity, capacity),
            rolvaliduntil: TimestampNanosecondBuilder::with_capacity(capacity),
            rolbypassrls: BooleanBuilder::with_capacity(capacity),
            rolconfig: ListBuilder::new(StringBuilder::with_capacity(capacity, capacity)),
            oid: UInt32Builder::with_capacity(capacity),
        }
    }

    fn add_role(&mut self, rolname: &str) {
        self.rolname.append_value(rolname);
        self.rolsuper.append_value(true);
        self.rolinherit.append_value(true);
        self.rolcreaterole.append_value(false);
        self.rolcreatedb.append_value(true);
        self.rolcanlogin.append_value(true);
        self.rolreplication.append_value(false);
        self.rolconnlimit.append_value(-1);
        self.rolpassword.append_value("********");
        self.rolvaliduntil.append_null();
        self.rolbypassrls.append_value(true);
        self.rolconfig.append(false);
        self.oid.append_value(10);
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(self.rolname.finish()),
            Arc::new(self.rolsuper.finish()),
            Arc::new(self.rolinherit.finish()),
            Arc::new(self.rolcreaterole.finish()),
            Arc::new(self.rolcreatedb.finish()),
            Arc::new(self.rolcanlogin.finish()),
            Arc::new(self.rolreplication.finish()),
            Arc::new(self.rolconnlimit.finish()),
            Arc::new(self.rolpassword.finish()),
            Arc::new(self.rolvaliduntil.finish()),
            Arc::new(self.rolbypassrls.finish()),
            Arc::new(self.rolconfig.finish()),
            Arc::new(self.oid.finish()),
        ];

        columns
    }
}

pub struct PgCatalogRolesProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl PgCatalogRolesProvider {
    pub fn new(role: &str) -> Self {
        let mut builder = PgCatalogRolesBuilder::new(1);
        builder.add_role(role);

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for PgCatalogRolesProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("rolname", DataType::Utf8, false),
            Field::new("rolsuper", DataType::Boolean, false),
            Field::new("rolinherit", DataType::Boolean, false),
            Field::new("rolcreaterole", DataType::Boolean, false),
            Field::new("rolcreatedb", DataType::Boolean, false),
            Field::new("rolcanlogin", DataType::Boolean, false),
            Field::new("rolreplication", DataType::Boolean, false),
            Field::new("rolconnlimit", DataType::Int32, false),
            Field::new("rolpassword", DataType::Utf8, false),
            Field::new(
                "rolvaliduntil",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new("rolbypassrls", DataType::Boolean, false),
            Field::new(
                "rolconfig",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new("oid", DataType::UInt32, false),
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
