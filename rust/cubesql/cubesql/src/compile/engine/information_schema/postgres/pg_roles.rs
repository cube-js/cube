use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{
            Array, ArrayRef, BooleanBuilder, Int32Builder, ListBuilder, StringBuilder,
            TimestampNanosecondBuilder,
        },
        datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

use super::utils::{ExtDataType, OidBuilder};

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
    oid: OidBuilder,
}

impl PgCatalogRolesBuilder {
    fn new(capacity: usize) -> Self {
        Self {
            rolname: StringBuilder::new(capacity),
            rolsuper: BooleanBuilder::new(capacity),
            rolinherit: BooleanBuilder::new(capacity),
            rolcreaterole: BooleanBuilder::new(capacity),
            rolcreatedb: BooleanBuilder::new(capacity),
            rolcanlogin: BooleanBuilder::new(capacity),
            rolreplication: BooleanBuilder::new(capacity),
            rolconnlimit: Int32Builder::new(capacity),
            rolpassword: StringBuilder::new(capacity),
            rolvaliduntil: TimestampNanosecondBuilder::new(capacity),
            rolbypassrls: BooleanBuilder::new(capacity),
            rolconfig: ListBuilder::new(StringBuilder::new(capacity)),
            oid: OidBuilder::new(capacity),
        }
    }

    fn add_role(&mut self, rolname: impl AsRef<str>) {
        self.rolname.append_value(rolname).unwrap();
        self.rolsuper.append_value(true).unwrap();
        self.rolinherit.append_value(true).unwrap();
        self.rolcreaterole.append_value(false).unwrap();
        self.rolcreatedb.append_value(false).unwrap();
        self.rolcanlogin.append_value(true).unwrap();
        self.rolreplication.append_value(false).unwrap();
        self.rolconnlimit.append_value(-1).unwrap();
        self.rolpassword.append_value("********").unwrap();
        self.rolvaliduntil.append_null().unwrap();
        self.rolbypassrls.append_value(true).unwrap();
        self.rolconfig.append(false).unwrap();
        self.oid.append_value(10).unwrap();
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];
        columns.push(Arc::new(self.rolname.finish()));
        columns.push(Arc::new(self.rolsuper.finish()));
        columns.push(Arc::new(self.rolinherit.finish()));
        columns.push(Arc::new(self.rolcreaterole.finish()));
        columns.push(Arc::new(self.rolcreatedb.finish()));
        columns.push(Arc::new(self.rolcanlogin.finish()));
        columns.push(Arc::new(self.rolreplication.finish()));
        columns.push(Arc::new(self.rolconnlimit.finish()));
        columns.push(Arc::new(self.rolpassword.finish()));
        columns.push(Arc::new(self.rolvaliduntil.finish()));
        columns.push(Arc::new(self.rolbypassrls.finish()));
        columns.push(Arc::new(self.rolconfig.finish()));
        columns.push(Arc::new(self.oid.finish()));

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
            Field::new("rolpassword", DataType::Utf8, true),
            Field::new(
                "rolvaliduntil",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new("rolbypassrls", DataType::Boolean, false),
            Field::new(
                "rolconfig",
                DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new("oid", ExtDataType::Oid.into(), false),
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
