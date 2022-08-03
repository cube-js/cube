use std::{any::Any, sync::Arc};

use async_trait::async_trait;

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, BooleanBuilder, Int32Builder, ListBuilder, StringBuilder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

use super::utils::{ExtDataType, Oid, OidBuilder, XidBuilder};

struct PgDatabase<'a> {
    oid: Oid,
    datname: &'a str,
}

struct PgCatalogDatabaseBuilder {
    oid: OidBuilder,
    datname: StringBuilder,
    datdba: OidBuilder,
    encoding: Int32Builder,
    datcollate: StringBuilder,
    datctype: StringBuilder,
    datistemplate: BooleanBuilder,
    datallowconn: BooleanBuilder,
    datconnlimit: Int32Builder,
    datlastsysoid: OidBuilder,
    datfrozenxid: XidBuilder,
    datminmxid: XidBuilder,
    dattablespace: OidBuilder,
    // TODO: type aclitem?
    datacl: ListBuilder<StringBuilder>,
}

impl PgCatalogDatabaseBuilder {
    fn new() -> Self {
        let capacity = 1;

        Self {
            oid: OidBuilder::new(capacity),
            datname: StringBuilder::new(capacity),
            datdba: OidBuilder::new(capacity),
            encoding: Int32Builder::new(capacity),
            datcollate: StringBuilder::new(capacity),
            datctype: StringBuilder::new(capacity),
            datistemplate: BooleanBuilder::new(capacity),
            datallowconn: BooleanBuilder::new(capacity),
            datconnlimit: Int32Builder::new(capacity),
            datlastsysoid: OidBuilder::new(capacity),
            datfrozenxid: XidBuilder::new(capacity),
            datminmxid: XidBuilder::new(capacity),
            dattablespace: OidBuilder::new(capacity),
            datacl: ListBuilder::new(StringBuilder::new(capacity)),
        }
    }

    fn add_database(&mut self, database: &PgDatabase) {
        self.oid.append_value(database.oid).unwrap();
        self.datname.append_value(database.datname).unwrap();
        self.datdba.append_value(10).unwrap();
        self.encoding.append_value(6).unwrap();
        self.datcollate.append_value("en_US.utf8").unwrap();
        self.datctype.append_value("en_US.utf8").unwrap();
        self.datistemplate.append_value(false).unwrap();
        self.datallowconn.append_value(true).unwrap();
        self.datconnlimit.append_value(-1).unwrap();
        self.datlastsysoid.append_value(13756).unwrap();
        self.datfrozenxid.append_value(727).unwrap();
        self.datminmxid.append_value(1).unwrap();
        self.dattablespace.append_value(1663).unwrap();
        self.datacl.append(false).unwrap();
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];
        columns.push(Arc::new(self.oid.finish()));
        columns.push(Arc::new(self.datname.finish()));
        columns.push(Arc::new(self.datdba.finish()));
        columns.push(Arc::new(self.encoding.finish()));
        columns.push(Arc::new(self.datcollate.finish()));
        columns.push(Arc::new(self.datctype.finish()));
        columns.push(Arc::new(self.datistemplate.finish()));
        columns.push(Arc::new(self.datallowconn.finish()));
        columns.push(Arc::new(self.datconnlimit.finish()));
        columns.push(Arc::new(self.datlastsysoid.finish()));
        columns.push(Arc::new(self.datfrozenxid.finish()));
        columns.push(Arc::new(self.datminmxid.finish()));
        columns.push(Arc::new(self.dattablespace.finish()));
        columns.push(Arc::new(self.datacl.finish()));

        columns
    }
}

pub struct PgCatalogDatabaseProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl PgCatalogDatabaseProvider {
    pub fn new(datname: &str) -> Self {
        let mut builder = PgCatalogDatabaseBuilder::new();
        builder.add_database(&PgDatabase {
            oid: 13757,
            datname,
        });

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for PgCatalogDatabaseProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("oid", ExtDataType::Oid.into(), false),
            Field::new("datname", DataType::Utf8, false),
            Field::new("datdba", ExtDataType::Oid.into(), false),
            Field::new("encoding", DataType::Int32, false),
            Field::new("datcollate", DataType::Utf8, false),
            Field::new("datctype", DataType::Utf8, false),
            Field::new("datistemplate", DataType::Boolean, false),
            Field::new("datallowconn", DataType::Boolean, false),
            Field::new("datconnlimit", DataType::Int32, false),
            Field::new("datlastsysoid", ExtDataType::Oid.into(), false),
            Field::new("datfrozenxid", ExtDataType::Xid.into(), false),
            Field::new("datminmxid", ExtDataType::Xid.into(), false),
            Field::new("dattablespace", ExtDataType::Oid.into(), false),
            Field::new(
                "datacl",
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
