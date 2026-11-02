use std::{any::Any, sync::Arc};

use async_trait::async_trait;

use datafusion::{
    arrow::{
        array::{
            Array, ArrayRef, BooleanBuilder, Int32Builder, ListBuilder, StringBuilder,
            UInt32Builder,
        },
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

struct PgDatabase<'a> {
    oid: u32,
    datname: &'a str,
}

struct PgCatalogDatabaseBuilder {
    oid: UInt32Builder,
    datname: StringBuilder,
    datdba: UInt32Builder,
    encoding: Int32Builder,
    datcollate: StringBuilder,
    datctype: StringBuilder,
    datistemplate: BooleanBuilder,
    datallowconn: BooleanBuilder,
    datconnlimit: Int32Builder,
    datlastsysoid: UInt32Builder,
    datfrozenxid: UInt32Builder,
    datminmxid: UInt32Builder,
    dattablespace: UInt32Builder,
    datacl: ListBuilder<StringBuilder>,
}

impl PgCatalogDatabaseBuilder {
    fn new() -> Self {
        let capacity = 1;

        Self {
            oid: UInt32Builder::with_capacity(capacity),
            datname: StringBuilder::with_capacity(capacity, capacity),
            datdba: UInt32Builder::with_capacity(capacity),
            encoding: Int32Builder::with_capacity(capacity),
            datcollate: StringBuilder::with_capacity(capacity, capacity),
            datctype: StringBuilder::with_capacity(capacity, capacity),
            datistemplate: BooleanBuilder::with_capacity(capacity),
            datallowconn: BooleanBuilder::with_capacity(capacity),
            datconnlimit: Int32Builder::with_capacity(capacity),
            datlastsysoid: UInt32Builder::with_capacity(capacity),
            datfrozenxid: UInt32Builder::with_capacity(capacity),
            datminmxid: UInt32Builder::with_capacity(capacity),
            dattablespace: UInt32Builder::with_capacity(capacity),
            datacl: ListBuilder::new(StringBuilder::with_capacity(capacity, capacity)),
        }
    }

    fn add_database(&mut self, database: &PgDatabase) {
        self.oid.append_value(database.oid);
        self.datname.append_value(database.datname);
        self.datdba.append_value(10);
        self.encoding.append_value(6);
        self.datcollate.append_value("en_US.utf8");
        self.datctype.append_value("en_US.utf8");
        self.datistemplate.append_value(false);
        self.datallowconn.append_value(true);
        self.datconnlimit.append_value(-1);
        self.datlastsysoid.append_value(13756);
        self.datfrozenxid.append_value(727);
        self.datminmxid.append_value(1);
        self.dattablespace.append_value(1663);
        self.datacl.append(false);
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(self.oid.finish()),
            Arc::new(self.datname.finish()),
            Arc::new(self.datdba.finish()),
            Arc::new(self.encoding.finish()),
            Arc::new(self.datcollate.finish()),
            Arc::new(self.datctype.finish()),
            Arc::new(self.datistemplate.finish()),
            Arc::new(self.datallowconn.finish()),
            Arc::new(self.datconnlimit.finish()),
            Arc::new(self.datlastsysoid.finish()),
            Arc::new(self.datfrozenxid.finish()),
            Arc::new(self.datminmxid.finish()),
            Arc::new(self.dattablespace.finish()),
            Arc::new(self.datacl.finish()),
        ];

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
            Field::new("oid", DataType::UInt32, false),
            Field::new("datname", DataType::Utf8, false),
            Field::new("datdba", DataType::UInt32, false),
            Field::new("encoding", DataType::Int32, false),
            Field::new("datcollate", DataType::Utf8, false),
            Field::new("datctype", DataType::Utf8, false),
            Field::new("datistemplate", DataType::Boolean, false),
            Field::new("datallowconn", DataType::Boolean, false),
            Field::new("datconnlimit", DataType::Int32, false),
            Field::new("datlastsysoid", DataType::UInt32, false),
            Field::new("datfrozenxid", DataType::UInt32, false),
            Field::new("datminmxid", DataType::UInt32, false),
            Field::new("dattablespace", DataType::UInt32, false),
            Field::new(
                "datacl",
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
