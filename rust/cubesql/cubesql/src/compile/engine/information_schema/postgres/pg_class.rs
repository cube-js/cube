use std::{any::Any, sync::Arc};

use async_trait::async_trait;

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, BooleanBuilder, Int32Builder, StringBuilder, UInt32Builder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};
use mysql_common::bigdecimal::ToPrimitive;

use crate::compile::CubeMetaTable;

struct PgClass {
    oid: u32,
    relname: String,
    relnamespace: u32,
    reltype: u32,
    relam: u32,
    relfilenode: u32,
    reltoastrelid: u32,
    relisshared: bool,
    relkind: String,
    relnatts: i32,
    relhasrules: bool,
    relreplident: String,
    relfrozenxid: i32,
    relminmxid: i32,
}

struct PgCatalogClassBuilder {
    oid: UInt32Builder,
    relname: StringBuilder,
    relnamespace: UInt32Builder,
    reltype: UInt32Builder,
    reloftype: UInt32Builder,
    relowner: UInt32Builder,
    relam: UInt32Builder,
    relfilenode: UInt32Builder,
    reltablespace: UInt32Builder,
    relpages: Int32Builder,
    reltuples: Int32Builder,
    relallvisible: Int32Builder,
    reltoastrelid: UInt32Builder,
    relhasindex: BooleanBuilder,
    relisshared: BooleanBuilder,
    relpersistence: StringBuilder,
    relkind: StringBuilder,
    relnatts: Int32Builder,
    relchecks: Int32Builder,
    relhasrules: BooleanBuilder,
    relhastriggers: BooleanBuilder,
    relhassubclass: BooleanBuilder,
    relrowsecurity: BooleanBuilder,
    relforcerowsecurity: BooleanBuilder,
    relispopulated: BooleanBuilder,
    relreplident: StringBuilder,
    relispartition: BooleanBuilder,
    relrewrite: UInt32Builder,
    relfrozenxid: Int32Builder,
    relminmxid: Int32Builder,
    relacl: StringBuilder,
    reloptions: StringBuilder,
    relpartbound: StringBuilder,
}

impl PgCatalogClassBuilder {
    fn new() -> Self {
        let capacity = 10;

        Self {
            oid: UInt32Builder::new(capacity),
            relname: StringBuilder::new(capacity),
            relnamespace: UInt32Builder::new(capacity),
            reltype: UInt32Builder::new(capacity),
            reloftype: UInt32Builder::new(capacity),
            relowner: UInt32Builder::new(capacity),
            relam: UInt32Builder::new(capacity),
            relfilenode: UInt32Builder::new(capacity),
            reltablespace: UInt32Builder::new(capacity),
            relpages: Int32Builder::new(capacity),
            reltuples: Int32Builder::new(capacity),
            relallvisible: Int32Builder::new(capacity),
            reltoastrelid: UInt32Builder::new(capacity),
            relhasindex: BooleanBuilder::new(capacity),
            relisshared: BooleanBuilder::new(capacity),
            relpersistence: StringBuilder::new(capacity),
            relkind: StringBuilder::new(capacity),
            relnatts: Int32Builder::new(capacity),
            relchecks: Int32Builder::new(capacity),
            relhasrules: BooleanBuilder::new(capacity),
            relhastriggers: BooleanBuilder::new(capacity),
            relhassubclass: BooleanBuilder::new(capacity),
            relrowsecurity: BooleanBuilder::new(capacity),
            relforcerowsecurity: BooleanBuilder::new(capacity),
            relispopulated: BooleanBuilder::new(capacity),
            relreplident: StringBuilder::new(capacity),
            relispartition: BooleanBuilder::new(capacity),
            relrewrite: UInt32Builder::new(capacity),
            relfrozenxid: Int32Builder::new(capacity),
            relminmxid: Int32Builder::new(capacity),
            relacl: StringBuilder::new(capacity),
            reloptions: StringBuilder::new(capacity),
            relpartbound: StringBuilder::new(capacity),
        }
    }

    fn add_class(&mut self, class: &PgClass) {
        self.oid.append_value(class.oid).unwrap();
        self.relname.append_value(&class.relname).unwrap();
        self.relnamespace.append_value(class.relnamespace).unwrap();
        self.reltype.append_value(class.reltype).unwrap();
        self.reloftype.append_value(0).unwrap();
        self.relowner.append_value(10).unwrap();
        self.relam.append_value(class.relam).unwrap();
        self.relfilenode.append_value(class.relfilenode).unwrap();
        self.reltablespace.append_value(0).unwrap();
        self.relpages.append_value(0).unwrap();
        self.reltuples.append_value(-1).unwrap();
        self.relallvisible.append_value(0).unwrap();
        self.reltoastrelid
            .append_value(class.reltoastrelid)
            .unwrap();
        self.relhasindex.append_value(false).unwrap();
        self.relisshared.append_value(class.relisshared).unwrap();
        self.relpersistence.append_value("p").unwrap();
        self.relkind.append_value(&class.relkind).unwrap();
        self.relnatts.append_value(class.relnatts).unwrap();
        self.relchecks.append_value(0).unwrap();
        self.relhasrules.append_value(class.relhasrules).unwrap();
        self.relhastriggers.append_value(false).unwrap();
        self.relhassubclass.append_value(false).unwrap();
        self.relrowsecurity.append_value(false).unwrap();
        self.relforcerowsecurity.append_value(false).unwrap();
        self.relispopulated.append_value(true).unwrap();
        self.relreplident.append_value(&class.relreplident).unwrap();
        self.relispartition.append_value(false).unwrap();
        self.relrewrite.append_value(0).unwrap();
        self.relfrozenxid.append_value(class.relfrozenxid).unwrap();
        self.relminmxid.append_value(class.relminmxid).unwrap();
        self.relacl.append_null().unwrap();
        self.reloptions.append_null().unwrap();
        self.relpartbound.append_null().unwrap();
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];
        columns.push(Arc::new(self.oid.finish()));
        columns.push(Arc::new(self.relname.finish()));
        columns.push(Arc::new(self.relnamespace.finish()));
        columns.push(Arc::new(self.reltype.finish()));
        columns.push(Arc::new(self.reloftype.finish()));
        columns.push(Arc::new(self.relowner.finish()));
        columns.push(Arc::new(self.relam.finish()));
        columns.push(Arc::new(self.relfilenode.finish()));
        columns.push(Arc::new(self.reltablespace.finish()));
        columns.push(Arc::new(self.relpages.finish()));
        columns.push(Arc::new(self.reltuples.finish()));
        columns.push(Arc::new(self.relallvisible.finish()));
        columns.push(Arc::new(self.reltoastrelid.finish()));
        columns.push(Arc::new(self.relhasindex.finish()));
        columns.push(Arc::new(self.relisshared.finish()));
        columns.push(Arc::new(self.relpersistence.finish()));
        columns.push(Arc::new(self.relkind.finish()));
        columns.push(Arc::new(self.relnatts.finish()));
        columns.push(Arc::new(self.relchecks.finish()));
        columns.push(Arc::new(self.relhasrules.finish()));
        columns.push(Arc::new(self.relhastriggers.finish()));
        columns.push(Arc::new(self.relhassubclass.finish()));
        columns.push(Arc::new(self.relrowsecurity.finish()));
        columns.push(Arc::new(self.relforcerowsecurity.finish()));
        columns.push(Arc::new(self.relispopulated.finish()));
        columns.push(Arc::new(self.relreplident.finish()));
        columns.push(Arc::new(self.relispartition.finish()));
        columns.push(Arc::new(self.relrewrite.finish()));
        columns.push(Arc::new(self.relfrozenxid.finish()));
        columns.push(Arc::new(self.relminmxid.finish()));
        columns.push(Arc::new(self.relacl.finish()));
        columns.push(Arc::new(self.reloptions.finish()));
        columns.push(Arc::new(self.relpartbound.finish()));

        columns
    }
}

pub struct PgCatalogClassProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl PgCatalogClassProvider {
    pub fn new(cube_tables: &Vec<CubeMetaTable>) -> Self {
        let mut builder = PgCatalogClassBuilder::new();

        for table in cube_tables.iter() {
            builder.add_class(&PgClass {
                oid: table.oid,
                relname: table.name.clone(),
                relnamespace: 2200,
                reltype: table.record_oid,
                relam: 2,
                relfilenode: 0,
                reltoastrelid: 0,
                relisshared: false,
                relkind: "r".to_string(),
                relnatts: table.columns.len().to_i32().unwrap_or(0),
                relhasrules: false,
                relreplident: "p".to_string(),
                relfrozenxid: 0,
                relminmxid: 1,
            });
        }

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for PgCatalogClassProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::UInt32, false),
            Field::new("relname", DataType::Utf8, false),
            // info_schma: 13391; pg_catalog: 11; user defined tables: 2200
            Field::new("relnamespace", DataType::UInt32, false),
            Field::new("reltype", DataType::UInt32, false),
            Field::new("reloftype", DataType::UInt32, false),
            Field::new("relowner", DataType::UInt32, false),
            //user defined tables: 2; system tables: 0 | 2
            Field::new("relam", DataType::UInt32, false),
            // TODO: to check that 0 if fine
            Field::new("relfilenode", DataType::UInt32, false),
            Field::new("reltablespace", DataType::UInt32, false),
            Field::new("relpages", DataType::Int32, false),
            Field::new("reltuples", DataType::Int32, false),
            Field::new("relallvisible", DataType::Int32, false),
            // TODO: sometimes is not 0. Check that 0 is fine
            Field::new("reltoastrelid", DataType::UInt32, false),
            Field::new("relhasindex", DataType::Boolean, false),
            //user defined tables: FALSE; system tables: FALSE | TRUE
            Field::new("relisshared", DataType::Boolean, false),
            Field::new("relpersistence", DataType::Utf8, false),
            // Tables: r; Views: v
            Field::new("relkind", DataType::Utf8, false),
            // number of columns in table
            Field::new("relnatts", DataType::Int32, false),
            Field::new("relchecks", DataType::Int32, false),
            //user defined tables: FALSE; system tables: FALSE | TRUE
            Field::new("relhasrules", DataType::Boolean, false),
            Field::new("relhastriggers", DataType::Boolean, false),
            Field::new("relhassubclass", DataType::Boolean, false),
            Field::new("relrowsecurity", DataType::Boolean, false),
            Field::new("relforcerowsecurity", DataType::Boolean, false),
            Field::new("relispopulated", DataType::Boolean, false),
            //user defined tables: p; system tables: n
            Field::new("relreplident", DataType::Utf8, false),
            Field::new("relispartition", DataType::Boolean, false),
            Field::new("relrewrite", DataType::UInt32, false),
            // TODO: can be not 0; check that 0 is fine
            Field::new("relfrozenxid", DataType::Int32, false),
            // Tables: 1; Other: v
            Field::new("relminmxid", DataType::Int32, false),
            Field::new("relacl", DataType::Utf8, true),
            Field::new("reloptions", DataType::Utf8, true),
            Field::new("relpartbound", DataType::Utf8, true),
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
