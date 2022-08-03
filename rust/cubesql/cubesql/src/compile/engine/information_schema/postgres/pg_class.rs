use std::{any::Any, sync::Arc};

use async_trait::async_trait;

use datafusion::{
    arrow::{
        array::{
            Array, ArrayRef, BooleanBuilder, Float32Builder, Int16Builder, Int32Builder,
            ListBuilder, StringBuilder,
        },
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};
use mysql_common::bigdecimal::ToPrimitive;

use super::utils::{ExtDataType, Oid, OidBuilder, Xid, XidBuilder};

use crate::compile::CubeMetaTable;

struct PgClass<'a> {
    oid: Oid,
    relname: &'a str,
    relnamespace: Oid,
    reltype: Oid,
    relam: Oid,
    relfilenode: Oid,
    reltoastrelid: Oid,
    relisshared: bool,
    relkind: &'static str,
    relnatts: i16,
    relhasrules: bool,
    relreplident: &'static str,
    relfrozenxid: Xid,
    relminmxid: Xid,
}

struct PgCatalogClassBuilder {
    oid: OidBuilder,
    relname: StringBuilder,
    relnamespace: OidBuilder,
    reltype: OidBuilder,
    reloftype: OidBuilder,
    relowner: OidBuilder,
    relam: OidBuilder,
    relfilenode: OidBuilder,
    reltablespace: OidBuilder,
    relpages: Int32Builder,
    reltuples: Float32Builder,
    relallvisible: Int32Builder,
    reltoastrelid: OidBuilder,
    relhasindex: BooleanBuilder,
    relisshared: BooleanBuilder,
    relpersistence: StringBuilder,
    relkind: StringBuilder,
    relnatts: Int16Builder,
    relchecks: Int16Builder,
    relhasrules: BooleanBuilder,
    relhastriggers: BooleanBuilder,
    relhassubclass: BooleanBuilder,
    relrowsecurity: BooleanBuilder,
    relforcerowsecurity: BooleanBuilder,
    relispopulated: BooleanBuilder,
    relreplident: StringBuilder,
    relispartition: BooleanBuilder,
    relrewrite: OidBuilder,
    relfrozenxid: XidBuilder,
    relminmxid: XidBuilder,
    // TODO: type aclitem?
    relacl: ListBuilder<StringBuilder>,
    reloptions: ListBuilder<StringBuilder>,
    // TODO: type pg_node_tree?
    relpartbound: StringBuilder,
    // This column was removed after PostgreSQL 12, but it's required to support Tableau Desktop with ODBC
    // True if we generate an OID for each row of the relation
    relhasoids: BooleanBuilder,
}

impl PgCatalogClassBuilder {
    fn new() -> Self {
        let capacity = 10;

        Self {
            oid: OidBuilder::new(capacity),
            relname: StringBuilder::new(capacity),
            relnamespace: OidBuilder::new(capacity),
            reltype: OidBuilder::new(capacity),
            reloftype: OidBuilder::new(capacity),
            relowner: OidBuilder::new(capacity),
            relam: OidBuilder::new(capacity),
            relfilenode: OidBuilder::new(capacity),
            reltablespace: OidBuilder::new(capacity),
            relpages: Int32Builder::new(capacity),
            reltuples: Float32Builder::new(capacity),
            relallvisible: Int32Builder::new(capacity),
            reltoastrelid: OidBuilder::new(capacity),
            relhasindex: BooleanBuilder::new(capacity),
            relisshared: BooleanBuilder::new(capacity),
            relpersistence: StringBuilder::new(capacity),
            relkind: StringBuilder::new(capacity),
            relnatts: Int16Builder::new(capacity),
            relchecks: Int16Builder::new(capacity),
            relhasrules: BooleanBuilder::new(capacity),
            relhastriggers: BooleanBuilder::new(capacity),
            relhassubclass: BooleanBuilder::new(capacity),
            relrowsecurity: BooleanBuilder::new(capacity),
            relforcerowsecurity: BooleanBuilder::new(capacity),
            relispopulated: BooleanBuilder::new(capacity),
            relreplident: StringBuilder::new(capacity),
            relispartition: BooleanBuilder::new(capacity),
            relrewrite: OidBuilder::new(capacity),
            relfrozenxid: XidBuilder::new(capacity),
            relminmxid: XidBuilder::new(capacity),
            relacl: ListBuilder::new(StringBuilder::new(capacity)),
            reloptions: ListBuilder::new(StringBuilder::new(capacity)),
            relpartbound: StringBuilder::new(capacity),
            relhasoids: BooleanBuilder::new(capacity),
        }
    }

    fn add_class(&mut self, class: &PgClass) {
        self.oid.append_value(class.oid).unwrap();
        self.relname.append_value(class.relname).unwrap();
        self.relnamespace.append_value(class.relnamespace).unwrap();
        self.reltype.append_value(class.reltype).unwrap();
        self.reloftype.append_value(0).unwrap();
        self.relowner.append_value(10).unwrap();
        self.relam.append_value(class.relam).unwrap();
        self.relfilenode.append_value(class.relfilenode).unwrap();
        self.reltablespace.append_value(0).unwrap();
        self.relpages.append_value(1).unwrap();
        self.reltuples.append_value(-1.0).unwrap();
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
        self.relacl.append(false).unwrap();
        self.reloptions.append(false).unwrap();
        self.relpartbound.append_null().unwrap();
        self.relhasoids.append_value(false).unwrap();
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
        columns.push(Arc::new(self.relhasoids.finish()));

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
                relname: table.name.as_str(),
                relnamespace: 2200,
                reltype: table.record_oid,
                relam: 2,
                relfilenode: 0,
                reltoastrelid: 0,
                relisshared: false,
                relkind: "r",
                relnatts: table.columns.len().to_i16().unwrap_or(0),
                relhasrules: false,
                relreplident: "d",
                relfrozenxid: 1,
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
            Field::new("oid", ExtDataType::Oid.into(), false),
            Field::new("relname", DataType::Utf8, false),
            // info_schma: 13000; pg_catalog: 11; user defined tables: 2200
            Field::new("relnamespace", ExtDataType::Oid.into(), false),
            Field::new("reltype", ExtDataType::Oid.into(), false),
            Field::new("reloftype", ExtDataType::Oid.into(), false),
            Field::new("relowner", ExtDataType::Oid.into(), false),
            //user defined tables: 2; system tables: 0 | 2
            Field::new("relam", ExtDataType::Oid.into(), false),
            // TODO: to check that 0 if fine
            Field::new("relfilenode", ExtDataType::Oid.into(), false),
            Field::new("reltablespace", ExtDataType::Oid.into(), false),
            Field::new("relpages", DataType::Int32, false),
            Field::new("reltuples", DataType::Float32, false),
            Field::new("relallvisible", DataType::Int32, false),
            // TODO: sometimes is not 0. Check that 0 is fine
            Field::new("reltoastrelid", ExtDataType::Oid.into(), false),
            Field::new("relhasindex", DataType::Boolean, false),
            //user defined tables: FALSE; system tables: FALSE | TRUE
            Field::new("relisshared", DataType::Boolean, false),
            Field::new("relpersistence", DataType::Utf8, false),
            // Tables: r; Views: v
            Field::new("relkind", DataType::Utf8, false),
            // number of columns in table
            Field::new("relnatts", DataType::Int16, false),
            Field::new("relchecks", DataType::Int16, false),
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
            Field::new("relrewrite", ExtDataType::Oid.into(), false),
            // TODO: can be not 0; check that 0 is fine
            Field::new("relfrozenxid", ExtDataType::Xid.into(), false),
            // Tables: 1; Other: v
            Field::new("relminmxid", ExtDataType::Xid.into(), false),
            Field::new(
                "relacl",
                DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new(
                "reloptions",
                DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new("relpartbound", DataType::Utf8, true),
            Field::new("relhasoids", DataType::Boolean, false),
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
