use std::{any::Any, sync::Arc};

use crate::{sql::ColumnType, transport::CubeMetaTable};
use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{
            Array, ArrayRef, BooleanBuilder, Int16Builder, Int32Builder, Int64Builder, ListBuilder,
            StringBuilder,
        },
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};
use pg_srv::PgType;

use super::utils::{ExtDataType, Oid, OidBuilder};

struct PgCatalogAttributeBuilder {
    attrelid: OidBuilder,
    attname: StringBuilder,
    atttypid: OidBuilder,
    attstattarget: Int32Builder,
    attlen: Int16Builder,
    attnum: Int16Builder,
    attndims: Int32Builder,
    attcacheoff: Int32Builder,
    // TODO: Add support for casts within case and switch back to Int32
    atttypmod: Int64Builder,
    attbyval: BooleanBuilder,
    attalign: StringBuilder,
    attstorage: StringBuilder,
    attcompression: StringBuilder,
    attnotnull: BooleanBuilder,
    atthasdef: BooleanBuilder,
    atthasmissing: BooleanBuilder,
    attidentity: StringBuilder,
    attgenerated: StringBuilder,
    attisdropped: BooleanBuilder,
    attislocal: BooleanBuilder,
    attinhcount: Int32Builder,
    attcollation: OidBuilder,
    // TODO: type aclitem?
    attacl: ListBuilder<StringBuilder>,
    attoptions: ListBuilder<StringBuilder>,
    attfdwoptions: ListBuilder<StringBuilder>,
    // TODO: type anyarray?
    attmissingval: StringBuilder,
}

impl PgCatalogAttributeBuilder {
    fn new() -> Self {
        let capacity = 10;

        Self {
            attrelid: OidBuilder::new(capacity),
            attname: StringBuilder::new(capacity),
            atttypid: OidBuilder::new(capacity),
            attstattarget: Int32Builder::new(capacity),
            attlen: Int16Builder::new(capacity),
            attnum: Int16Builder::new(capacity),
            attndims: Int32Builder::new(capacity),
            attcacheoff: Int32Builder::new(capacity),
            atttypmod: Int64Builder::new(capacity),
            attbyval: BooleanBuilder::new(capacity),
            attalign: StringBuilder::new(capacity),
            attstorage: StringBuilder::new(capacity),
            attcompression: StringBuilder::new(capacity),
            attnotnull: BooleanBuilder::new(capacity),
            atthasdef: BooleanBuilder::new(capacity),
            atthasmissing: BooleanBuilder::new(capacity),
            attidentity: StringBuilder::new(capacity),
            attgenerated: StringBuilder::new(capacity),
            attisdropped: BooleanBuilder::new(capacity),
            attislocal: BooleanBuilder::new(capacity),
            attinhcount: Int32Builder::new(capacity),
            attcollation: OidBuilder::new(capacity),
            attacl: ListBuilder::new(StringBuilder::new(capacity)),
            attoptions: ListBuilder::new(StringBuilder::new(capacity)),
            attfdwoptions: ListBuilder::new(StringBuilder::new(capacity)),
            attmissingval: StringBuilder::new(capacity),
        }
    }

    fn add_attribute(
        &mut self,
        attrelid: Oid,
        attname: impl AsRef<str>,
        column_type: &ColumnType,
        attnum: i16,
        is_array: bool,
        attnotnull: bool,
    ) {
        let pg_typ = PgType::get_by_tid(column_type.to_pg_tid());

        self.attrelid.append_value(attrelid).unwrap();
        self.attname.append_value(attname).unwrap();
        self.atttypid.append_value(pg_typ.oid).unwrap();
        self.attstattarget.append_value(0).unwrap();
        self.attlen.append_value(pg_typ.typlen).unwrap();
        self.attnum.append_value(attnum).unwrap();
        self.attndims.append_value(is_array as i32).unwrap();
        self.attcacheoff.append_value(-1).unwrap();
        self.atttypmod.append_value(-1).unwrap();
        self.attbyval.append_value(pg_typ.typbyval).unwrap();
        self.attalign.append_value(pg_typ.typalign).unwrap();
        self.attstorage.append_value(pg_typ.typstorage).unwrap();
        self.attcompression.append_value("\0").unwrap();
        self.attnotnull.append_value(attnotnull).unwrap();
        self.atthasdef.append_value(false).unwrap();
        self.atthasmissing.append_value(false).unwrap();
        self.attidentity.append_value("").unwrap();
        self.attgenerated.append_value("").unwrap();
        self.attisdropped.append_value(false).unwrap();
        self.attislocal.append_value(true).unwrap();
        self.attinhcount.append_value(0).unwrap();
        // FIXME: attcollation should be equal to pg_catalog.pg_collation.oid if type is collatable, 0 otherwise
        self.attcollation.append_value(0).unwrap();
        self.attacl.append(false).unwrap();
        self.attoptions.append(false).unwrap();
        self.attfdwoptions.append(false).unwrap();
        self.attmissingval.append_null().unwrap();
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];
        columns.push(Arc::new(self.attrelid.finish()));
        columns.push(Arc::new(self.attname.finish()));
        columns.push(Arc::new(self.atttypid.finish()));
        columns.push(Arc::new(self.attstattarget.finish()));
        columns.push(Arc::new(self.attlen.finish()));
        columns.push(Arc::new(self.attnum.finish()));
        columns.push(Arc::new(self.attndims.finish()));
        columns.push(Arc::new(self.attcacheoff.finish()));
        columns.push(Arc::new(self.atttypmod.finish()));
        columns.push(Arc::new(self.attbyval.finish()));
        columns.push(Arc::new(self.attalign.finish()));
        columns.push(Arc::new(self.attstorage.finish()));
        columns.push(Arc::new(self.attcompression.finish()));
        columns.push(Arc::new(self.attnotnull.finish()));
        columns.push(Arc::new(self.atthasdef.finish()));
        columns.push(Arc::new(self.atthasmissing.finish()));
        columns.push(Arc::new(self.attidentity.finish()));
        columns.push(Arc::new(self.attgenerated.finish()));
        columns.push(Arc::new(self.attisdropped.finish()));
        columns.push(Arc::new(self.attislocal.finish()));
        columns.push(Arc::new(self.attinhcount.finish()));
        columns.push(Arc::new(self.attcollation.finish()));
        columns.push(Arc::new(self.attacl.finish()));
        columns.push(Arc::new(self.attoptions.finish()));
        columns.push(Arc::new(self.attfdwoptions.finish()));
        columns.push(Arc::new(self.attmissingval.finish()));

        columns
    }
}

pub struct PgCatalogAttributeProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl PgCatalogAttributeProvider {
    pub fn new(tables: &Vec<CubeMetaTable>) -> Self {
        let mut builder = PgCatalogAttributeBuilder::new();

        for table in tables {
            let mut column_id = 1..;
            for column in &table.columns {
                builder.add_attribute(
                    table.oid,
                    &column.name,
                    &column.column_type,
                    column_id.next().unwrap_or(0),
                    false,
                    !column.can_be_null,
                );
            }
        }

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for PgCatalogAttributeProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("attrelid", ExtDataType::Oid.into(), true),
            Field::new("attname", DataType::Utf8, false),
            Field::new("atttypid", ExtDataType::Oid.into(), false),
            Field::new("attstattarget", DataType::Int32, false),
            Field::new("attlen", DataType::Int16, false),
            Field::new("attnum", DataType::Int16, true),
            Field::new("attndims", DataType::Int32, false),
            Field::new("attcacheoff", DataType::Int32, false),
            Field::new("atttypmod", DataType::Int64, false),
            Field::new("attbyval", DataType::Boolean, false),
            Field::new("attalign", DataType::Utf8, false),
            Field::new("attstorage", DataType::Utf8, false),
            Field::new("attcompression", DataType::Utf8, false),
            Field::new("attnotnull", DataType::Boolean, false),
            Field::new("atthasdef", DataType::Boolean, false),
            Field::new("atthasmissing", DataType::Boolean, false),
            Field::new("attidentity", DataType::Utf8, false),
            Field::new("attgenerated", DataType::Utf8, false),
            Field::new("attisdropped", DataType::Boolean, false),
            Field::new("attislocal", DataType::Boolean, false),
            Field::new("attinhcount", DataType::Int32, false),
            Field::new("attcollation", ExtDataType::Oid.into(), false),
            Field::new(
                "attacl",
                DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new(
                "attoptions",
                DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new(
                "attfdwoptions",
                DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new("attmissingval", DataType::Utf8, true),
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
