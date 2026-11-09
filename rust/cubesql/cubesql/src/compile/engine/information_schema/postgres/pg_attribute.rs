use std::{any::Any, sync::Arc};

use crate::{sql::ColumnType, transport::CubeMetaTable};
use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{
            Array, ArrayRef, BooleanBuilder, Int16Builder, Int32Builder, Int64Builder,
            StringBuilder, UInt32Builder,
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

struct PgCatalogAttributeBuilder {
    attrelid: UInt32Builder,
    attname: StringBuilder,
    atttypid: UInt32Builder,
    attstattarget: Int32Builder,
    attlen: Int16Builder,
    attnum: Int16Builder,
    attndims: UInt32Builder,
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
    attinhcount: UInt32Builder,
    attcollation: UInt32Builder,
    attacl: StringBuilder,
    attoptions: StringBuilder,
    attfdwoptions: StringBuilder,
    attmissingval: StringBuilder,
    xmin: UInt32Builder,
}

impl PgCatalogAttributeBuilder {
    fn new() -> Self {
        let capacity = 10;

        Self {
            attrelid: UInt32Builder::with_capacity(capacity),
            attname: StringBuilder::with_capacity(capacity, capacity),
            atttypid: UInt32Builder::with_capacity(capacity),
            attstattarget: Int32Builder::with_capacity(capacity),
            attlen: Int16Builder::with_capacity(capacity),
            attnum: Int16Builder::with_capacity(capacity),
            attndims: UInt32Builder::with_capacity(capacity),
            attcacheoff: Int32Builder::with_capacity(capacity),
            atttypmod: Int64Builder::with_capacity(capacity),
            attbyval: BooleanBuilder::with_capacity(capacity),
            attalign: StringBuilder::with_capacity(capacity, capacity),
            attstorage: StringBuilder::with_capacity(capacity, capacity),
            attcompression: StringBuilder::with_capacity(capacity, capacity),
            attnotnull: BooleanBuilder::with_capacity(capacity),
            atthasdef: BooleanBuilder::with_capacity(capacity),
            atthasmissing: BooleanBuilder::with_capacity(capacity),
            attidentity: StringBuilder::with_capacity(capacity, capacity),
            attgenerated: StringBuilder::with_capacity(capacity, capacity),
            attisdropped: BooleanBuilder::with_capacity(capacity),
            attislocal: BooleanBuilder::with_capacity(capacity),
            attinhcount: UInt32Builder::with_capacity(capacity),
            attcollation: UInt32Builder::with_capacity(capacity),
            attacl: StringBuilder::with_capacity(capacity, capacity),
            attoptions: StringBuilder::with_capacity(capacity, capacity),
            attfdwoptions: StringBuilder::with_capacity(capacity, capacity),
            attmissingval: StringBuilder::with_capacity(capacity, capacity),
            xmin: UInt32Builder::with_capacity(capacity),
        }
    }

    fn add_attribute(
        &mut self,
        attrelid: u32,
        attname: impl AsRef<str>,
        column_type: &ColumnType,
        attnum: i16,
        is_array: bool,
        attnotnull: bool,
    ) {
        let pg_typ = PgType::get_by_tid(column_type.to_pg_tid());

        // TODO: get data from pg_type description
        self.attrelid.append_value(attrelid);
        self.attname.append_value(attname.as_ref());
        self.atttypid.append_value(pg_typ.oid);
        self.attstattarget.append_value(0);
        self.attlen.append_value(pg_typ.typlen);
        self.attnum.append_value(attnum);
        self.attndims.append_value(is_array as u32);
        self.attcacheoff.append_value(-1);
        self.atttypmod.append_value(-1);
        self.attbyval.append_value(pg_typ.typbyval);
        self.attalign.append_value(pg_typ.typalign);
        self.attstorage.append_value(pg_typ.typstorage);
        self.attcompression.append_value("\0");
        self.attnotnull.append_value(attnotnull);
        self.atthasdef.append_value(false);
        self.atthasmissing.append_value(false);
        self.attidentity.append_value("");
        self.attgenerated.append_value("");
        self.attisdropped.append_value(false);
        self.attislocal.append_value(true);
        self.attinhcount.append_value(0);
        // FIXME: attcollation should be equal to pg_catalog.pg_collation.oid if type is collatable, 0 otherwise
        self.attcollation.append_value(0);
        self.attacl.append_null();
        self.attoptions.append_null();
        self.attfdwoptions.append_null();
        self.attmissingval.append_null();
        self.xmin.append_value(1);
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(self.attrelid.finish()),
            Arc::new(self.attname.finish()),
            Arc::new(self.atttypid.finish()),
            Arc::new(self.attstattarget.finish()),
            Arc::new(self.attlen.finish()),
            Arc::new(self.attnum.finish()),
            Arc::new(self.attndims.finish()),
            Arc::new(self.attcacheoff.finish()),
            Arc::new(self.atttypmod.finish()),
            Arc::new(self.attbyval.finish()),
            Arc::new(self.attalign.finish()),
            Arc::new(self.attstorage.finish()),
            Arc::new(self.attcompression.finish()),
            Arc::new(self.attnotnull.finish()),
            Arc::new(self.atthasdef.finish()),
            Arc::new(self.atthasmissing.finish()),
            Arc::new(self.attidentity.finish()),
            Arc::new(self.attgenerated.finish()),
            Arc::new(self.attisdropped.finish()),
            Arc::new(self.attislocal.finish()),
            Arc::new(self.attinhcount.finish()),
            Arc::new(self.attcollation.finish()),
            Arc::new(self.attacl.finish()),
            Arc::new(self.attoptions.finish()),
            Arc::new(self.attfdwoptions.finish()),
            Arc::new(self.attmissingval.finish()),
            Arc::new(self.xmin.finish()),
        ];

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
            for (column_id, column) in (1..).zip(&table.columns) {
                builder.add_attribute(
                    table.oid,
                    &column.name,
                    &column.column_type,
                    column_id,
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
            Field::new("attrelid", DataType::UInt32, false),
            Field::new("attname", DataType::Utf8, false),
            Field::new("atttypid", DataType::UInt32, false),
            Field::new("attstattarget", DataType::Int32, false),
            Field::new("attlen", DataType::Int16, false),
            Field::new("attnum", DataType::Int16, true),
            Field::new("attndims", DataType::UInt32, false),
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
            Field::new("attinhcount", DataType::UInt32, false),
            Field::new("attcollation", DataType::UInt32, false),
            Field::new("attacl", DataType::Utf8, true),
            Field::new("attoptions", DataType::Utf8, true),
            Field::new("attfdwoptions", DataType::Utf8, true),
            Field::new("attmissingval", DataType::Utf8, true),
            Field::new("xmin", DataType::UInt32, false),
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
