use std::{any::Any, sync::Arc};

use async_trait::async_trait;

use crate::transport::CubeMetaTable;
use datafusion::{
    arrow::{
        array::{
            Array, ArrayRef, BooleanBuilder, Int16Builder, Int32Builder, StringBuilder,
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
use pg_srv::PgType;

struct PgCatalogTypeBuilder {
    oid: UInt32Builder,
    typname: StringBuilder,
    typnamespace: UInt32Builder,
    typowner: UInt32Builder,
    typlen: Int16Builder,
    typbyval: BooleanBuilder,
    typtype: StringBuilder,
    typcategory: StringBuilder,
    typisprefered: BooleanBuilder,
    typisdefined: BooleanBuilder,
    typdelim: StringBuilder,
    typrelid: UInt32Builder,
    typsubscript: StringBuilder,
    typelem: UInt32Builder,
    typarray: UInt32Builder,
    // TODO: Check
    typinput: StringBuilder,
    typoutput: StringBuilder,
    typreceive: StringBuilder,
    typsend: StringBuilder,
    typmodin: StringBuilder,
    typmodout: StringBuilder,
    typanalyze: StringBuilder,
    typalign: StringBuilder,
    typstorage: StringBuilder,
    typnotnull: BooleanBuilder,
    typbasetype: UInt32Builder,
    typtypmod: Int32Builder,
    typndims: StringBuilder,
    typcollation: StringBuilder,
    typdefaultbin: StringBuilder,
    typdefault: StringBuilder,
    typacl: StringBuilder,
}

impl PgCatalogTypeBuilder {
    fn new() -> Self {
        let capacity = 10;

        Self {
            oid: UInt32Builder::new(capacity),
            typname: StringBuilder::new(capacity),
            typnamespace: UInt32Builder::new(capacity),
            typowner: UInt32Builder::new(capacity),
            typlen: Int16Builder::new(capacity),
            typbyval: BooleanBuilder::new(capacity),
            typtype: StringBuilder::new(capacity),
            typcategory: StringBuilder::new(capacity),
            typisprefered: BooleanBuilder::new(capacity),
            typisdefined: BooleanBuilder::new(capacity),
            typdelim: StringBuilder::new(capacity),
            typrelid: UInt32Builder::new(capacity),
            typsubscript: StringBuilder::new(capacity),
            typelem: UInt32Builder::new(capacity),
            typarray: UInt32Builder::new(capacity),
            // TODO: Check
            typinput: StringBuilder::new(capacity),
            typoutput: StringBuilder::new(capacity),
            typreceive: StringBuilder::new(capacity),
            typsend: StringBuilder::new(capacity),
            typmodin: StringBuilder::new(capacity),
            typmodout: StringBuilder::new(capacity),
            typanalyze: StringBuilder::new(capacity),
            typalign: StringBuilder::new(capacity),
            typstorage: StringBuilder::new(capacity),
            typnotnull: BooleanBuilder::new(capacity),
            typbasetype: UInt32Builder::new(capacity),
            typtypmod: Int32Builder::new(capacity),
            typndims: StringBuilder::new(capacity),
            typcollation: StringBuilder::new(capacity),
            typdefaultbin: StringBuilder::new(capacity),
            typdefault: StringBuilder::new(capacity),
            typacl: StringBuilder::new(capacity),
        }
    }

    fn add_type(&mut self, typ: &PgType) {
        self.oid.append_value(typ.oid).unwrap();
        self.typname.append_value(&typ.typname).unwrap();
        self.typnamespace.append_value(typ.typnamespace).unwrap();
        self.typlen.append_value(typ.typlen).unwrap();
        self.typowner.append_value(typ.typowner).unwrap();
        self.typbyval.append_value(typ.typbyval).unwrap();
        self.typtype.append_value(typ.typtype).unwrap();
        self.typcategory.append_value(typ.typcategory).unwrap();
        self.typisprefered.append_value(typ.typisprefered).unwrap();
        self.typisdefined.append_value(typ.typisdefined).unwrap();
        self.typdelim.append_value(",").unwrap();
        self.typrelid.append_value(typ.typrelid).unwrap();
        self.typsubscript.append_value(typ.typsubscript).unwrap();
        self.typelem.append_value(typ.typelem).unwrap();
        self.typarray.append_value(typ.typarray).unwrap();
        // TODO: Check
        self.typinput.append_null().unwrap();
        self.typoutput.append_null().unwrap();
        self.typreceive.append_null().unwrap();
        self.typsend.append_null().unwrap();
        self.typmodin.append_null().unwrap();
        self.typmodout.append_null().unwrap();
        self.typanalyze.append_null().unwrap();
        self.typalign.append_value(typ.typalign).unwrap();
        self.typstorage.append_value(typ.typstorage).unwrap();
        self.typnotnull.append_value(false).unwrap();
        self.typbasetype.append_value(typ.typbasetype).unwrap();
        self.typtypmod.append_value(-1).unwrap();
        self.typndims.append_null().unwrap();
        self.typcollation.append_null().unwrap();
        self.typdefaultbin.append_null().unwrap();
        self.typdefault.append_null().unwrap();
        self.typacl.append_null().unwrap();
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];
        columns.push(Arc::new(self.oid.finish()));
        columns.push(Arc::new(self.typname.finish()));
        columns.push(Arc::new(self.typnamespace.finish()));
        columns.push(Arc::new(self.typowner.finish()));
        columns.push(Arc::new(self.typlen.finish()));
        columns.push(Arc::new(self.typbyval.finish()));
        columns.push(Arc::new(self.typtype.finish()));
        columns.push(Arc::new(self.typcategory.finish()));
        columns.push(Arc::new(self.typisprefered.finish()));
        columns.push(Arc::new(self.typisdefined.finish()));
        columns.push(Arc::new(self.typdelim.finish()));
        columns.push(Arc::new(self.typrelid.finish()));
        columns.push(Arc::new(self.typsubscript.finish()));
        columns.push(Arc::new(self.typelem.finish()));
        columns.push(Arc::new(self.typarray.finish()));
        columns.push(Arc::new(self.typinput.finish()));
        columns.push(Arc::new(self.typoutput.finish()));
        columns.push(Arc::new(self.typreceive.finish()));
        columns.push(Arc::new(self.typsend.finish()));
        columns.push(Arc::new(self.typmodin.finish()));
        columns.push(Arc::new(self.typmodout.finish()));
        columns.push(Arc::new(self.typanalyze.finish()));
        columns.push(Arc::new(self.typalign.finish()));
        columns.push(Arc::new(self.typstorage.finish()));
        columns.push(Arc::new(self.typnotnull.finish()));
        columns.push(Arc::new(self.typbasetype.finish()));
        columns.push(Arc::new(self.typtypmod.finish()));
        columns.push(Arc::new(self.typndims.finish()));
        columns.push(Arc::new(self.typcollation.finish()));
        columns.push(Arc::new(self.typdefaultbin.finish()));
        columns.push(Arc::new(self.typdefault.finish()));
        columns.push(Arc::new(self.typacl.finish()));

        columns
    }
}

pub struct PgCatalogTypeProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl PgCatalogTypeProvider {
    pub fn new(tables: &Vec<CubeMetaTable>) -> Self {
        let mut builder = PgCatalogTypeBuilder::new();

        for typ in PgType::get_all() {
            builder.add_type(typ);
        }

        for table in tables {
            builder.add_type(&PgType {
                oid: table.record_oid,
                typname: table.name.as_str(),
                typnamespace: 2200,
                typowner: 10,
                typlen: -1,
                typbyval: false,
                typtype: "c",
                typcategory: "C",
                typisprefered: false,
                typisdefined: true,
                typrelid: table.oid,
                typsubscript: "-",
                typelem: 0,
                typarray: table.array_handler_oid,
                // TODO Verify
                typalign: "i",
                typstorage: "x",
                typbasetype: 0,
            });

            builder.add_type(&PgType {
                oid: table.array_handler_oid,
                typname: format!("_{}", table.name).as_str(),
                typnamespace: 2200,
                typowner: 10,
                typlen: -1,
                typbyval: false,
                typtype: "b",
                typcategory: "A",
                typisprefered: false,
                typisdefined: true,
                typrelid: 0,
                typsubscript: "array_subscript_handler",
                typelem: table.record_oid,
                typarray: 0,
                // TODO Verify
                typalign: "d",
                typstorage: "x",
                typbasetype: 0,
            });
        }

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for PgCatalogTypeProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::UInt32, false),
            Field::new("typname", DataType::Utf8, false),
            Field::new("typnamespace", DataType::UInt32, false),
            Field::new("typowner", DataType::UInt32, false),
            Field::new("typlen", DataType::Int16, false),
            Field::new("typbyval", DataType::Boolean, false),
            Field::new("typtype", DataType::Utf8, false),
            Field::new("typcategory", DataType::Utf8, false),
            Field::new("typisprefered", DataType::Boolean, false),
            Field::new("typisdefined", DataType::Boolean, false),
            Field::new("typdelim", DataType::Utf8, true),
            Field::new("typrelid", DataType::UInt32, true),
            Field::new("typsubscript", DataType::Utf8, true),
            Field::new("typelem", DataType::UInt32, true),
            Field::new("typarray", DataType::UInt32, true),
            // TODO: Check
            Field::new("typinput", DataType::Utf8, true),
            Field::new("typoutput", DataType::Utf8, true),
            Field::new("typreceive", DataType::Utf8, true),
            Field::new("typsend", DataType::Utf8, true),
            Field::new("typmodin", DataType::Utf8, true),
            Field::new("typmodout", DataType::Utf8, true),
            Field::new("typanalyze", DataType::Utf8, true),
            Field::new("typalign", DataType::Utf8, true),
            Field::new("typstorage", DataType::Utf8, true),
            Field::new("typnotnull", DataType::Boolean, true),
            Field::new("typbasetype", DataType::UInt32, true),
            Field::new("typtypmod", DataType::Int32, true),
            Field::new("typndims", DataType::Utf8, true),
            Field::new("typcollation", DataType::Utf8, true),
            Field::new("typdefaultbin", DataType::Utf8, true),
            Field::new("typdefault", DataType::Utf8, true),
            Field::new("typacl", DataType::Utf8, true),
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
