use std::{any::Any, sync::Arc};

use async_trait::async_trait;

use datafusion::{
    arrow::{
        array::{
            Array, ArrayRef, BooleanBuilder, Int16Builder, Int64Builder, StringBuilder,
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

use crate::{
    compile::engine::information_schema::postgres::PG_NAMESPACE_PUBLIC_OID,
    transport::CubeMetaTable,
};

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
    typinput: StringBuilder,
    // TODO: Check
    typoutput: StringBuilder,
    // In real tables, it's an additional type, but in pg_proc it's an oid
    typreceive: UInt32Builder,
    typsend: StringBuilder,
    typmodin: StringBuilder,
    typmodout: StringBuilder,
    typanalyze: StringBuilder,
    typalign: StringBuilder,
    typstorage: StringBuilder,
    typnotnull: BooleanBuilder,
    typbasetype: UInt32Builder,
    // TODO: See pg_attribute.atttypmod
    typtypmod: Int64Builder,
    typndims: StringBuilder,
    typcollation: StringBuilder,
    typdefaultbin: StringBuilder,
    typdefault: StringBuilder,
    typacl: StringBuilder,
    xmin: UInt32Builder,
}

impl PgCatalogTypeBuilder {
    fn new() -> Self {
        let capacity = 10;

        Self {
            oid: UInt32Builder::with_capacity(capacity),
            typname: StringBuilder::with_capacity(capacity, capacity),
            typnamespace: UInt32Builder::with_capacity(capacity),
            typowner: UInt32Builder::with_capacity(capacity),
            typlen: Int16Builder::with_capacity(capacity),
            typbyval: BooleanBuilder::with_capacity(capacity),
            typtype: StringBuilder::with_capacity(capacity, capacity),
            typcategory: StringBuilder::with_capacity(capacity, capacity),
            typisprefered: BooleanBuilder::with_capacity(capacity),
            typisdefined: BooleanBuilder::with_capacity(capacity),
            typdelim: StringBuilder::with_capacity(capacity, capacity),
            typrelid: UInt32Builder::with_capacity(capacity),
            typsubscript: StringBuilder::with_capacity(capacity, capacity),
            typelem: UInt32Builder::with_capacity(capacity),
            typarray: UInt32Builder::with_capacity(capacity),
            // In real tables, it's an additional type, but in pg_proc it's an oid
            typreceive: UInt32Builder::with_capacity(capacity),
            typinput: StringBuilder::with_capacity(capacity, capacity),
            // TODO: Check
            typoutput: StringBuilder::with_capacity(capacity, capacity),
            typsend: StringBuilder::with_capacity(capacity, capacity),
            typmodin: StringBuilder::with_capacity(capacity, capacity),
            typmodout: StringBuilder::with_capacity(capacity, capacity),
            typanalyze: StringBuilder::with_capacity(capacity, capacity),
            typalign: StringBuilder::with_capacity(capacity, capacity),
            typstorage: StringBuilder::with_capacity(capacity, capacity),
            typnotnull: BooleanBuilder::with_capacity(capacity),
            typbasetype: UInt32Builder::with_capacity(capacity),
            typtypmod: Int64Builder::with_capacity(capacity),
            typndims: StringBuilder::with_capacity(capacity, capacity),
            typcollation: StringBuilder::with_capacity(capacity, capacity),
            typdefaultbin: StringBuilder::with_capacity(capacity, capacity),
            typdefault: StringBuilder::with_capacity(capacity, capacity),
            typacl: StringBuilder::with_capacity(capacity, capacity),
            xmin: UInt32Builder::with_capacity(capacity),
        }
    }

    fn add_type(&mut self, typ: &PgType) {
        self.oid.append_value(typ.oid);
        self.typname.append_value(&typ.typname);
        self.typnamespace.append_value(typ.typnamespace);
        self.typlen.append_value(typ.typlen);
        self.typowner.append_value(typ.typowner);
        self.typbyval.append_value(typ.typbyval);
        self.typtype.append_value(typ.typtype);
        self.typcategory.append_value(typ.typcategory);
        self.typisprefered.append_value(typ.typisprefered);
        self.typisdefined.append_value(typ.typisdefined);
        self.typdelim.append_value(",");
        self.typrelid.append_value(typ.typrelid);
        self.typsubscript.append_value(typ.typsubscript);
        self.typelem.append_value(typ.typelem);
        self.typarray.append_value(typ.typarray);
        self.typreceive.append_value(typ.typreceive_oid);
        self.typinput.append_value(typ.get_typinput());
        // TODO: Check
        self.typoutput.append_null();
        self.typsend.append_null();
        self.typmodin.append_null();
        self.typmodout.append_null();
        self.typanalyze.append_null();
        self.typalign.append_value(typ.typalign);
        self.typstorage.append_value(typ.typstorage);
        self.typnotnull.append_value(false);
        self.typbasetype.append_value(typ.typbasetype);
        self.typtypmod.append_value(-1);
        self.typndims.append_null();
        self.typcollation.append_null();
        self.typdefaultbin.append_null();
        self.typdefault.append_null();
        self.typacl.append_null();
        self.xmin.append_value(1);
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(self.oid.finish()),
            Arc::new(self.typname.finish()),
            Arc::new(self.typnamespace.finish()),
            Arc::new(self.typowner.finish()),
            Arc::new(self.typlen.finish()),
            Arc::new(self.typbyval.finish()),
            Arc::new(self.typtype.finish()),
            Arc::new(self.typcategory.finish()),
            Arc::new(self.typisprefered.finish()),
            Arc::new(self.typisdefined.finish()),
            Arc::new(self.typdelim.finish()),
            Arc::new(self.typrelid.finish()),
            Arc::new(self.typsubscript.finish()),
            Arc::new(self.typelem.finish()),
            Arc::new(self.typarray.finish()),
            Arc::new(self.typinput.finish()),
            Arc::new(self.typoutput.finish()),
            Arc::new(self.typreceive.finish()),
            Arc::new(self.typsend.finish()),
            Arc::new(self.typmodin.finish()),
            Arc::new(self.typmodout.finish()),
            Arc::new(self.typanalyze.finish()),
            Arc::new(self.typalign.finish()),
            Arc::new(self.typstorage.finish()),
            Arc::new(self.typnotnull.finish()),
            Arc::new(self.typbasetype.finish()),
            Arc::new(self.typtypmod.finish()),
            Arc::new(self.typndims.finish()),
            Arc::new(self.typcollation.finish()),
            Arc::new(self.typdefaultbin.finish()),
            Arc::new(self.typdefault.finish()),
            Arc::new(self.typacl.finish()),
            Arc::new(self.xmin.finish()),
        ];

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
                regtype: table.name.as_str(),
                typnamespace: PG_NAMESPACE_PUBLIC_OID,
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
                // TODO Verify
                typreceive: "",
                // TODO: Get from pg_proc
                typreceive_oid: 0,
            });

            builder.add_type(&PgType {
                oid: table.array_handler_oid,
                typname: format!("_{}", table.name).as_str(),
                regtype: format!("{}[]", table.name).as_str(),
                typnamespace: PG_NAMESPACE_PUBLIC_OID,
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
                // TODO Verify
                typreceive: "",
                // TODO: Get from pg_proc
                typreceive_oid: 0,
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
            Field::new("typinput", DataType::Utf8, false),
            // TODO: Check
            Field::new("typoutput", DataType::Utf8, true),
            // In real tables, it's an additional type, but in pg_proc it's an oid
            Field::new("typreceive", DataType::UInt32, true),
            Field::new("typsend", DataType::Utf8, true),
            Field::new("typmodin", DataType::Utf8, true),
            Field::new("typmodout", DataType::Utf8, true),
            Field::new("typanalyze", DataType::Utf8, true),
            Field::new("typalign", DataType::Utf8, true),
            Field::new("typstorage", DataType::Utf8, true),
            Field::new("typnotnull", DataType::Boolean, true),
            Field::new("typbasetype", DataType::UInt32, true),
            Field::new("typtypmod", DataType::Int64, true),
            Field::new("typndims", DataType::Utf8, true),
            Field::new("typcollation", DataType::Utf8, true),
            Field::new("typdefaultbin", DataType::Utf8, true),
            Field::new("typdefault", DataType::Utf8, true),
            Field::new("typacl", DataType::Utf8, true),
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
