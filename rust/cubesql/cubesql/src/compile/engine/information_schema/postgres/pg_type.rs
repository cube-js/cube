use std::{any::Any, sync::Arc};

use async_trait::async_trait;

use crate::transport::CubeMetaTable;
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

use super::utils::{ExtDataType, OidBuilder};

struct PgCatalogTypeBuilder {
    oid: OidBuilder,
    typname: StringBuilder,
    typnamespace: OidBuilder,
    typowner: OidBuilder,
    typlen: Int16Builder,
    typbyval: BooleanBuilder,
    typtype: StringBuilder,
    typcategory: StringBuilder,
    typisprefered: BooleanBuilder,
    typisdefined: BooleanBuilder,
    typdelim: StringBuilder,
    typrelid: OidBuilder,
    // TODO: type regproc?
    typsubscript: StringBuilder,
    typelem: OidBuilder,
    typarray: OidBuilder,
    // TODO: type regproc?
    typinput: StringBuilder,
    // TODO: type regproc?
    typoutput: StringBuilder,
    // TODO: type regproc?
    // In real tables, it's an additional type, but in pg_proc it's an oid
    typreceive: Int32Builder,
    // TODO: type regproc?
    typsend: StringBuilder,
    // TODO: type regproc?
    typmodin: StringBuilder,
    // TODO: type regproc?
    typmodout: StringBuilder,
    // TODO: type regproc?
    typanalyze: StringBuilder,
    typalign: StringBuilder,
    typstorage: StringBuilder,
    typnotnull: BooleanBuilder,
    typbasetype: OidBuilder,
    // TODO: See pg_attribute.atttypmod
    typtypmod: Int64Builder,
    typndims: Int32Builder,
    typcollation: OidBuilder,
    // TODO: type pg_node_tree?
    typdefaultbin: StringBuilder,
    typdefault: StringBuilder,
    // TODO: type aclitem?
    typacl: ListBuilder<StringBuilder>,
}

impl PgCatalogTypeBuilder {
    fn new() -> Self {
        let capacity = 10;

        Self {
            oid: OidBuilder::new(capacity),
            typname: StringBuilder::new(capacity),
            typnamespace: OidBuilder::new(capacity),
            typowner: OidBuilder::new(capacity),
            typlen: Int16Builder::new(capacity),
            typbyval: BooleanBuilder::new(capacity),
            typtype: StringBuilder::new(capacity),
            typcategory: StringBuilder::new(capacity),
            typisprefered: BooleanBuilder::new(capacity),
            typisdefined: BooleanBuilder::new(capacity),
            typdelim: StringBuilder::new(capacity),
            typrelid: OidBuilder::new(capacity),
            typsubscript: StringBuilder::new(capacity),
            typelem: OidBuilder::new(capacity),
            typarray: OidBuilder::new(capacity),
            // In real tables, it's an additional type, but in pg_proc it's an oid
            typreceive: Int32Builder::new(capacity),
            typinput: StringBuilder::new(capacity),
            // TODO: Check
            typoutput: StringBuilder::new(capacity),
            typsend: StringBuilder::new(capacity),
            typmodin: StringBuilder::new(capacity),
            typmodout: StringBuilder::new(capacity),
            typanalyze: StringBuilder::new(capacity),
            typalign: StringBuilder::new(capacity),
            typstorage: StringBuilder::new(capacity),
            typnotnull: BooleanBuilder::new(capacity),
            typbasetype: OidBuilder::new(capacity),
            typtypmod: Int64Builder::new(capacity),
            typndims: Int32Builder::new(capacity),
            typcollation: OidBuilder::new(capacity),
            typdefaultbin: StringBuilder::new(capacity),
            typdefault: StringBuilder::new(capacity),
            typacl: ListBuilder::new(StringBuilder::new(capacity)),
        }
    }

    fn add_type(&mut self, typ: &PgType) {
        self.oid.append_value(typ.oid).unwrap();
        self.typname.append_value(typ.typname).unwrap();
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
        self.typreceive
            .append_value(typ.get_typreceive_oid())
            .unwrap();
        self.typinput.append_value(typ.typinput).unwrap();
        // TODO: Check
        self.typoutput.append_null().unwrap();
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
        self.typacl.append(false).unwrap();
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
                typalign: "d",
                typstorage: "x",
                typbasetype: 0,
                typreceive: "record_recv",
                typinput: "record_in",
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
                typalign: "d",
                typstorage: "x",
                typbasetype: 0,
                typreceive: "array_recv",
                typinput: "array_in",
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
            Field::new("oid", ExtDataType::Oid.into(), false),
            Field::new("typname", DataType::Utf8, false),
            Field::new("typnamespace", ExtDataType::Oid.into(), false),
            Field::new("typowner", ExtDataType::Oid.into(), false),
            Field::new("typlen", DataType::Int16, false),
            Field::new("typbyval", DataType::Boolean, false),
            Field::new("typtype", DataType::Utf8, false),
            Field::new("typcategory", DataType::Utf8, false),
            Field::new("typisprefered", DataType::Boolean, false),
            Field::new("typisdefined", DataType::Boolean, false),
            Field::new("typdelim", DataType::Utf8, true),
            Field::new("typrelid", ExtDataType::Oid.into(), true),
            Field::new("typsubscript", DataType::Utf8, true),
            Field::new("typelem", ExtDataType::Oid.into(), true),
            Field::new("typarray", ExtDataType::Oid.into(), true),
            Field::new("typinput", DataType::Utf8, false),
            // TODO: Check
            Field::new("typoutput", DataType::Utf8, true),
            // In real tables, it's an additional type, but in pg_proc it's an oid
            Field::new("typreceive", DataType::Int32, true),
            Field::new("typsend", DataType::Utf8, true),
            Field::new("typmodin", DataType::Utf8, true),
            Field::new("typmodout", DataType::Utf8, true),
            Field::new("typanalyze", DataType::Utf8, true),
            Field::new("typalign", DataType::Utf8, true),
            Field::new("typstorage", DataType::Utf8, true),
            Field::new("typnotnull", DataType::Boolean, true),
            Field::new("typbasetype", ExtDataType::Oid.into(), true),
            Field::new("typtypmod", DataType::Int64, true),
            Field::new("typndims", DataType::Int32, true),
            Field::new("typcollation", ExtDataType::Oid.into(), true),
            Field::new("typdefaultbin", DataType::Utf8, true),
            Field::new("typdefault", DataType::Utf8, true),
            Field::new(
                "typacl",
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
