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

struct PgProc {
    oid: u32,
    proname: String,
    prokind: String,
    proleakproof: bool,
    proisstrict: bool,
    proretset: bool,
    provolatile: String,
    proparallel: String,
    pronargs: i32,
    prorettype: u32,
    proargtypes: String,
    proallargtypes: String,
    proargmodes: String,
    proargnames: String,
    prosrc: String,
}

struct PgCatalogProcBuilder {
    oid: UInt32Builder,
    proname: StringBuilder,
    pronamespace: UInt32Builder,
    proowner: UInt32Builder,
    prolang: UInt32Builder,
    procost: Int32Builder,
    prorows: Int32Builder,
    provariadic: UInt32Builder,
    prosupport: StringBuilder,
    prokind: StringBuilder,
    prosecdef: BooleanBuilder,
    proleakproof: BooleanBuilder,
    proisstrict: BooleanBuilder,
    proretset: BooleanBuilder,
    provolatile: StringBuilder,
    proparallel: StringBuilder,
    pronargs: Int32Builder,
    pronargdefaults: Int32Builder,
    prorettype: UInt32Builder,
    proargtypes: StringBuilder,
    proallargtypes: StringBuilder,
    proargmodes: StringBuilder,
    proargnames: StringBuilder,
    proargdefaults: StringBuilder,
    protrftypes: StringBuilder,
    prosrc: StringBuilder,
    probin: StringBuilder,
    prosqlbody: StringBuilder,
    proconfig: StringBuilder,
    proacl: StringBuilder,
}

impl PgCatalogProcBuilder {
    fn new() -> Self {
        let capacity = 10;

        Self {
            oid: UInt32Builder::new(capacity),
            proname: StringBuilder::new(capacity),
            pronamespace: UInt32Builder::new(capacity),
            proowner: UInt32Builder::new(capacity),
            prolang: UInt32Builder::new(capacity),
            procost: Int32Builder::new(capacity),
            prorows: Int32Builder::new(capacity),
            provariadic: UInt32Builder::new(capacity),
            prosupport: StringBuilder::new(capacity),
            prokind: StringBuilder::new(capacity),
            prosecdef: BooleanBuilder::new(capacity),
            proleakproof: BooleanBuilder::new(capacity),
            proisstrict: BooleanBuilder::new(capacity),
            proretset: BooleanBuilder::new(capacity),
            provolatile: StringBuilder::new(capacity),
            proparallel: StringBuilder::new(capacity),
            pronargs: Int32Builder::new(capacity),
            pronargdefaults: Int32Builder::new(capacity),
            prorettype: UInt32Builder::new(capacity),
            proargtypes: StringBuilder::new(capacity),
            proallargtypes: StringBuilder::new(capacity),
            proargmodes: StringBuilder::new(capacity),
            proargnames: StringBuilder::new(capacity),
            proargdefaults: StringBuilder::new(capacity),
            protrftypes: StringBuilder::new(capacity),
            prosrc: StringBuilder::new(capacity),
            probin: StringBuilder::new(capacity),
            prosqlbody: StringBuilder::new(capacity),
            proconfig: StringBuilder::new(capacity),
            proacl: StringBuilder::new(capacity),
        }
    }

    fn add_proc(&mut self, proc: &PgProc) {
        self.oid.append_value(proc.oid).unwrap();
        self.proname.append_value(proc.proname.clone()).unwrap();
        self.pronamespace.append_value(11).unwrap();
        self.proowner.append_value(10).unwrap();
        self.prolang.append_value(12).unwrap();
        self.procost.append_value(1).unwrap();
        self.prorows.append_value(0).unwrap();
        self.provariadic.append_value(0).unwrap();
        self.prosupport.append_value("-".to_string()).unwrap();
        self.prokind.append_value(proc.prokind.clone()).unwrap();
        self.prosecdef.append_value(false).unwrap();
        self.proleakproof.append_value(proc.proleakproof).unwrap();
        self.proisstrict.append_value(proc.proisstrict).unwrap();
        self.proretset.append_value(proc.proretset).unwrap();
        self.provolatile
            .append_value(proc.provolatile.clone())
            .unwrap();
        self.proparallel
            .append_value(proc.proparallel.clone())
            .unwrap();
        self.pronargs.append_value(proc.pronargs).unwrap();
        self.pronargdefaults.append_value(0).unwrap();
        self.prorettype.append_value(proc.prorettype).unwrap();
        self.proargtypes
            .append_value(proc.proargtypes.clone())
            .unwrap();
        self.proallargtypes
            .append_value(proc.proallargtypes.clone())
            .unwrap();
        self.proargmodes
            .append_value(proc.proargmodes.clone())
            .unwrap();
        self.proargnames
            .append_value(proc.proargnames.clone())
            .unwrap();
        self.proargdefaults.append_null().unwrap();
        self.protrftypes.append_null().unwrap();
        self.prosrc.append_value(proc.prosrc.clone()).unwrap();
        self.probin.append_null().unwrap();
        self.prosqlbody.append_null().unwrap();
        self.proconfig.append_null().unwrap();
        self.proacl.append_null().unwrap();
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];

        columns.push(Arc::new(self.oid.finish()));
        columns.push(Arc::new(self.proname.finish()));
        columns.push(Arc::new(self.pronamespace.finish()));
        columns.push(Arc::new(self.proowner.finish()));
        columns.push(Arc::new(self.prolang.finish()));
        columns.push(Arc::new(self.procost.finish()));
        columns.push(Arc::new(self.prorows.finish()));
        columns.push(Arc::new(self.provariadic.finish()));
        columns.push(Arc::new(self.prosupport.finish()));
        columns.push(Arc::new(self.prokind.finish()));
        columns.push(Arc::new(self.prosecdef.finish()));
        columns.push(Arc::new(self.proleakproof.finish()));
        columns.push(Arc::new(self.proisstrict.finish()));
        columns.push(Arc::new(self.proretset.finish()));
        columns.push(Arc::new(self.provolatile.finish()));
        columns.push(Arc::new(self.proparallel.finish()));
        columns.push(Arc::new(self.pronargs.finish()));
        columns.push(Arc::new(self.pronargdefaults.finish()));
        columns.push(Arc::new(self.prorettype.finish()));
        columns.push(Arc::new(self.proargtypes.finish()));
        columns.push(Arc::new(self.proallargtypes.finish()));
        columns.push(Arc::new(self.proargmodes.finish()));
        columns.push(Arc::new(self.proargnames.finish()));
        columns.push(Arc::new(self.proargdefaults.finish()));
        columns.push(Arc::new(self.protrftypes.finish()));
        columns.push(Arc::new(self.prosrc.finish()));
        columns.push(Arc::new(self.probin.finish()));
        columns.push(Arc::new(self.prosqlbody.finish()));
        columns.push(Arc::new(self.proconfig.finish()));
        columns.push(Arc::new(self.proacl.finish()));

        columns
    }
}

pub struct PgCatalogProcProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl PgCatalogProcProvider {
    pub fn new() -> Self {
        let mut builder = PgCatalogProcBuilder::new();

        builder.add_proc(&PgProc {
            oid: 89,
            proname: "version".to_string(),
            prokind: "f".to_string(),
            proleakproof: false,
            proisstrict: true,
            proretset: false,
            provolatile: "s".to_string(),
            proparallel: "s".to_string(),
            pronargs: 0,
            prorettype: 25,
            proargtypes: "".to_string(),
            proallargtypes: "".to_string(),
            proargmodes: "".to_string(),
            proargnames: "".to_string(),
            prosrc: "pgsql_version".to_string(),
        });

        builder.add_proc(&PgProc {
            oid: 1191,
            proname: "generate_subscripts".to_string(),
            prokind: "f".to_string(),
            proleakproof: false,
            proisstrict: true,
            proretset: true,
            provolatile: "i".to_string(),
            proparallel: "s".to_string(),
            pronargs: 3,
            prorettype: 23,
            proargtypes: "".to_string(),
            proallargtypes: "".to_string(),
            proargmodes: "".to_string(),
            proargnames: "".to_string(),
            prosrc: "generate_subscripts".to_string(),
        });

        builder.add_proc(&PgProc {
            oid: 1191,
            proname: "generate_subscripts".to_string(),
            prokind: "f".to_string(),
            proleakproof: false,
            proisstrict: true,
            proretset: true,
            provolatile: "i".to_string(),
            proparallel: "s".to_string(),
            pronargs: 3,
            prorettype: 23,
            proargtypes: "".to_string(),
            proallargtypes: "".to_string(),
            proargmodes: "".to_string(),
            proargnames: "".to_string(),
            prosrc: "generate_subscripts".to_string(),
        });

        builder.add_proc(&PgProc {
            oid: 1192,
            proname: "generate_subscripts".to_string(),
            prokind: "f".to_string(),
            proleakproof: false,
            proisstrict: true,
            proretset: true,
            provolatile: "i".to_string(),
            proparallel: "s".to_string(),
            pronargs: 2,
            prorettype: 23,
            proargtypes: "".to_string(),
            proallargtypes: "".to_string(),
            proargmodes: "".to_string(),
            proargnames: "".to_string(),
            prosrc: "generate_subscripts_nodir".to_string(),
        });

        builder.add_proc(&PgProc {
            oid: 2331,
            proname: "unnest".to_string(),
            prokind: "f".to_string(),
            proleakproof: false,
            proisstrict: true,
            proretset: true,
            provolatile: "i".to_string(),
            proparallel: "s".to_string(),
            pronargs: 1,
            prorettype: 2283,
            proargtypes: "".to_string(),
            proallargtypes: "".to_string(),
            proargmodes: "".to_string(),
            proargnames: "".to_string(),
            prosrc: "array_unnest".to_string(),
        });

        builder.add_proc(&PgProc {
            oid: 1081,
            proname: "format_type".to_string(),
            prokind: "f".to_string(),
            proleakproof: false,
            proisstrict: false,
            proretset: false,
            provolatile: "s".to_string(),
            proparallel: "s".to_string(),
            pronargs: 2,
            prorettype: 25,
            proargtypes: "".to_string(),
            proallargtypes: "".to_string(),
            proargmodes: "".to_string(),
            proargnames: "".to_string(),
            prosrc: "format_type".to_string(),
        });

        builder.add_proc(&PgProc {
            oid: 1402,
            proname: "current_schema".to_string(),
            prokind: "f".to_string(),
            proleakproof: false,
            proisstrict: true,
            proretset: false,
            provolatile: "s".to_string(),
            proparallel: "u".to_string(),
            pronargs: 0,
            prorettype: 19,
            proargtypes: "".to_string(),
            proallargtypes: "".to_string(),
            proargmodes: "".to_string(),
            proargnames: "".to_string(),
            prosrc: "current_schema".to_string(),
        });

        builder.add_proc(&PgProc {
            oid: 1387,
            proname: "pg_get_constraintdef".to_string(),
            prokind: "f".to_string(),
            proleakproof: false,
            proisstrict: true,
            proretset: false,
            provolatile: "s".to_string(),
            proparallel: "s".to_string(),
            pronargs: 1,
            prorettype: 25,
            proargtypes: "".to_string(),
            proallargtypes: "".to_string(),
            proargmodes: "".to_string(),
            proargnames: "".to_string(),
            prosrc: "pg_get_constraintdef".to_string(),
        });

        builder.add_proc(&PgProc {
            oid: 1716,
            proname: "pg_get_expr".to_string(),
            prokind: "f".to_string(),
            proleakproof: false,
            proisstrict: true,
            proretset: false,
            provolatile: "s".to_string(),
            proparallel: "s".to_string(),
            pronargs: 2,
            prorettype: 25,
            proargtypes: "".to_string(),
            proallargtypes: "".to_string(),
            proargmodes: "".to_string(),
            proargnames: "".to_string(),
            prosrc: "pg_get_expr".to_string(),
        });

        builder.add_proc(&PgProc {
            oid: 2080,
            proname: "pg_type_is_visible".to_string(),
            prokind: "f".to_string(),
            proleakproof: false,
            proisstrict: true,
            proretset: false,
            provolatile: "s".to_string(),
            proparallel: "s".to_string(),
            pronargs: 1,
            prorettype: 16,
            proargtypes: "".to_string(),
            proallargtypes: "".to_string(),
            proargmodes: "".to_string(),
            proargnames: "".to_string(),
            prosrc: "pg_type_is_visible".to_string(),
        });

        for (oid, prorettype) in [
            (2115, 20),
            (2116, 23),
            (2117, 21),
            (2118, 26),
            (2119, 700),
            (2120, 701),
            (2122, 1082),
            (2123, 1083),
            (2124, 1266),
            (2125, 790),
            (2126, 1114),
            (2127, 1184),
            (2129, 25),
            (2130, 1700),
            (2150, 2277),
            (2244, 1042),
            (2797, 27),
            (3564, 869),
            (4189, 3220),
            (3526, 3500),
        ]
        .iter()
        {
            builder.add_proc(&PgProc {
                oid: *oid,
                proname: "max".to_string(),
                prokind: "a".to_string(),
                proleakproof: false,
                proisstrict: false,
                proretset: false,
                provolatile: "i".to_string(),
                proparallel: "s".to_string(),
                pronargs: 1,
                prorettype: *prorettype,
                proargtypes: "".to_string(),
                proallargtypes: "".to_string(),
                proargmodes: "".to_string(),
                proargnames: "".to_string(),
                prosrc: "aggregate_dummy".to_string(),
            });
        }

        builder.add_proc(&PgProc {
            oid: 2508,
            proname: "pg_get_constraintdef".to_string(),
            prokind: "f".to_string(),
            proleakproof: false,
            proisstrict: true,
            proretset: false,
            provolatile: "s".to_string(),
            proparallel: "s".to_string(),
            pronargs: 2,
            prorettype: 25,
            proargtypes: "".to_string(),
            proallargtypes: "".to_string(),
            proargmodes: "".to_string(),
            proargnames: "".to_string(),
            prosrc: "pg_get_constraintdef_ext".to_string(),
        });

        builder.add_proc(&PgProc {
            oid: 2509,
            proname: "pg_get_expr".to_string(),
            prokind: "f".to_string(),
            proleakproof: false,
            proisstrict: true,
            proretset: false,
            provolatile: "s".to_string(),
            proparallel: "s".to_string(),
            pronargs: 3,
            prorettype: 25,
            proargtypes: "".to_string(),
            proallargtypes: "".to_string(),
            proargmodes: "".to_string(),
            proargnames: "".to_string(),
            prosrc: "pg_get_expr_ext".to_string(),
        });

        builder.add_proc(&PgProc {
            oid: 3322,
            proname: "unnest".to_string(),
            prokind: "f".to_string(),
            proleakproof: false,
            proisstrict: true,
            proretset: true,
            provolatile: "i".to_string(),
            proparallel: "s".to_string(),
            pronargs: 1,
            prorettype: 2249,
            proargtypes: "".to_string(),
            proallargtypes: "".to_string(),
            proargmodes: "".to_string(),
            proargnames: "".to_string(),
            prosrc: "tsvector_unnest".to_string(),
        });

        builder.add_proc(&PgProc {
            oid: 1293,
            proname: "unnest".to_string(),
            prokind: "f".to_string(),
            proleakproof: false,
            proisstrict: true,
            proretset: true,
            provolatile: "i".to_string(),
            proparallel: "s".to_string(),
            pronargs: 1,
            prorettype: 3831,
            proargtypes: "".to_string(),
            proallargtypes: "".to_string(),
            proargmodes: "".to_string(),
            proargnames: "".to_string(),
            prosrc: "multirange_unnest".to_string(),
        });

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for PgCatalogProcProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::UInt32, false),
            Field::new("proname", DataType::Utf8, false),
            Field::new("pronamespace", DataType::UInt32, false),
            Field::new("proowner", DataType::UInt32, false),
            Field::new("prolang", DataType::UInt32, false),
            Field::new("procost", DataType::Int32, false),
            Field::new("prorows", DataType::Int32, false),
            Field::new("provariadic", DataType::UInt32, false),
            Field::new("prosupport", DataType::Utf8, false),
            Field::new("prokind", DataType::Utf8, false),
            Field::new("prosecdef", DataType::Boolean, false),
            Field::new("proleakproof", DataType::Boolean, false),
            Field::new("proisstrict", DataType::Boolean, false),
            Field::new("proretset", DataType::Boolean, false),
            Field::new("provolatile", DataType::Utf8, false),
            Field::new("proparallel", DataType::Utf8, false),
            Field::new("pronargs", DataType::Int32, false),
            Field::new("pronargdefaults", DataType::Int32, false),
            Field::new("prorettype", DataType::UInt32, false),
            Field::new("proargtypes", DataType::Utf8, false),
            Field::new("proallargtypes", DataType::Utf8, true),
            Field::new("proargmodes", DataType::Utf8, true),
            Field::new("proargnames", DataType::Utf8, true),
            Field::new("proargdefaults", DataType::Utf8, true),
            Field::new("protrftypes", DataType::Utf8, true),
            Field::new("prosrc", DataType::Utf8, false),
            Field::new("probin", DataType::Utf8, true),
            Field::new("prosqlbody", DataType::Utf8, true),
            Field::new("proconfig", DataType::Utf8, true),
            Field::new("proacl", DataType::Utf8, true),
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
