use std::{any::Any, sync::Arc};

use async_trait::async_trait;

use datafusion::{
    arrow::{
        array::{
            Array, ArrayRef, BooleanBuilder, Float32Builder, Int16Builder, ListBuilder,
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

use super::utils::{ExtDataType, Oid, OidBuilder};

struct PgProc {
    oid: Oid,
    proname: &'static str,
    pronamespace: Oid,
    prokind: &'static str,
    prolang: Oid,
    proisstrict: bool,
    proretset: bool,
    provolatile: &'static str,
    proparallel: &'static str,
    pronargs: i16,
    prorettype: Oid,
    proargtypes: Vec<Oid>,
    prosrc: &'static str,
}

struct PgCatalogProcBuilder {
    oid: OidBuilder,
    proname: StringBuilder,
    pronamespace: OidBuilder,
    proowner: OidBuilder,
    prolang: OidBuilder,
    procost: Float32Builder,
    prorows: Float32Builder,
    provariadic: OidBuilder,
    // TODO: type regproc?
    prosupport: StringBuilder,
    prokind: StringBuilder,
    prosecdef: BooleanBuilder,
    proleakproof: BooleanBuilder,
    proisstrict: BooleanBuilder,
    proretset: BooleanBuilder,
    provolatile: StringBuilder,
    proparallel: StringBuilder,
    pronargs: Int16Builder,
    pronargdefaults: Int16Builder,
    prorettype: OidBuilder,
    // TODO: List<Oid>! Though oidvector has different output format...
    proargtypes: StringBuilder,
    proallargtypes: ListBuilder<OidBuilder>,
    proargmodes: ListBuilder<StringBuilder>,
    proargnames: ListBuilder<StringBuilder>,
    // TODO: type pg_node_tree?
    proargdefaults: StringBuilder,
    protrftypes: ListBuilder<OidBuilder>,
    prosrc: StringBuilder,
    probin: StringBuilder,
    // TODO: type pg_node_tree?
    prosqlbody: StringBuilder,
    proconfig: ListBuilder<StringBuilder>,
    // TODO: type aclitem?
    proacl: ListBuilder<StringBuilder>,
}

impl PgCatalogProcBuilder {
    fn new() -> Self {
        let capacity = 10;

        Self {
            oid: OidBuilder::new(capacity),
            proname: StringBuilder::new(capacity),
            pronamespace: OidBuilder::new(capacity),
            proowner: OidBuilder::new(capacity),
            prolang: OidBuilder::new(capacity),
            procost: Float32Builder::new(capacity),
            prorows: Float32Builder::new(capacity),
            provariadic: OidBuilder::new(capacity),
            prosupport: StringBuilder::new(capacity),
            prokind: StringBuilder::new(capacity),
            prosecdef: BooleanBuilder::new(capacity),
            proleakproof: BooleanBuilder::new(capacity),
            proisstrict: BooleanBuilder::new(capacity),
            proretset: BooleanBuilder::new(capacity),
            provolatile: StringBuilder::new(capacity),
            proparallel: StringBuilder::new(capacity),
            pronargs: Int16Builder::new(capacity),
            pronargdefaults: Int16Builder::new(capacity),
            prorettype: OidBuilder::new(capacity),
            proargtypes: StringBuilder::new(capacity),
            proallargtypes: ListBuilder::new(OidBuilder::new(capacity)),
            proargmodes: ListBuilder::new(StringBuilder::new(capacity)),
            proargnames: ListBuilder::new(StringBuilder::new(capacity)),
            proargdefaults: StringBuilder::new(capacity),
            protrftypes: ListBuilder::new(OidBuilder::new(capacity)),
            prosrc: StringBuilder::new(capacity),
            probin: StringBuilder::new(capacity),
            prosqlbody: StringBuilder::new(capacity),
            proconfig: ListBuilder::new(StringBuilder::new(capacity)),
            proacl: ListBuilder::new(StringBuilder::new(capacity)),
        }
    }

    fn add_proc(&mut self, proc: &PgProc) {
        self.oid.append_value(proc.oid).unwrap();
        self.proname.append_value(proc.proname).unwrap();
        self.pronamespace.append_value(proc.pronamespace).unwrap();
        self.proowner.append_value(10).unwrap();
        self.prolang.append_value(proc.prolang).unwrap();
        self.procost.append_value(1.0).unwrap();
        self.prorows.append_value(0.0).unwrap();
        self.provariadic.append_value(0).unwrap();
        self.prosupport.append_value("-").unwrap();
        self.prokind.append_value(proc.prokind).unwrap();
        self.prosecdef.append_value(false).unwrap();
        self.proleakproof.append_value(false).unwrap();
        self.proisstrict.append_value(proc.proisstrict).unwrap();
        self.proretset.append_value(proc.proretset).unwrap();
        self.provolatile.append_value(proc.provolatile).unwrap();
        self.proparallel.append_value(proc.proparallel).unwrap();
        self.pronargs.append_value(proc.pronargs).unwrap();
        self.pronargdefaults.append_value(0).unwrap();
        self.prorettype.append_value(proc.prorettype).unwrap();
        self.proargtypes
            .append_value(
                proc.proargtypes
                    .iter()
                    .map(|argtype| argtype.to_string())
                    .collect::<Vec<_>>()
                    .join(" "),
            )
            .unwrap();
        self.proallargtypes.append(false).unwrap();
        self.proargmodes.append(false).unwrap();
        self.proargnames.append(false).unwrap();
        self.proargdefaults.append_null().unwrap();
        self.protrftypes.append(false).unwrap();
        self.prosrc.append_value(proc.prosrc).unwrap();
        self.probin.append_null().unwrap();
        self.prosqlbody.append_null().unwrap();
        self.proconfig.append(false).unwrap();
        self.proacl.append(false).unwrap();
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
            proname: "version",
            pronamespace: 11,
            prolang: 12,
            prokind: "f",
            proisstrict: true,
            proretset: false,
            provolatile: "s",
            proparallel: "s",
            pronargs: 0,
            prorettype: 25,
            proargtypes: vec![],
            prosrc: "pgsql_version",
        });

        builder.add_proc(&PgProc {
            oid: 745,
            proname: "current_user",
            pronamespace: 11,
            prolang: 12,
            prokind: "f",
            proisstrict: true,
            proretset: false,
            provolatile: "s",
            proparallel: "s",
            pronargs: 0,
            prorettype: 19,
            proargtypes: vec![],
            prosrc: "current_user",
        });

        builder.add_proc(&PgProc {
            oid: 938,
            proname: "generate_series",
            pronamespace: 11,
            prolang: 12,
            prokind: "f",
            proisstrict: true,
            proretset: true,
            provolatile: "i",
            proparallel: "s",
            pronargs: 3,
            prorettype: 1114,
            proargtypes: vec![1114, 1114, 1186],
            prosrc: "generate_series_timestamp",
        });

        builder.add_proc(&PgProc {
            oid: 939,
            proname: "generate_series",
            pronamespace: 11,
            prolang: 12,
            prokind: "f",
            proisstrict: true,
            proretset: true,
            provolatile: "s",
            proparallel: "s",
            pronargs: 3,
            prorettype: 1184,
            proargtypes: vec![1184, 1184, 1186],
            prosrc: "generate_series_timestamptz",
        });

        builder.add_proc(&PgProc {
            oid: 1066,
            proname: "generate_series",
            pronamespace: 11,
            prolang: 12,
            prokind: "f",
            proisstrict: true,
            proretset: true,
            provolatile: "i",
            proparallel: "s",
            pronargs: 3,
            prorettype: 23,
            proargtypes: vec![23, 23, 23],
            prosrc: "generate_series_step_int4",
        });

        builder.add_proc(&PgProc {
            oid: 1067,
            proname: "generate_series",
            pronamespace: 11,
            prolang: 12,
            prokind: "f",
            proisstrict: true,
            proretset: true,
            provolatile: "i",
            proparallel: "s",
            pronargs: 2,
            prorettype: 23,
            proargtypes: vec![23, 23],
            prosrc: "generate_series_int4",
        });

        builder.add_proc(&PgProc {
            oid: 1068,
            proname: "generate_series",
            pronamespace: 11,
            prolang: 12,
            prokind: "f",
            proisstrict: true,
            proretset: true,
            provolatile: "i",
            proparallel: "s",
            pronargs: 3,
            prorettype: 20,
            proargtypes: vec![20, 20, 20],
            prosrc: "generate_series_step_int8",
        });

        builder.add_proc(&PgProc {
            oid: 1069,
            proname: "generate_series",
            pronamespace: 11,
            prolang: 12,
            prokind: "f",
            proisstrict: true,
            proretset: true,
            provolatile: "i",
            proparallel: "s",
            pronargs: 2,
            prorettype: 20,
            proargtypes: vec![20, 20],
            prosrc: "generate_series_int4",
        });

        builder.add_proc(&PgProc {
            oid: 1081,
            proname: "format_type",
            pronamespace: 11,
            prolang: 12,
            prokind: "f",
            proisstrict: false,
            proretset: false,
            provolatile: "s",
            proparallel: "s",
            pronargs: 2,
            prorettype: 25,
            proargtypes: vec![26, 23],
            prosrc: "format_type",
        });

        builder.add_proc(&PgProc {
            oid: 1178,
            proname: "date",
            pronamespace: 11,
            prolang: 12,
            prokind: "f",
            proisstrict: true,
            proretset: false,
            provolatile: "i",
            proparallel: "s",
            pronargs: 1,
            prorettype: 1082,
            proargtypes: vec![1184],
            prosrc: "timestamptz_date",
        });

        builder.add_proc(&PgProc {
            oid: 1191,
            proname: "generate_subscripts",
            pronamespace: 11,
            prolang: 12,
            prokind: "f",
            proisstrict: true,
            proretset: true,
            provolatile: "i",
            proparallel: "s",
            pronargs: 3,
            prorettype: 23,
            proargtypes: vec![2277, 23, 16],
            prosrc: "generate_subscripts",
        });

        builder.add_proc(&PgProc {
            oid: 1192,
            proname: "generate_subscripts",
            pronamespace: 11,
            prolang: 12,
            prokind: "f",
            proisstrict: true,
            proretset: true,
            provolatile: "i",
            proparallel: "s",
            pronargs: 2,
            prorettype: 23,
            proargtypes: vec![2277, 23],
            prosrc: "generate_subscripts_nodir",
        });

        builder.add_proc(&PgProc {
            oid: 1293,
            proname: "unnest",
            pronamespace: 11,
            prolang: 12,
            prokind: "f",
            proisstrict: true,
            proretset: true,
            provolatile: "i",
            proparallel: "s",
            pronargs: 1,
            prorettype: 3831,
            proargtypes: vec![4537],
            prosrc: "multirange_unnest",
        });

        builder.add_proc(&PgProc {
            oid: 1387,
            proname: "pg_get_constraintdef",
            pronamespace: 11,
            prolang: 12,
            prokind: "f",
            proisstrict: true,
            proretset: false,
            provolatile: "s",
            proparallel: "s",
            pronargs: 1,
            prorettype: 25,
            proargtypes: vec![26],
            prosrc: "pg_get_constraintdef",
        });

        builder.add_proc(&PgProc {
            oid: 1402,
            proname: "current_schema",
            pronamespace: 11,
            prolang: 12,
            prokind: "f",
            proisstrict: true,
            proretset: false,
            provolatile: "s",
            proparallel: "u",
            pronargs: 0,
            prorettype: 19,
            proargtypes: vec![],
            prosrc: "current_schema",
        });

        builder.add_proc(&PgProc {
            oid: 1403,
            proname: "current_schemas",
            pronamespace: 11,
            prolang: 12,
            prokind: "f",
            proisstrict: true,
            proretset: false,
            provolatile: "s",
            proparallel: "u",
            pronargs: 1,
            prorettype: 1003,
            proargtypes: vec![16],
            prosrc: "current_schemas",
        });

        builder.add_proc(&PgProc {
            oid: 1642,
            proname: "pg_get_userbyid",
            pronamespace: 11,
            prolang: 12,
            prokind: "f",
            proisstrict: true,
            proretset: false,
            provolatile: "s",
            proparallel: "s",
            pronargs: 1,
            prorettype: 19,
            proargtypes: vec![26],
            prosrc: "pg_get_userbyid",
        });

        builder.add_proc(&PgProc {
            oid: 1716,
            proname: "pg_get_expr",
            pronamespace: 11,
            prolang: 12,
            prokind: "f",
            proisstrict: true,
            proretset: false,
            provolatile: "s",
            proparallel: "s",
            pronargs: 2,
            prorettype: 25,
            proargtypes: vec![194, 26],
            prosrc: "pg_get_expr",
        });

        builder.add_proc(&PgProc {
            oid: 2026,
            proname: "pg_backend_pid",
            pronamespace: 11,
            prolang: 12,
            prokind: "f",
            proisstrict: true,
            proretset: false,
            provolatile: "s",
            proparallel: "r",
            pronargs: 0,
            prorettype: 23,
            proargtypes: vec![],
            prosrc: "pg_backend_pid",
        });

        builder.add_proc(&PgProc {
            oid: 2029,
            proname: "date",
            pronamespace: 11,
            prolang: 12,
            prokind: "f",
            proisstrict: true,
            proretset: false,
            provolatile: "i",
            proparallel: "s",
            pronargs: 1,
            prorettype: 1082,
            proargtypes: vec![1114],
            prosrc: "timestamp_date",
        });

        builder.add_proc(&PgProc {
            oid: 2079,
            proname: "pg_table_is_visible",
            pronamespace: 11,
            prolang: 12,
            prokind: "f",
            proisstrict: true,
            proretset: false,
            provolatile: "s",
            proparallel: "s",
            pronargs: 1,
            prorettype: 16,
            proargtypes: vec![26],
            prosrc: "pg_table_is_visible",
        });

        builder.add_proc(&PgProc {
            oid: 2080,
            proname: "pg_type_is_visible",
            pronamespace: 11,
            prolang: 12,
            prokind: "f",
            proisstrict: true,
            proretset: false,
            provolatile: "s",
            proparallel: "s",
            pronargs: 1,
            prorettype: 16,
            proargtypes: vec![26],
            prosrc: "pg_type_is_visible",
        });

        builder.add_proc(&PgProc {
            oid: 2331,
            proname: "unnest",
            pronamespace: 11,
            prolang: 12,
            prokind: "f",
            proisstrict: true,
            proretset: true,
            provolatile: "i",
            proparallel: "s",
            pronargs: 1,
            prorettype: 2283,
            proargtypes: vec![2277],
            prosrc: "array_unnest",
        });

        builder.add_proc(&PgProc {
            oid: 2400,
            proname: "array_recv",
            pronamespace: 11,
            prolang: 12,
            prokind: "f",
            proisstrict: true,
            proretset: false,
            provolatile: "s",
            proparallel: "s",
            pronargs: 3,
            prorettype: 2277,
            proargtypes: vec![2281, 26, 23],
            prosrc: "array_recv",
        });

        builder.add_proc(&PgProc {
            oid: 2404,
            proname: "int2recv",
            pronamespace: 11,
            prolang: 12,
            prokind: "f",
            proisstrict: true,
            proretset: false,
            provolatile: "s",
            proparallel: "s",
            pronargs: 1,
            prorettype: 21,
            proargtypes: vec![2281],
            prosrc: "int2recv",
        });

        builder.add_proc(&PgProc {
            oid: 2406,
            proname: "int4recv",
            pronamespace: 11,
            prolang: 12,
            prokind: "f",
            proisstrict: true,
            proretset: false,
            provolatile: "i",
            proparallel: "s",
            pronargs: 1,
            prorettype: 23,
            proargtypes: vec![2281],
            prosrc: "int4recv",
        });

        builder.add_proc(&PgProc {
            oid: 2408,
            proname: "int8recv",
            pronamespace: 11,
            prolang: 12,
            prokind: "f",
            proisstrict: true,
            proretset: false,
            provolatile: "i",
            proparallel: "s",
            pronargs: 1,
            prorettype: 20,
            proargtypes: vec![2281],
            prosrc: "int8recv",
        });

        builder.add_proc(&PgProc {
            oid: 2414,
            proname: "textrecv",
            pronamespace: 11,
            prolang: 12,
            prokind: "f",
            proisstrict: true,
            proretset: false,
            provolatile: "s",
            proparallel: "s",
            pronargs: 1,
            prorettype: 25,
            proargtypes: vec![2281],
            prosrc: "textrecv",
        });

        builder.add_proc(&PgProc {
            oid: 2420,
            proname: "oidvectorrecv",
            pronamespace: 11,
            prolang: 12,
            prokind: "f",
            proisstrict: true,
            proretset: false,
            provolatile: "i",
            proparallel: "s",
            pronargs: 1,
            prorettype: 30,
            proargtypes: vec![2281],
            prosrc: "oidvectorrecv",
        });

        builder.add_proc(&PgProc {
            oid: 2424,
            proname: "float4recv",
            pronamespace: 11,
            prolang: 12,
            prokind: "f",
            proisstrict: true,
            proretset: false,
            provolatile: "i",
            proparallel: "s",
            pronargs: 1,
            prorettype: 700,
            proargtypes: vec![2281],
            prosrc: "float4recv",
        });

        builder.add_proc(&PgProc {
            oid: 2426,
            proname: "float8recv",
            pronamespace: 11,
            prolang: 12,
            prokind: "f",
            proisstrict: true,
            proretset: false,
            provolatile: "i",
            proparallel: "s",
            pronargs: 1,
            prorettype: 701,
            proargtypes: vec![2281],
            prosrc: "float8recv",
        });

        builder.add_proc(&PgProc {
            oid: 2432,
            proname: "varcharrecv",
            pronamespace: 11,
            prolang: 12,
            prokind: "f",
            proisstrict: true,
            proretset: false,
            provolatile: "s",
            proparallel: "s",
            pronargs: 3,
            prorettype: 1043,
            proargtypes: vec![2281, 26, 23],
            prosrc: "varcharrecv",
        });

        builder.add_proc(&PgProc {
            oid: 2436,
            proname: "boolrecv",
            pronamespace: 11,
            prolang: 12,
            prokind: "f",
            proisstrict: true,
            proretset: false,
            provolatile: "s",
            proparallel: "s",
            pronargs: 1,
            prorettype: 16,
            proargtypes: vec![2281],
            prosrc: "boolrecv",
        });

        builder.add_proc(&PgProc {
            oid: 2460,
            proname: "numeric_recv",
            pronamespace: 11,
            prolang: 12,
            prokind: "f",
            proisstrict: true,
            proretset: false,
            provolatile: "i",
            proparallel: "s",
            pronargs: 3,
            prorettype: 1700,
            proargtypes: vec![2281, 26, 23],
            prosrc: "numeric_recv",
        });

        builder.add_proc(&PgProc {
            oid: 2474,
            proname: "timestamp_recv",
            pronamespace: 11,
            prolang: 12,
            prokind: "f",
            proisstrict: true,
            proretset: false,
            provolatile: "i",
            proparallel: "s",
            pronargs: 3,
            prorettype: 1114,
            proargtypes: vec![2281, 26, 23],
            prosrc: "timestamp_recv",
        });

        builder.add_proc(&PgProc {
            oid: 2508,
            proname: "pg_get_constraintdef",
            pronamespace: 11,
            prolang: 12,
            prokind: "f",
            proisstrict: true,
            proretset: false,
            provolatile: "s",
            proparallel: "s",
            pronargs: 2,
            prorettype: 25,
            proargtypes: vec![26, 16],
            prosrc: "pg_get_constraintdef_ext",
        });

        builder.add_proc(&PgProc {
            oid: 2509,
            proname: "pg_get_expr",
            pronamespace: 11,
            prolang: 12,
            prokind: "f",
            proisstrict: true,
            proretset: false,
            provolatile: "s",
            proparallel: "s",
            pronargs: 3,
            prorettype: 25,
            proargtypes: vec![194, 26, 16],
            prosrc: "pg_get_expr_ext",
        });

        builder.add_proc(&PgProc {
            oid: 3259,
            proname: "generate_series",
            pronamespace: 11,
            prolang: 12,
            prokind: "f",
            proisstrict: true,
            proretset: true,
            provolatile: "i",
            proparallel: "s",
            pronargs: 3,
            prorettype: 1700,
            proargtypes: vec![1700, 1700, 1700],
            prosrc: "generate_series_step_numeric",
        });

        builder.add_proc(&PgProc {
            oid: 3260,
            proname: "generate_series",
            pronamespace: 11,
            prolang: 12,
            prokind: "f",
            proisstrict: true,
            proretset: true,
            provolatile: "i",
            proparallel: "s",
            pronargs: 2,
            prorettype: 1700,
            proargtypes: vec![1700, 1700],
            prosrc: "generate_series_numeric",
        });

        builder.add_proc(&PgProc {
            oid: 3322,
            proname: "unnest",
            pronamespace: 11,
            prolang: 12,
            prokind: "f",
            proisstrict: true,
            proretset: true,
            provolatile: "i",
            proparallel: "s",
            pronargs: 1,
            prorettype: 2249,
            proargtypes: vec![3614],
            prosrc: "tsvector_unnest",
        });

        builder.add_proc(&PgProc {
            oid: 13392,
            proname: "_pg_expandarray",
            pronamespace: 13000,
            prolang: 14,
            prokind: "f",
            proisstrict: true,
            proretset: true,
            provolatile: "i",
            proparallel: "s",
            pronargs: 1,
            prorettype: 2249,
            proargtypes: vec![2277],
            prosrc: "",
        });

        builder.add_proc(&PgProc {
            oid: 13399,
            proname: "_pg_numeric_precision",
            pronamespace: 13000,
            prolang: 14,
            prokind: "f",
            proisstrict: true,
            proretset: false,
            provolatile: "i",
            proparallel: "s",
            pronargs: 2,
            prorettype: 23,
            proargtypes: vec![26, 23],
            prosrc: "",
        });

        builder.add_proc(&PgProc {
            oid: 13401,
            proname: "_pg_numeric_scale",
            pronamespace: 13000,
            prolang: 14,
            prokind: "f",
            proisstrict: true,
            proretset: false,
            provolatile: "i",
            proparallel: "s",
            pronargs: 2,
            prorettype: 23,
            proargtypes: vec![26, 23],
            prosrc: "",
        });

        builder.add_proc(&PgProc {
            oid: 13402,
            proname: "_pg_datetime_precision",
            pronamespace: 13000,
            prolang: 14,
            prokind: "f",
            proisstrict: true,
            proretset: false,
            provolatile: "i",
            proparallel: "s",
            pronargs: 2,
            prorettype: 23,
            proargtypes: vec![26, 23],
            prosrc: "",
        });

        for (oid, prorettype) in [
            (2050, 2277),
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
            (2128, 1186),
            (2129, 25),
            (2130, 1700),
            (2244, 1042),
            (2797, 27),
            (3526, 3500),
            (3564, 869),
            (4189, 3220),
        ]
        .iter()
        {
            builder.add_proc(&PgProc {
                oid: *oid,
                proname: "max",
                pronamespace: 11,
                prolang: 12,
                prokind: "a",
                proisstrict: false,
                proretset: false,
                provolatile: "i",
                proparallel: "s",
                pronargs: 1,
                prorettype: *prorettype,
                proargtypes: vec![*prorettype],
                prosrc: "aggregate_dummy",
            });
        }

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
            Field::new("oid", ExtDataType::Oid.into(), false),
            Field::new("proname", DataType::Utf8, false),
            Field::new("pronamespace", ExtDataType::Oid.into(), false),
            Field::new("proowner", ExtDataType::Oid.into(), false),
            Field::new("prolang", ExtDataType::Oid.into(), false),
            Field::new("procost", DataType::Float32, false),
            Field::new("prorows", DataType::Float32, false),
            Field::new("provariadic", ExtDataType::Oid.into(), false),
            Field::new("prosupport", DataType::Utf8, false),
            Field::new("prokind", DataType::Utf8, false),
            Field::new("prosecdef", DataType::Boolean, false),
            Field::new("proleakproof", DataType::Boolean, false),
            Field::new("proisstrict", DataType::Boolean, false),
            Field::new("proretset", DataType::Boolean, false),
            Field::new("provolatile", DataType::Utf8, false),
            Field::new("proparallel", DataType::Utf8, false),
            Field::new("pronargs", DataType::Int16, false),
            Field::new("pronargdefaults", DataType::Int16, false),
            Field::new("prorettype", ExtDataType::Oid.into(), false),
            Field::new("proargtypes", DataType::Utf8, false),
            Field::new(
                "proallargtypes",
                DataType::List(Box::new(Field::new("item", ExtDataType::Oid.into(), true))),
                true,
            ),
            Field::new(
                "proargmodes",
                DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new(
                "proargnames",
                DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new("proargdefaults", DataType::Utf8, true),
            Field::new(
                "protrftypes",
                DataType::List(Box::new(Field::new("item", ExtDataType::Oid.into(), true))),
                true,
            ),
            Field::new("prosrc", DataType::Utf8, false),
            Field::new("probin", DataType::Utf8, true),
            Field::new("prosqlbody", DataType::Utf8, true),
            Field::new(
                "proconfig",
                DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new(
                "proacl",
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
