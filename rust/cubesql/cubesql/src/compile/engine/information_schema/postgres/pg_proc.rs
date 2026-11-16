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

use crate::compile::engine::information_schema::postgres::PG_NAMESPACE_CATALOG_OID;

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
    xmin: UInt32Builder,
}

impl PgCatalogProcBuilder {
    fn new() -> Self {
        let capacity = 10;

        Self {
            oid: UInt32Builder::with_capacity(capacity),
            proname: StringBuilder::with_capacity(capacity, capacity),
            pronamespace: UInt32Builder::with_capacity(capacity),
            proowner: UInt32Builder::with_capacity(capacity),
            prolang: UInt32Builder::with_capacity(capacity),
            procost: Int32Builder::with_capacity(capacity),
            prorows: Int32Builder::with_capacity(capacity),
            provariadic: UInt32Builder::with_capacity(capacity),
            prosupport: StringBuilder::with_capacity(capacity, capacity),
            prokind: StringBuilder::with_capacity(capacity, capacity),
            prosecdef: BooleanBuilder::with_capacity(capacity),
            proleakproof: BooleanBuilder::with_capacity(capacity),
            proisstrict: BooleanBuilder::with_capacity(capacity),
            proretset: BooleanBuilder::with_capacity(capacity),
            provolatile: StringBuilder::with_capacity(capacity, capacity),
            proparallel: StringBuilder::with_capacity(capacity, capacity),
            pronargs: Int32Builder::with_capacity(capacity),
            pronargdefaults: Int32Builder::with_capacity(capacity),
            prorettype: UInt32Builder::with_capacity(capacity),
            proargtypes: StringBuilder::with_capacity(capacity, capacity),
            proallargtypes: StringBuilder::with_capacity(capacity, capacity),
            proargmodes: StringBuilder::with_capacity(capacity, capacity),
            proargnames: StringBuilder::with_capacity(capacity, capacity),
            proargdefaults: StringBuilder::with_capacity(capacity, capacity),
            protrftypes: StringBuilder::with_capacity(capacity, capacity),
            prosrc: StringBuilder::with_capacity(capacity, capacity),
            probin: StringBuilder::with_capacity(capacity, capacity),
            prosqlbody: StringBuilder::with_capacity(capacity, capacity),
            proconfig: StringBuilder::with_capacity(capacity, capacity),
            proacl: StringBuilder::with_capacity(capacity, capacity),
            xmin: UInt32Builder::with_capacity(capacity),
        }
    }

    fn add_proc(&mut self, proc: &PgProc) {
        self.oid.append_value(proc.oid);
        self.proname.append_value(proc.proname.clone());
        self.pronamespace.append_value(PG_NAMESPACE_CATALOG_OID);
        self.proowner.append_value(10);
        self.prolang.append_value(12);
        self.procost.append_value(1);
        self.prorows.append_value(0);
        self.provariadic.append_value(0);
        self.prosupport.append_value("-".to_string());
        self.prokind.append_value(proc.prokind.clone());
        self.prosecdef.append_value(false);
        self.proleakproof.append_value(proc.proleakproof);
        self.proisstrict.append_value(proc.proisstrict);
        self.proretset.append_value(proc.proretset);
        self.provolatile.append_value(proc.provolatile.clone());
        self.proparallel.append_value(proc.proparallel.clone());
        self.pronargs.append_value(proc.pronargs);
        self.pronargdefaults.append_value(0);
        self.prorettype.append_value(proc.prorettype);
        self.proargtypes.append_value(proc.proargtypes.clone());
        self.proallargtypes
            .append_value(proc.proallargtypes.clone());
        self.proargmodes.append_value(proc.proargmodes.clone());
        self.proargnames.append_value(proc.proargnames.clone());
        self.proargdefaults.append_null();
        self.protrftypes.append_null();
        self.prosrc.append_value(proc.prosrc.clone());
        self.probin.append_null();
        self.prosqlbody.append_null();
        self.proconfig.append_null();
        self.proacl.append_null();
        self.xmin.append_value(1);
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(self.oid.finish()),
            Arc::new(self.proname.finish()),
            Arc::new(self.pronamespace.finish()),
            Arc::new(self.proowner.finish()),
            Arc::new(self.prolang.finish()),
            Arc::new(self.procost.finish()),
            Arc::new(self.prorows.finish()),
            Arc::new(self.provariadic.finish()),
            Arc::new(self.prosupport.finish()),
            Arc::new(self.prokind.finish()),
            Arc::new(self.prosecdef.finish()),
            Arc::new(self.proleakproof.finish()),
            Arc::new(self.proisstrict.finish()),
            Arc::new(self.proretset.finish()),
            Arc::new(self.provolatile.finish()),
            Arc::new(self.proparallel.finish()),
            Arc::new(self.pronargs.finish()),
            Arc::new(self.pronargdefaults.finish()),
            Arc::new(self.prorettype.finish()),
            Arc::new(self.proargtypes.finish()),
            Arc::new(self.proallargtypes.finish()),
            Arc::new(self.proargmodes.finish()),
            Arc::new(self.proargnames.finish()),
            Arc::new(self.proargdefaults.finish()),
            Arc::new(self.protrftypes.finish()),
            Arc::new(self.prosrc.finish()),
            Arc::new(self.probin.finish()),
            Arc::new(self.prosqlbody.finish()),
            Arc::new(self.proconfig.finish()),
            Arc::new(self.proacl.finish()),
            Arc::new(self.xmin.finish()),
        ];

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

        builder.add_proc(&PgProc {
            oid: 2400,
            proname: "array_recv".to_string(),
            prokind: "f".to_string(),
            proleakproof: false,
            proisstrict: true,
            proretset: false,
            provolatile: "s".to_string(),
            proparallel: "s".to_string(),
            pronargs: 3,
            prorettype: 2277,
            proargtypes: "2281 26 23".to_string(),
            // TODO: NULL
            proallargtypes: "".to_string(),
            // TODO: NULL
            proargmodes: "".to_string(),
            // TODO: NULL
            proargnames: "".to_string(),
            prosrc: "array_recv".to_string(),
        });

        builder.add_proc(&PgProc {
            oid: 2404,
            proname: "int2recv".to_string(),
            prokind: "f".to_string(),
            proleakproof: false,
            proisstrict: true,
            proretset: false,
            provolatile: "s".to_string(),
            proparallel: "s".to_string(),
            pronargs: 1,
            prorettype: 21,
            proargtypes: "2281".to_string(),
            // TODO: NULL
            proallargtypes: "".to_string(),
            // TODO: NULL
            proargmodes: "".to_string(),
            // TODO: NULL
            proargnames: "".to_string(),
            prosrc: "int2recv".to_string(),
        });

        builder.add_proc(&PgProc {
            oid: 2406,
            proname: "int4recv".to_string(),
            prokind: "f".to_string(),
            proleakproof: false,
            proisstrict: true,
            proretset: false,
            provolatile: "i".to_string(),
            proparallel: "s".to_string(),
            pronargs: 1,
            prorettype: 23,
            proargtypes: "2281".to_string(),
            // TODO: NULL
            proallargtypes: "".to_string(),
            // TODO: NULL
            proargmodes: "".to_string(),
            // TODO: NULL
            proargnames: "".to_string(),
            prosrc: "int4recv".to_string(),
        });

        builder.add_proc(&PgProc {
            oid: 2408,
            proname: "int8recv".to_string(),
            prokind: "f".to_string(),
            proleakproof: false,
            proisstrict: true,
            proretset: false,
            provolatile: "i".to_string(),
            proparallel: "s".to_string(),
            pronargs: 1,
            prorettype: 20,
            proargtypes: "2281".to_string(),
            // TODO: NULL
            proallargtypes: "".to_string(),
            // TODO: NULL
            proargmodes: "".to_string(),
            // TODO: NULL
            proargnames: "".to_string(),
            prosrc: "int8recv".to_string(),
        });

        builder.add_proc(&PgProc {
            oid: 2414,
            proname: "textrecv".to_string(),
            prokind: "f".to_string(),
            proleakproof: false,
            proisstrict: true,
            proretset: false,
            provolatile: "s".to_string(),
            proparallel: "s".to_string(),
            pronargs: 1,
            prorettype: 25,
            proargtypes: "2281".to_string(),
            // TODO: NULL
            proallargtypes: "".to_string(),
            // TODO: NULL
            proargmodes: "".to_string(),
            // TODO: NULL
            proargnames: "".to_string(),
            prosrc: "textrecv".to_string(),
        });

        builder.add_proc(&PgProc {
            oid: 2424,
            proname: "float4recv".to_string(),
            prokind: "f".to_string(),
            proleakproof: false,
            proisstrict: true,
            proretset: false,
            provolatile: "i".to_string(),
            proparallel: "s".to_string(),
            pronargs: 1,
            prorettype: 700,
            proargtypes: "2281".to_string(),
            // TODO: NULL
            proallargtypes: "".to_string(),
            // TODO: NULL
            proargmodes: "".to_string(),
            // TODO: NULL
            proargnames: "".to_string(),
            prosrc: "float4recv".to_string(),
        });

        builder.add_proc(&PgProc {
            oid: 2426,
            proname: "float8recv".to_string(),
            prokind: "f".to_string(),
            proleakproof: false,
            proisstrict: true,
            proretset: false,
            provolatile: "i".to_string(),
            proparallel: "s".to_string(),
            pronargs: 1,
            prorettype: 701,
            proargtypes: "2281".to_string(),
            // TODO: NULL
            proallargtypes: "".to_string(),
            // TODO: NULL
            proargmodes: "".to_string(),
            // TODO: NULL
            proargnames: "".to_string(),
            prosrc: "float8recv".to_string(),
        });

        builder.add_proc(&PgProc {
            oid: 2432,
            proname: "varcharrecv".to_string(),
            prokind: "f".to_string(),
            proleakproof: false,
            proisstrict: true,
            proretset: false,
            provolatile: "s".to_string(),
            proparallel: "s".to_string(),
            pronargs: 3,
            prorettype: 1043,
            proargtypes: "2281 26 23".to_string(),
            // TODO: NULL
            proallargtypes: "".to_string(),
            // TODO: NULL
            proargmodes: "".to_string(),
            // TODO: NULL
            proargnames: "".to_string(),
            prosrc: "varcharrecv".to_string(),
        });

        builder.add_proc(&PgProc {
            oid: 2460,
            proname: "numeric_recv".to_string(),
            prokind: "f".to_string(),
            proleakproof: false,
            proisstrict: true,
            proretset: false,
            provolatile: "i".to_string(),
            proparallel: "s".to_string(),
            pronargs: 3,
            prorettype: 1700,
            proargtypes: "2281 26 23".to_string(),
            // TODO: NULL
            proallargtypes: "".to_string(),
            // TODO: NULL
            proargmodes: "".to_string(),
            // TODO: NULL
            proargnames: "".to_string(),
            prosrc: "numeric_recv".to_string(),
        });

        builder.add_proc(&PgProc {
            oid: 2474,
            proname: "timestamp_recv".to_string(),
            prokind: "f".to_string(),
            proleakproof: false,
            proisstrict: true,
            proretset: false,
            provolatile: "i".to_string(),
            proparallel: "s".to_string(),
            pronargs: 3,
            prorettype: 1114,
            proargtypes: "2281 26 23".to_string(),
            // TODO: NULL
            proallargtypes: "".to_string(),
            // TODO: NULL
            proargmodes: "".to_string(),
            // TODO: NULL
            proargnames: "".to_string(),
            prosrc: "timestamp_recv".to_string(),
        });

        builder.add_proc(&PgProc {
            oid: 2436,
            proname: "boolrecv".to_string(),
            prokind: "f".to_string(),
            proleakproof: false,
            proisstrict: true,
            proretset: false,
            provolatile: "s".to_string(),
            proparallel: "s".to_string(),
            pronargs: 1,
            prorettype: 16,
            proargtypes: "2281".to_string(),
            // TODO: NULL
            proallargtypes: "".to_string(),
            // TODO: NULL
            proargmodes: "".to_string(),
            // TODO: NULL
            proargnames: "".to_string(),
            prosrc: "boolrecv".to_string(),
        });

        builder.add_proc(&PgProc {
            oid: 2420,
            proname: "oidvectorrecv".to_string(),
            prokind: "f".to_string(),
            proleakproof: false,
            proisstrict: true,
            proretset: false,
            provolatile: "i".to_string(),
            proparallel: "s".to_string(),
            pronargs: 1,
            prorettype: 30,
            proargtypes: "2281".to_string(),
            // TODO: NULL
            proallargtypes: "".to_string(),
            // TODO: NULL
            proargmodes: "".to_string(),
            // TODO: NULL
            proargnames: "".to_string(),
            prosrc: "oidvectorrecv".to_string(),
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
