use std::{any::Any, sync::Arc};

use async_trait::async_trait;

use datafusion::{
    arrow::{
        array::{
            Array, ArrayRef, Int64Builder, StringBuilder, TimestampNanosecondBuilder, UInt32Builder,
        },
        datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

use crate::compile::CubeMetaTable;

struct PgCatalogStatUserTablesBuilder {
    relid: UInt32Builder,
    schemaname: StringBuilder,
    relname: StringBuilder,
    seq_scan: Int64Builder,
    seq_tup_read: Int64Builder,
    idx_scan: Int64Builder,
    idx_tup_fetch: Int64Builder,
    n_tup_ins: Int64Builder,
    n_tup_upd: Int64Builder,
    n_tup_del: Int64Builder,
    n_tup_hot_upd: Int64Builder,
    n_live_tup: Int64Builder,
    n_dead_tup: Int64Builder,
    n_mod_since_analyze: Int64Builder,
    n_ins_since_vacuum: Int64Builder,
    last_vacuum: TimestampNanosecondBuilder,
    last_autovacuum: TimestampNanosecondBuilder,
    last_analyze: TimestampNanosecondBuilder,
    last_autoanalyze: TimestampNanosecondBuilder,
    vacuum_count: Int64Builder,
    autovacuum_count: Int64Builder,
    analyze_count: Int64Builder,
    autoanalyze_count: Int64Builder,
}

impl PgCatalogStatUserTablesBuilder {
    fn new(capacity: usize) -> Self {
        Self {
            relid: UInt32Builder::new(capacity),
            schemaname: StringBuilder::new(capacity),
            relname: StringBuilder::new(capacity),
            seq_scan: Int64Builder::new(capacity),
            seq_tup_read: Int64Builder::new(capacity),
            idx_scan: Int64Builder::new(capacity),
            idx_tup_fetch: Int64Builder::new(capacity),
            n_tup_ins: Int64Builder::new(capacity),
            n_tup_upd: Int64Builder::new(capacity),
            n_tup_del: Int64Builder::new(capacity),
            n_tup_hot_upd: Int64Builder::new(capacity),
            n_live_tup: Int64Builder::new(capacity),
            n_dead_tup: Int64Builder::new(capacity),
            n_mod_since_analyze: Int64Builder::new(capacity),
            n_ins_since_vacuum: Int64Builder::new(capacity),
            last_vacuum: TimestampNanosecondBuilder::new(capacity),
            last_autovacuum: TimestampNanosecondBuilder::new(capacity),
            last_analyze: TimestampNanosecondBuilder::new(capacity),
            last_autoanalyze: TimestampNanosecondBuilder::new(capacity),
            vacuum_count: Int64Builder::new(capacity),
            autovacuum_count: Int64Builder::new(capacity),
            analyze_count: Int64Builder::new(capacity),
            autoanalyze_count: Int64Builder::new(capacity),
        }
    }

    fn add_table(&mut self, relid: u32, schemaname: impl AsRef<str>, relname: impl AsRef<str>) {
        self.relid.append_value(relid).unwrap();
        self.schemaname.append_value(schemaname).unwrap();
        self.relname.append_value(relname).unwrap();
        self.seq_scan.append_value(0).unwrap();
        self.seq_tup_read.append_value(0).unwrap();
        self.idx_scan.append_value(0).unwrap();
        self.idx_tup_fetch.append_value(0).unwrap();
        self.n_tup_ins.append_value(0).unwrap();
        self.n_tup_upd.append_value(0).unwrap();
        self.n_tup_del.append_value(0).unwrap();
        self.n_tup_hot_upd.append_value(0).unwrap();
        self.n_live_tup.append_value(0).unwrap();
        self.n_dead_tup.append_value(0).unwrap();
        self.n_mod_since_analyze.append_value(0).unwrap();
        self.n_ins_since_vacuum.append_value(0).unwrap();
        self.last_vacuum.append_null().unwrap();
        self.last_autovacuum.append_null().unwrap();
        self.last_analyze.append_null().unwrap();
        self.last_autoanalyze.append_null().unwrap();
        self.vacuum_count.append_value(0).unwrap();
        self.autovacuum_count.append_value(0).unwrap();
        self.analyze_count.append_value(0).unwrap();
        self.autoanalyze_count.append_value(0).unwrap();
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];

        columns.push(Arc::new(self.relid.finish()));
        columns.push(Arc::new(self.schemaname.finish()));
        columns.push(Arc::new(self.relname.finish()));
        columns.push(Arc::new(self.seq_scan.finish()));
        columns.push(Arc::new(self.seq_tup_read.finish()));
        columns.push(Arc::new(self.idx_scan.finish()));
        columns.push(Arc::new(self.idx_tup_fetch.finish()));
        columns.push(Arc::new(self.n_tup_ins.finish()));
        columns.push(Arc::new(self.n_tup_upd.finish()));
        columns.push(Arc::new(self.n_tup_del.finish()));
        columns.push(Arc::new(self.n_tup_hot_upd.finish()));
        columns.push(Arc::new(self.n_live_tup.finish()));
        columns.push(Arc::new(self.n_dead_tup.finish()));
        columns.push(Arc::new(self.n_mod_since_analyze.finish()));
        columns.push(Arc::new(self.n_ins_since_vacuum.finish()));
        columns.push(Arc::new(self.last_vacuum.finish()));
        columns.push(Arc::new(self.last_autovacuum.finish()));
        columns.push(Arc::new(self.last_analyze.finish()));
        columns.push(Arc::new(self.last_autoanalyze.finish()));
        columns.push(Arc::new(self.vacuum_count.finish()));
        columns.push(Arc::new(self.autovacuum_count.finish()));
        columns.push(Arc::new(self.analyze_count.finish()));
        columns.push(Arc::new(self.autoanalyze_count.finish()));

        columns
    }
}

pub struct PgCatalogStatUserTablesProvider {
    data: Arc<Vec<ArrayRef>>,
}

// https://www.postgresql.org/docs/14/monitoring-stats.html#MONITORING-PG-STAT-ALL-TABLES-VIEW
impl PgCatalogStatUserTablesProvider {
    pub fn new(cube_tables: &Vec<CubeMetaTable>) -> Self {
        let mut builder = PgCatalogStatUserTablesBuilder::new(cube_tables.len());

        for table in cube_tables.iter() {
            builder.add_table(table.oid, "public", &table.name);
        }

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for PgCatalogStatUserTablesProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("relid", DataType::UInt32, false),
            Field::new("schemaname", DataType::Utf8, false),
            Field::new("relname", DataType::Utf8, false),
            Field::new("seq_scan", DataType::Int64, true),
            Field::new("seq_tup_read", DataType::Int64, true),
            Field::new("idx_scan", DataType::Int64, true),
            Field::new("idx_tup_fetch", DataType::Int64, true),
            Field::new("n_tup_ins", DataType::Int64, true),
            Field::new("n_tup_upd", DataType::Int64, true),
            Field::new("n_tup_del", DataType::Int64, true),
            Field::new("n_tup_hot_upd", DataType::Int64, true),
            Field::new("n_live_tup", DataType::Int64, true),
            Field::new("n_dead_tup", DataType::Int64, true),
            Field::new("n_mod_since_analyze", DataType::Int64, true),
            Field::new("n_ins_since_vacuum", DataType::Int64, true),
            Field::new(
                "last_vacuum",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new(
                "last_autovacuum",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new(
                "last_analyze",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new(
                "last_autoanalyze",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new("vacuum_count", DataType::Int64, true),
            Field::new("autovacuum_count", DataType::Int64, true),
            Field::new("analyze_count", DataType::Int64, true),
            Field::new("autoanalyze_count", DataType::Int64, true),
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
