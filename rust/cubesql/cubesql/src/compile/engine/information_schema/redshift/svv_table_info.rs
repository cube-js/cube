use std::{any::Any, sync::Arc};

use crate::transport::CubeMetaTable;
use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{
            Array, ArrayRef, Float32Builder, Float64Builder, Int32Builder, Int64Builder,
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

struct RedshiftSvvTableInfoBuilder {
    databases: StringBuilder,
    schemas: StringBuilder,
    table_ids: UInt32Builder,
    tables: StringBuilder,
    encoded: StringBuilder,
    diststyles: StringBuilder,
    sortkey1s: StringBuilder,
    max_varchars: Int32Builder,
    sortkey1_encs: StringBuilder,
    sortkey_nums: Int32Builder,
    sizes: Int64Builder,
    pct_used: Float64Builder,
    empty: Int64Builder,
    unsorted: Float32Builder,
    stats_off: Float32Builder,
    // TODO: Bigint? (numeric(38,0))
    tbl_rows: Float64Builder,
    skew_sortkey1s: Float64Builder,
    skew_rows: Float64Builder,
    // TODO: Bigint? (numeric(38,0))
    estimated_visible_rows: Float64Builder,
    risk_events: StringBuilder,
    vacuum_sort_benefits: Float64Builder,
}

impl RedshiftSvvTableInfoBuilder {
    fn new(capacity: usize) -> Self {
        Self {
            databases: StringBuilder::with_capacity(capacity, capacity),
            schemas: StringBuilder::with_capacity(capacity, capacity),
            table_ids: UInt32Builder::with_capacity(capacity),
            tables: StringBuilder::with_capacity(capacity, capacity),
            encoded: StringBuilder::with_capacity(capacity, capacity),
            diststyles: StringBuilder::with_capacity(capacity, capacity),
            sortkey1s: StringBuilder::with_capacity(capacity, capacity),
            max_varchars: Int32Builder::with_capacity(capacity),
            sortkey1_encs: StringBuilder::with_capacity(capacity, capacity),
            sortkey_nums: Int32Builder::with_capacity(capacity),
            sizes: Int64Builder::with_capacity(capacity),
            pct_used: Float64Builder::with_capacity(capacity),
            empty: Int64Builder::with_capacity(capacity),
            unsorted: Float32Builder::with_capacity(capacity),
            stats_off: Float32Builder::with_capacity(capacity),
            tbl_rows: Float64Builder::with_capacity(capacity),
            skew_sortkey1s: Float64Builder::with_capacity(capacity),
            skew_rows: Float64Builder::with_capacity(capacity),
            estimated_visible_rows: Float64Builder::with_capacity(capacity),
            risk_events: StringBuilder::with_capacity(capacity, capacity),
            vacuum_sort_benefits: Float64Builder::with_capacity(capacity),
        }
    }

    fn add_table(
        &mut self,
        database: impl AsRef<str>,
        schema: impl AsRef<str>,
        table_id: u32,
        table: impl AsRef<str>,
    ) {
        self.databases.append_value(database);
        self.schemas.append_value(schema);
        self.table_ids.append_value(table_id);
        self.tables.append_value(table);
        self.encoded.append_value("N");
        self.diststyles.append_value("AUTO(ALL)");
        self.sortkey1s.append_value("AUTO(SORTKEY)");
        self.max_varchars.append_value(0);
        self.sortkey1_encs.append_null();
        self.sortkey_nums.append_value(0);
        self.sizes.append_value(5);
        self.pct_used.append_value(0.0013);
        self.empty.append_value(0);
        self.unsorted.append_null();
        self.stats_off.append_value(100.0);
        // Use an arbitrarily high value of estimated number of rows
        self.tbl_rows.append_value(100000.0);
        self.skew_sortkey1s.append_null();
        self.skew_rows.append_null();
        // Use an arbitrarily high value of estimated number of rows
        self.estimated_visible_rows.append_value(100000.0);
        self.risk_events.append_null();
        self.vacuum_sort_benefits.append_null();
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(self.databases.finish()),
            Arc::new(self.schemas.finish()),
            Arc::new(self.table_ids.finish()),
            Arc::new(self.tables.finish()),
            Arc::new(self.encoded.finish()),
            Arc::new(self.diststyles.finish()),
            Arc::new(self.sortkey1s.finish()),
            Arc::new(self.max_varchars.finish()),
            Arc::new(self.sortkey1_encs.finish()),
            Arc::new(self.sortkey_nums.finish()),
            Arc::new(self.sizes.finish()),
            Arc::new(self.pct_used.finish()),
            Arc::new(self.empty.finish()),
            Arc::new(self.unsorted.finish()),
            Arc::new(self.stats_off.finish()),
            Arc::new(self.tbl_rows.finish()),
            Arc::new(self.skew_sortkey1s.finish()),
            Arc::new(self.skew_rows.finish()),
            Arc::new(self.estimated_visible_rows.finish()),
            Arc::new(self.risk_events.finish()),
            Arc::new(self.vacuum_sort_benefits.finish()),
        ];

        columns
    }
}

pub struct RedshiftSvvTableInfoProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl RedshiftSvvTableInfoProvider {
    pub fn new(db_name: impl AsRef<str>, cube_tables: &Vec<CubeMetaTable>) -> Self {
        let mut builder = RedshiftSvvTableInfoBuilder::new(cube_tables.len());

        for cube in cube_tables {
            builder.add_table(&db_name, "public", cube.oid, &cube.name);
        }

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for RedshiftSvvTableInfoProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("database", DataType::Utf8, false),
            Field::new("schema", DataType::Utf8, false),
            Field::new("table_id", DataType::UInt32, false),
            Field::new("table", DataType::Utf8, false),
            Field::new("encoded", DataType::Utf8, false),
            Field::new("diststyle", DataType::Utf8, false),
            Field::new("sortkey1", DataType::Utf8, false),
            Field::new("max_varchar", DataType::Int32, false),
            Field::new("sortkey1_enc", DataType::Utf8, true),
            Field::new("sortkey_num", DataType::Int32, false),
            Field::new("size", DataType::Int64, false),
            Field::new("pct_used", DataType::Float64, false),
            Field::new("empty", DataType::Int64, false),
            Field::new("unsorted", DataType::Float32, true),
            Field::new("stats_off", DataType::Float32, false),
            Field::new("tbl_rows", DataType::Float64, false),
            Field::new("skew_sortkey1", DataType::Float64, true),
            Field::new("skew_rows", DataType::Float64, true),
            Field::new("estimated_visible_rows", DataType::Float64, false),
            Field::new("risk_event", DataType::Utf8, true),
            Field::new("vacuum_sort_benefit", DataType::Float64, true),
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
