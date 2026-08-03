use std::{any::Any, sync::Arc};

use async_trait::async_trait;

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, BooleanBuilder, IntervalMonthDayNanoBuilder, StringBuilder},
        datatypes::{DataType, Field, IntervalMonthDayNanoType, IntervalUnit, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

struct PgTimezoneAbbrev {
    abbrev: &'static str,
    utc_offset: i128, // IntervalMonthDayNano
    is_dst: bool,
}

struct PgCatalogTimezoneAbbrevsBuilder {
    abbrev: StringBuilder,
    utc_offset: IntervalMonthDayNanoBuilder,
    is_dst: BooleanBuilder,
}

impl PgCatalogTimezoneAbbrevsBuilder {
    fn new() -> Self {
        let capacity = 1;

        Self {
            abbrev: StringBuilder::new(capacity),
            utc_offset: IntervalMonthDayNanoBuilder::new(capacity),
            is_dst: BooleanBuilder::new(capacity),
        }
    }

    fn add_timezone_abbrev(&mut self, tzabbrev: &PgTimezoneAbbrev) {
        self.abbrev.append_value(tzabbrev.abbrev).unwrap();
        self.utc_offset.append_value(tzabbrev.utc_offset).unwrap();
        self.is_dst.append_value(tzabbrev.is_dst).unwrap();
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(self.abbrev.finish()),
            Arc::new(self.utc_offset.finish()),
            Arc::new(self.is_dst.finish()),
        ];

        columns
    }
}

pub struct PgCatalogTimezoneAbbrevsProvider {
    data: Arc<Vec<ArrayRef>>,
}

// https://www.postgresql.org/docs/14/view-pg-timezone-abbrevs.html
impl PgCatalogTimezoneAbbrevsProvider {
    pub fn new() -> Self {
        let mut builder = PgCatalogTimezoneAbbrevsBuilder::new();
        builder.add_timezone_abbrev(&PgTimezoneAbbrev {
            abbrev: "UTC",
            utc_offset: IntervalMonthDayNanoType::make_value(0, 0, 0),
            is_dst: false,
        });

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for PgCatalogTimezoneAbbrevsProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("abbrev", DataType::Utf8, false),
            Field::new(
                "utc_offset",
                DataType::Interval(IntervalUnit::MonthDayNano),
                false,
            ),
            Field::new("is_dst", DataType::Boolean, false),
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
