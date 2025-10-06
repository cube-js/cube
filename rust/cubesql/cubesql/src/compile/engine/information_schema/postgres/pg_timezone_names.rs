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

struct PgTimezoneName {
    name: &'static str,
    abbrev: &'static str,
    utc_offset: i128, // IntervalMonthDayNano
    is_dst: bool,
}

struct PgCatalogTimezoneNamesBuilder {
    name: StringBuilder,
    abbrev: StringBuilder,
    utc_offset: IntervalMonthDayNanoBuilder,
    is_dst: BooleanBuilder,
}

impl PgCatalogTimezoneNamesBuilder {
    fn new() -> Self {
        let capacity = 1;

        Self {
            name: StringBuilder::new(capacity),
            abbrev: StringBuilder::new(capacity),
            utc_offset: IntervalMonthDayNanoBuilder::new(capacity),
            is_dst: BooleanBuilder::new(capacity),
        }
    }

    fn add_timezone_name(&mut self, tzname: &PgTimezoneName) {
        self.name.append_value(tzname.name).unwrap();
        self.abbrev.append_value(tzname.abbrev).unwrap();
        self.utc_offset.append_value(tzname.utc_offset).unwrap();
        self.is_dst.append_value(tzname.is_dst).unwrap();
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(self.name.finish()),
            Arc::new(self.abbrev.finish()),
            Arc::new(self.utc_offset.finish()),
            Arc::new(self.is_dst.finish()),
        ];

        columns
    }
}

pub struct PgCatalogTimezoneNamesProvider {
    data: Arc<Vec<ArrayRef>>,
}

// https://www.postgresql.org/docs/14/view-pg-timezone-names.html
impl PgCatalogTimezoneNamesProvider {
    pub fn new() -> Self {
        let mut builder = PgCatalogTimezoneNamesBuilder::new();
        builder.add_timezone_name(&PgTimezoneName {
            name: "UTC",
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
impl TableProvider for PgCatalogTimezoneNamesProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
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
