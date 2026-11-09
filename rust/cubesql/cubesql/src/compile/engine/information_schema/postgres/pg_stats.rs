use std::{any::Any, sync::Arc};

use crate::transport::CubeMetaTable;
use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{
            Array, ArrayRef, BooleanBuilder, Float32Builder, Int32Builder, ListBuilder,
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

struct PgCatalogStatsBuilder {
    schemaname: StringBuilder,
    tablename: StringBuilder,
    attname: StringBuilder,
    inherited: BooleanBuilder,
    null_frac: Float32Builder,
    avg_width: Int32Builder,
    n_distinct: Float32Builder,
    // TODO: anyarray type?
    most_common_vals: ListBuilder<StringBuilder>,
    most_common_freqs: ListBuilder<Float32Builder>,
    // TODO: anyarray type?
    histogram_bounds: ListBuilder<StringBuilder>,
    correlation: Float32Builder,
    // TODO: anyarray type?
    most_common_elems: ListBuilder<StringBuilder>,
    most_common_elem_freqs: ListBuilder<Float32Builder>,
    elem_count_histogram: ListBuilder<Float32Builder>,
}

impl PgCatalogStatsBuilder {
    fn new() -> Self {
        let capacity = 10;

        Self {
            schemaname: StringBuilder::with_capacity(capacity, capacity),
            tablename: StringBuilder::with_capacity(capacity, capacity),
            attname: StringBuilder::with_capacity(capacity, capacity),
            inherited: BooleanBuilder::with_capacity(capacity),
            null_frac: Float32Builder::with_capacity(capacity),
            avg_width: Int32Builder::with_capacity(capacity),
            n_distinct: Float32Builder::with_capacity(capacity),
            most_common_vals: ListBuilder::new(StringBuilder::with_capacity(capacity, capacity)),
            most_common_freqs: ListBuilder::new(Float32Builder::with_capacity(capacity)),
            histogram_bounds: ListBuilder::new(StringBuilder::with_capacity(capacity, capacity)),
            correlation: Float32Builder::with_capacity(capacity),
            most_common_elems: ListBuilder::new(StringBuilder::with_capacity(capacity, capacity)),
            most_common_elem_freqs: ListBuilder::new(Float32Builder::with_capacity(capacity)),
            elem_count_histogram: ListBuilder::new(Float32Builder::with_capacity(capacity)),
        }
    }

    fn add_stats(
        &mut self,
        schemaname: impl AsRef<str>,
        tablename: impl AsRef<str>,
        attname: impl AsRef<str>,
        avg_width: usize,
    ) {
        self.schemaname.append_value(schemaname);
        self.tablename.append_value(tablename);
        self.attname.append_value(attname);
        self.inherited.append_value(false);
        self.null_frac.append_value(0.0);
        self.avg_width.append_value(avg_width as i32);
        self.n_distinct.append_value(0.0);
        self.most_common_vals.append(false);
        self.most_common_freqs.append(false);
        self.histogram_bounds.append(false);
        self.correlation.append_null();
        self.most_common_elems.append(false);
        self.most_common_elem_freqs.append(false);
        self.elem_count_histogram.append(false);
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(self.schemaname.finish()),
            Arc::new(self.tablename.finish()),
            Arc::new(self.attname.finish()),
            Arc::new(self.inherited.finish()),
            Arc::new(self.null_frac.finish()),
            Arc::new(self.avg_width.finish()),
            Arc::new(self.n_distinct.finish()),
            Arc::new(self.most_common_vals.finish()),
            Arc::new(self.most_common_freqs.finish()),
            Arc::new(self.histogram_bounds.finish()),
            Arc::new(self.correlation.finish()),
            Arc::new(self.most_common_elems.finish()),
            Arc::new(self.most_common_elem_freqs.finish()),
            Arc::new(self.elem_count_histogram.finish()),
        ];

        columns
    }
}

pub struct PgCatalogStatsProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl PgCatalogStatsProvider {
    pub fn new(tables: &[CubeMetaTable]) -> Self {
        let mut builder = PgCatalogStatsBuilder::new();

        for table in tables {
            for column in &table.columns {
                builder.add_stats(
                    "public",
                    &table.name,
                    &column.name,
                    column.column_type.avg_size(),
                );
            }
        }

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for PgCatalogStatsProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("schemaname", DataType::Utf8, false),
            Field::new("tablename", DataType::Utf8, false),
            Field::new("attname", DataType::Utf8, false),
            Field::new("inherited", DataType::Boolean, false),
            Field::new("null_frac", DataType::Float32, false),
            Field::new("avg_width", DataType::Int32, false),
            Field::new("n_distinct", DataType::Float32, false),
            Field::new(
                "most_common_vals",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new(
                "most_common_freqs",
                DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
                true,
            ),
            Field::new(
                "histogram_bounds",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new("correlation", DataType::Float32, true),
            Field::new(
                "most_common_elems",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new(
                "most_common_elem_freqs",
                DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
                true,
            ),
            Field::new(
                "elem_count_histogram",
                DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
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
