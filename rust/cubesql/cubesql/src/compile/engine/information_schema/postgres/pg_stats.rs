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
            schemaname: StringBuilder::new(capacity),
            tablename: StringBuilder::new(capacity),
            attname: StringBuilder::new(capacity),
            inherited: BooleanBuilder::new(capacity),
            null_frac: Float32Builder::new(capacity),
            avg_width: Int32Builder::new(capacity),
            n_distinct: Float32Builder::new(capacity),
            most_common_vals: ListBuilder::new(StringBuilder::new(capacity)),
            most_common_freqs: ListBuilder::new(Float32Builder::new(capacity)),
            histogram_bounds: ListBuilder::new(StringBuilder::new(capacity)),
            correlation: Float32Builder::new(capacity),
            most_common_elems: ListBuilder::new(StringBuilder::new(capacity)),
            most_common_elem_freqs: ListBuilder::new(Float32Builder::new(capacity)),
            elem_count_histogram: ListBuilder::new(Float32Builder::new(capacity)),
        }
    }

    fn add_stats(
        &mut self,
        schemaname: impl AsRef<str>,
        tablename: impl AsRef<str>,
        attname: impl AsRef<str>,
        avg_width: usize,
    ) {
        self.schemaname.append_value(schemaname).unwrap();
        self.tablename.append_value(tablename).unwrap();
        self.attname.append_value(attname).unwrap();
        self.inherited.append_value(false).unwrap();
        self.null_frac.append_value(0.0).unwrap();
        self.avg_width.append_value(avg_width as i32).unwrap();
        self.n_distinct.append_value(0.0).unwrap();
        self.most_common_vals.append(false).unwrap();
        self.most_common_freqs.append(false).unwrap();
        self.histogram_bounds.append(false).unwrap();
        self.correlation.append_null().unwrap();
        self.most_common_elems.append(false).unwrap();
        self.most_common_elem_freqs.append(false).unwrap();
        self.elem_count_histogram.append(false).unwrap();
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];
        columns.push(Arc::new(self.schemaname.finish()));
        columns.push(Arc::new(self.tablename.finish()));
        columns.push(Arc::new(self.attname.finish()));
        columns.push(Arc::new(self.inherited.finish()));
        columns.push(Arc::new(self.null_frac.finish()));
        columns.push(Arc::new(self.avg_width.finish()));
        columns.push(Arc::new(self.n_distinct.finish()));
        columns.push(Arc::new(self.most_common_vals.finish()));
        columns.push(Arc::new(self.most_common_freqs.finish()));
        columns.push(Arc::new(self.histogram_bounds.finish()));
        columns.push(Arc::new(self.correlation.finish()));
        columns.push(Arc::new(self.most_common_elems.finish()));
        columns.push(Arc::new(self.most_common_elem_freqs.finish()));
        columns.push(Arc::new(self.elem_count_histogram.finish()));

        columns
    }
}

pub struct PgCatalogStatsProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl PgCatalogStatsProvider {
    pub fn new(tables: &Vec<CubeMetaTable>) -> Self {
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
                DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new(
                "most_common_freqs",
                DataType::List(Box::new(Field::new("item", DataType::Float32, true))),
                true,
            ),
            Field::new(
                "histogram_bounds",
                DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new("correlation", DataType::Float32, true),
            Field::new(
                "most_common_elems",
                DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new(
                "most_common_elem_freqs",
                DataType::List(Box::new(Field::new("item", DataType::Float32, true))),
                true,
            ),
            Field::new(
                "elem_count_histogram",
                DataType::List(Box::new(Field::new("item", DataType::Float32, true))),
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
