use std::{any::Any, sync::Arc};

use async_trait::async_trait;

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, BooleanBuilder, StringBuilder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

struct PgCatalogMatviewsBuilder {
    schemaname: StringBuilder,
    matviewname: StringBuilder,
    matviewowner: StringBuilder,
    tablespace: StringBuilder,
    hasindexes: BooleanBuilder,
    ispopulated: BooleanBuilder,
    definition: StringBuilder,
}

impl PgCatalogMatviewsBuilder {
    fn new() -> Self {
        let capacity = 1;

        Self {
            schemaname: StringBuilder::new(capacity),
            matviewname: StringBuilder::new(capacity),
            matviewowner: StringBuilder::new(capacity),
            tablespace: StringBuilder::new(capacity),
            hasindexes: BooleanBuilder::new(capacity),
            ispopulated: BooleanBuilder::new(capacity),
            definition: StringBuilder::new(capacity),
        }
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];

        columns.push(Arc::new(self.schemaname.finish()));
        columns.push(Arc::new(self.matviewname.finish()));
        columns.push(Arc::new(self.matviewowner.finish()));
        columns.push(Arc::new(self.tablespace.finish()));
        columns.push(Arc::new(self.hasindexes.finish()));
        columns.push(Arc::new(self.ispopulated.finish()));
        columns.push(Arc::new(self.definition.finish()));

        columns
    }
}

pub struct PgCatalogMatviewsProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl PgCatalogMatviewsProvider {
    pub fn new() -> Self {
        let builder = PgCatalogMatviewsBuilder::new();

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for PgCatalogMatviewsProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("schemaname", DataType::Utf8, false),
            Field::new("matviewname", DataType::Utf8, false),
            Field::new("matviewowner", DataType::Utf8, true),
            Field::new("tablespace", DataType::Utf8, false),
            Field::new("hasindexes", DataType::Boolean, false),
            Field::new("ispopulated", DataType::Boolean, false),
            Field::new("definition", DataType::Utf8, true),
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
