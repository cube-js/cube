use std::{any::Any, sync::Arc};

use async_trait::async_trait;

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, BooleanBuilder, ListBuilder, StringBuilder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

struct PgCatalogAvailableExtensionVersionsBuilder {
    name: StringBuilder,
    version: StringBuilder,
    installed: BooleanBuilder,
    superuser: BooleanBuilder,
    trusted: BooleanBuilder,
    relocatable: BooleanBuilder,
    schema: StringBuilder,
    requires: ListBuilder<StringBuilder>,
    comment: StringBuilder,
}

impl PgCatalogAvailableExtensionVersionsBuilder {
    fn new() -> Self {
        let capacity = 1;

        Self {
            name: StringBuilder::new(capacity),
            version: StringBuilder::new(capacity),
            installed: BooleanBuilder::new(capacity),
            superuser: BooleanBuilder::new(capacity),
            trusted: BooleanBuilder::new(capacity),
            relocatable: BooleanBuilder::new(capacity),
            schema: StringBuilder::new(capacity),
            requires: ListBuilder::new(StringBuilder::new(capacity)),
            comment: StringBuilder::new(capacity),
        }
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(self.name.finish()),
            Arc::new(self.version.finish()),
            Arc::new(self.installed.finish()),
            Arc::new(self.superuser.finish()),
            Arc::new(self.trusted.finish()),
            Arc::new(self.relocatable.finish()),
            Arc::new(self.schema.finish()),
            Arc::new(self.requires.finish()),
            Arc::new(self.comment.finish()),
        ];

        columns
    }
}

pub struct PgCatalogAvailableExtensionVersionsProvider {
    data: Arc<Vec<ArrayRef>>,
}

// https://www.postgresql.org/docs/14/view-pg-available-extension-versions.html
impl PgCatalogAvailableExtensionVersionsProvider {
    pub fn new() -> Self {
        let builder = PgCatalogAvailableExtensionVersionsBuilder::new();

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for PgCatalogAvailableExtensionVersionsProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("version", DataType::Utf8, false),
            Field::new("installed", DataType::Boolean, false),
            Field::new("superuser", DataType::Boolean, false),
            Field::new("trusted", DataType::Boolean, false),
            Field::new("relocatable", DataType::Boolean, false),
            Field::new("schema", DataType::Utf8, true),
            Field::new(
                "requires",
                DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new("comment", DataType::Utf8, true),
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
