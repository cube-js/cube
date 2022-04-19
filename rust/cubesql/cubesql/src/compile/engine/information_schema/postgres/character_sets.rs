use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{Array, ArrayRef, StringBuilder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

struct InfoSchemaCharacterSetsBuilder {
    character_set_catalog: StringBuilder,
    character_set_schema: StringBuilder,
    character_set_name: StringBuilder,
    character_repertoire: StringBuilder,
    form_of_use: StringBuilder,
    default_collate_catalog: StringBuilder,
    default_collate_schema: StringBuilder,
    default_collate_name: StringBuilder,
}

impl InfoSchemaCharacterSetsBuilder {
    fn new() -> Self {
        let capacity = 10;

        Self {
            character_set_catalog: StringBuilder::new(capacity),
            character_set_schema: StringBuilder::new(capacity),
            character_set_name: StringBuilder::new(capacity),
            character_repertoire: StringBuilder::new(capacity),
            form_of_use: StringBuilder::new(capacity),
            default_collate_catalog: StringBuilder::new(capacity),
            default_collate_schema: StringBuilder::new(capacity),
            default_collate_name: StringBuilder::new(capacity),
        }
    }

    fn add_character_set(
        &mut self,
        character_set_name: impl AsRef<str> + Clone,
        character_repertoire: impl AsRef<str>,
        default_collate_catalog: impl AsRef<str>,
        default_collate_schema: impl AsRef<str>,
        default_collate_name: impl AsRef<str>,
    ) {
        self.character_set_catalog.append_null().unwrap();
        self.character_set_schema.append_null().unwrap();
        self.character_set_name
            .append_value(character_set_name.clone())
            .unwrap();
        self.character_repertoire
            .append_value(character_repertoire)
            .unwrap();
        self.form_of_use.append_value(character_set_name).unwrap();
        self.default_collate_catalog
            .append_value(default_collate_catalog)
            .unwrap();
        self.default_collate_schema
            .append_value(default_collate_schema)
            .unwrap();
        self.default_collate_name
            .append_value(default_collate_name)
            .unwrap();
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];
        columns.push(Arc::new(self.character_set_catalog.finish()));
        columns.push(Arc::new(self.character_set_schema.finish()));
        columns.push(Arc::new(self.character_set_name.finish()));
        columns.push(Arc::new(self.character_repertoire.finish()));
        columns.push(Arc::new(self.form_of_use.finish()));
        columns.push(Arc::new(self.default_collate_catalog.finish()));
        columns.push(Arc::new(self.default_collate_schema.finish()));
        columns.push(Arc::new(self.default_collate_name.finish()));

        columns
    }
}

pub struct InfoSchemaCharacterSetsProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl InfoSchemaCharacterSetsProvider {
    pub fn new(db_name: &str) -> Self {
        let mut builder = InfoSchemaCharacterSetsBuilder::new();

        builder.add_character_set("UTF8", "UCS", db_name, "pg_catalog", "en_US.utf8");

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for InfoSchemaCharacterSetsProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("character_set_catalog", DataType::Utf8, true),
            Field::new("character_set_schema", DataType::Utf8, true),
            Field::new("character_set_name", DataType::Utf8, false),
            Field::new("character_repertoire", DataType::Utf8, false),
            Field::new("form_of_use", DataType::Utf8, false),
            Field::new("default_collate_catalog", DataType::Utf8, false),
            Field::new("default_collate_schema", DataType::Utf8, false),
            Field::new("default_collate_name", DataType::Utf8, false),
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
