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

struct InformationSchemaSchemataBuilder {
    catalog_names: StringBuilder,
    schema_names: StringBuilder,
    schema_owners: StringBuilder,
    default_character_set_catalogs: StringBuilder,
    default_character_set_schemas: StringBuilder,
    default_character_set_names: StringBuilder,
    sql_paths: StringBuilder,
}

impl InformationSchemaSchemataBuilder {
    fn new(capacity: usize) -> Self {
        Self {
            catalog_names: StringBuilder::new(capacity),
            schema_names: StringBuilder::new(capacity),
            schema_owners: StringBuilder::new(capacity),
            default_character_set_catalogs: StringBuilder::new(capacity),
            default_character_set_schemas: StringBuilder::new(capacity),
            default_character_set_names: StringBuilder::new(capacity),
            sql_paths: StringBuilder::new(capacity),
        }
    }

    fn add_schema(
        &mut self,
        catalog_name: impl AsRef<str>,
        schema_name: impl AsRef<str>,
        schema_owner: impl AsRef<str>,
    ) {
        self.catalog_names
            .append_value(catalog_name.as_ref())
            .unwrap();
        self.schema_names
            .append_value(schema_name.as_ref())
            .unwrap();
        self.schema_owners
            .append_value(schema_owner.as_ref())
            .unwrap();
        self.default_character_set_catalogs.append_null().unwrap();
        self.default_character_set_schemas.append_null().unwrap();
        self.default_character_set_names.append_null().unwrap();
        self.sql_paths.append_null().unwrap();
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];

        columns.push(Arc::new(self.catalog_names.finish()));
        columns.push(Arc::new(self.schema_names.finish()));
        columns.push(Arc::new(self.schema_owners.finish()));
        columns.push(Arc::new(self.default_character_set_catalogs.finish()));
        columns.push(Arc::new(self.default_character_set_schemas.finish()));
        columns.push(Arc::new(self.default_character_set_names.finish()));
        columns.push(Arc::new(self.sql_paths.finish()));

        columns
    }
}

pub struct InfoSchemaSchemataProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl InfoSchemaSchemataProvider {
    pub fn new(db_name: &str) -> Self {
        let mut builder = InformationSchemaSchemataBuilder::new(4);

        builder.add_schema(db_name, "public", "pg_database_owner");
        builder.add_schema(db_name, "information_schema", "postgres");
        builder.add_schema(db_name, "pg_catalog", "postgres");
        builder.add_schema(db_name, "pg_toast", "postgres");

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for InfoSchemaSchemataProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, false),
            Field::new("schema_name", DataType::Utf8, false),
            Field::new("schema_owner", DataType::Utf8, false),
            Field::new("default_character_set_catalog", DataType::Utf8, true),
            Field::new("default_character_set_schema", DataType::Utf8, true),
            Field::new("default_character_set_name", DataType::Utf8, true),
            Field::new("sql_path", DataType::Utf8, true),
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
