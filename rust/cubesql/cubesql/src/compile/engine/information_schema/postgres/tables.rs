use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use cubeclient::models::V1CubeMeta;
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

use crate::compile::engine::information_schema::utils::ExtDataType;

use super::utils::{new_string_array_with_placeholder, new_yes_no_array_with_placeholder};

struct InformationSchemaTablesBuilder {
    table_catalog: StringBuilder,
    table_schema: StringBuilder,
    table_name: StringBuilder,
    table_type: StringBuilder,
}

impl InformationSchemaTablesBuilder {
    fn new() -> Self {
        let capacity = 10;

        Self {
            table_catalog: StringBuilder::new(capacity),
            table_schema: StringBuilder::new(capacity),
            table_name: StringBuilder::new(capacity),
            table_type: StringBuilder::new(capacity),
        }
    }

    fn add_table(
        &mut self,
        table_catalog: impl AsRef<str>,
        table_schema: impl AsRef<str>,
        table_name: impl AsRef<str>,
        table_type: impl AsRef<str>,
    ) {
        self.table_catalog.append_value(table_catalog).unwrap();
        self.table_schema.append_value(table_schema).unwrap();
        self.table_name.append_value(table_name).unwrap();
        self.table_type.append_value(table_type).unwrap();
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];

        let table_catalog = self.table_catalog.finish();
        let total = table_catalog.len();
        columns.push(Arc::new(table_catalog));
        columns.push(Arc::new(self.table_schema.finish()));
        columns.push(Arc::new(self.table_name.finish()));
        columns.push(Arc::new(self.table_type.finish()));

        // self_referencing_column_name
        columns.push(Arc::new(new_string_array_with_placeholder(total, None)));

        // reference_generation
        columns.push(Arc::new(new_string_array_with_placeholder(total, None)));

        // user_defined_type_catalog
        columns.push(Arc::new(new_string_array_with_placeholder(total, None)));

        // user_defined_type_schema
        columns.push(Arc::new(new_string_array_with_placeholder(total, None)));

        // user_defined_type_name
        columns.push(Arc::new(new_string_array_with_placeholder(total, None)));

        // is_insertable_into
        columns.push(Arc::new(new_yes_no_array_with_placeholder(
            total,
            Some(false),
        )));

        // is_typed
        columns.push(Arc::new(new_yes_no_array_with_placeholder(
            total,
            Some(false),
        )));

        // commit_action
        columns.push(Arc::new(new_string_array_with_placeholder(total, None)));

        columns
    }
}

pub struct InfoSchemaTableProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl InfoSchemaTableProvider {
    pub fn new(db_name: &str, cubes: &Vec<V1CubeMeta>) -> Self {
        let mut builder = InformationSchemaTablesBuilder::new();
        // information_schema
        builder.add_table(db_name, "information_schema", "tables", "VIEW");
        builder.add_table(db_name, "information_schema", "columns", "VIEW");
        builder.add_table(db_name, "pg_catalog", "pg_tables", "VIEW");

        for cube in cubes {
            builder.add_table(db_name, "public", cube.name.clone(), "BASE TABLE");
        }

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for InfoSchemaTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("table_catalog", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
            Field::new("self_referencing_column_name", DataType::Utf8, true),
            Field::new("reference_generation", DataType::Utf8, true),
            Field::new("user_defined_type_catalog", DataType::Utf8, true),
            Field::new("user_defined_type_schema", DataType::Utf8, true),
            Field::new("user_defined_type_name", DataType::Utf8, true),
            Field::new("is_insertable_into", ExtDataType::YesNo.into(), false),
            Field::new("is_typed", ExtDataType::YesNo.into(), false),
            Field::new("commit_action", DataType::Utf8, true),
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
