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

use super::utils::{new_boolean_array_with_placeholder, new_string_array_with_placeholder};

struct PgCatalogTablesBuilder {
    schemanames: StringBuilder,
    tablenames: StringBuilder,
    tableowners: StringBuilder,
}

impl PgCatalogTablesBuilder {
    fn new() -> Self {
        let capacity = 10;

        Self {
            schemanames: StringBuilder::new(capacity),
            tablenames: StringBuilder::new(capacity),
            tableowners: StringBuilder::new(capacity),
        }
    }

    fn add_table(
        &mut self,
        schemaname: impl AsRef<str>,
        tablename: impl AsRef<str>,
        tableowner: impl AsRef<str>,
    ) {
        self.schemanames.append_value(schemaname.as_ref()).unwrap();
        self.tablenames.append_value(tablename.as_ref()).unwrap();
        self.tableowners.append_value(tableowner.as_ref()).unwrap();
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];
        columns.push(Arc::new(self.schemanames.finish()));
        columns.push(Arc::new(self.tablenames.finish()));

        let tablesowners = self.tableowners.finish();
        let total = tablesowners.len();
        columns.push(Arc::new(tablesowners));

        // tablespace
        columns.push(Arc::new(new_string_array_with_placeholder(
            total,
            Some("".to_string()),
        )));

        // hasindexes
        columns.push(Arc::new(new_boolean_array_with_placeholder(total, true)));

        // hasrules
        columns.push(Arc::new(new_boolean_array_with_placeholder(total, false)));

        // hastriggers
        columns.push(Arc::new(new_boolean_array_with_placeholder(total, false)));

        // rowsecurity
        columns.push(Arc::new(new_boolean_array_with_placeholder(total, false)));

        columns
    }
}

pub struct PgCatalogTableProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl PgCatalogTableProvider {
    pub fn new(cubes: &Vec<V1CubeMeta>) -> Self {
        let mut builder = PgCatalogTablesBuilder::new();

        for cube in cubes {
            builder.add_table("db", cube.name.clone(), "def");
        }

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for PgCatalogTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("schemaname", DataType::Utf8, false),
            Field::new("tablename", DataType::Utf8, false),
            Field::new("tableowner", DataType::Utf8, false),
            Field::new("tablespace", DataType::Utf8, false),
            Field::new("hasindexes", DataType::Boolean, false),
            Field::new("hasrules", DataType::Boolean, false),
            Field::new("hastriggers", DataType::Boolean, false),
            Field::new("rowsecurity", DataType::Boolean, false),
        ]))
    }

    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        _batch_size: usize,
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
