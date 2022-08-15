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

struct RedshiftSvvTablesBuilder {
    catalog_names: StringBuilder,
    schema_names: StringBuilder,
    table_names: StringBuilder,
    table_types: StringBuilder,
    remarks: StringBuilder,
}

impl RedshiftSvvTablesBuilder {
    fn new(capacity: usize) -> Self {
        Self {
            catalog_names: StringBuilder::new(capacity),
            schema_names: StringBuilder::new(capacity),
            table_names: StringBuilder::new(capacity),
            table_types: StringBuilder::new(capacity),
            remarks: StringBuilder::new(capacity),
        }
    }

    fn add_table(
        &mut self,
        catalog_name: impl AsRef<str>,
        schema_name: impl AsRef<str>,
        table_name: impl AsRef<str>,
        table_type: impl AsRef<str>,
    ) {
        self.catalog_names
            .append_value(catalog_name.as_ref())
            .unwrap();
        self.schema_names
            .append_value(schema_name.as_ref())
            .unwrap();
        self.table_names.append_value(table_name.as_ref()).unwrap();
        self.table_types.append_value(table_type.as_ref()).unwrap();
        self.remarks.append_null().unwrap();
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];
        columns.push(Arc::new(self.catalog_names.finish()));
        columns.push(Arc::new(self.schema_names.finish()));
        columns.push(Arc::new(self.table_names.finish()));
        columns.push(Arc::new(self.table_types.finish()));
        columns.push(Arc::new(self.remarks.finish()));

        columns
    }
}

pub struct RedshiftSvvTablesTableProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl RedshiftSvvTablesTableProvider {
    pub fn new(cubes: &Vec<V1CubeMeta>) -> Self {
        let mut builder = RedshiftSvvTablesBuilder::new(cubes.len());

        for cube in cubes {
            builder.add_table("db", "public", &cube.name, "TABLE");
        }

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for RedshiftSvvTablesTableProvider {
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
            Field::new("remarks", DataType::Utf8, true),
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
