use std::{any::Any, sync::Arc};

use crate::transport::V1CubeMetaExt;
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

struct InfoSchemaRoleColumnGrantsBuilder {
    grantor: StringBuilder,
    grantee: StringBuilder,
    table_catalog: StringBuilder,
    table_schema: StringBuilder,
    table_name: StringBuilder,
    column_name: StringBuilder,
    privilege_type: StringBuilder,
    is_grantable: StringBuilder,
}

impl InfoSchemaRoleColumnGrantsBuilder {
    fn new(capacity: usize) -> Self {
        Self {
            grantor: StringBuilder::new(capacity),
            grantee: StringBuilder::new(capacity),
            table_catalog: StringBuilder::new(capacity),
            table_schema: StringBuilder::new(capacity),
            table_name: StringBuilder::new(capacity),
            column_name: StringBuilder::new(capacity),
            privilege_type: StringBuilder::new(capacity),
            is_grantable: StringBuilder::new(capacity),
        }
    }

    fn add_column(
        &mut self,
        user: impl AsRef<str>,
        table_catalog: impl AsRef<str>,
        table_schema: impl AsRef<str>,
        table_name: impl AsRef<str>,
        column_name: impl AsRef<str>,
        privilege_type: impl AsRef<str>,
    ) {
        self.grantor.append_value(&user).unwrap();
        self.grantee.append_value(&user).unwrap();
        self.table_catalog.append_value(&table_catalog).unwrap();
        self.table_schema.append_value(&table_schema).unwrap();
        self.table_name.append_value(&table_name).unwrap();
        self.column_name.append_value(&column_name).unwrap();
        self.privilege_type.append_value(&privilege_type).unwrap();
        self.is_grantable.append_value(&"YES").unwrap();
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];
        columns.push(Arc::new(self.grantor.finish()));
        columns.push(Arc::new(self.grantee.finish()));
        columns.push(Arc::new(self.table_catalog.finish()));
        columns.push(Arc::new(self.table_schema.finish()));
        columns.push(Arc::new(self.table_name.finish()));
        columns.push(Arc::new(self.column_name.finish()));
        columns.push(Arc::new(self.privilege_type.finish()));
        columns.push(Arc::new(self.is_grantable.finish()));

        columns
    }
}

pub struct InfoSchemaRoleColumnGrantsProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl InfoSchemaRoleColumnGrantsProvider {
    pub fn new(current_user: String, cubes: &Vec<V1CubeMeta>) -> Self {
        let mut builder = InfoSchemaRoleColumnGrantsBuilder::new(cubes.len());

        for cube in cubes {
            for column in cube.get_columns() {
                builder.add_column(
                    &current_user,
                    "db",
                    "public",
                    cube.name.clone(),
                    &column.get_name(),
                    &"SELECT",
                );
            }
        }

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for InfoSchemaRoleColumnGrantsProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("grantor", DataType::Utf8, false),
            Field::new("grantee", DataType::Utf8, false),
            Field::new("table_catalog", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("column_name", DataType::Utf8, false),
            Field::new("privilege_type", DataType::Utf8, false),
            Field::new("is_grantable", DataType::Utf8, false),
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
