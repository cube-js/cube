use std::{any::Any, sync::Arc};

use crate::compile::engine::provider::TableName;
use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{Array, StringBuilder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

use crate::sql::database_variables::DatabaseVariables;

pub struct PerfSchemaVariablesProvider {
    table_name: String,
    variables: DatabaseVariables,
}

impl TableName for PerfSchemaVariablesProvider {
    fn table_name(&self) -> &str {
        &self.table_name
    }
}

impl PerfSchemaVariablesProvider {
    pub fn new(table_name: String, vars: DatabaseVariables) -> Self {
        Self {
            table_name,
            variables: vars,
        }
    }
}

#[async_trait]
impl TableProvider for PerfSchemaVariablesProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("VARIABLE_NAME", DataType::Utf8, false),
            Field::new("VARIABLE_VALUE", DataType::Utf8, false),
        ]))
    }

    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let mut names = StringBuilder::new(100);
        let mut values = StringBuilder::new(100);

        for (key, variable) in self.variables.iter() {
            names.append_value(key.clone()).unwrap();
            values.append_value(variable.value.to_string()).unwrap();
        }

        let mut data: Vec<Arc<dyn Array>> = vec![];
        data.push(Arc::new(names.finish()));
        data.push(Arc::new(values.finish()));

        let batch = RecordBatch::try_new(self.schema(), data)?;

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
