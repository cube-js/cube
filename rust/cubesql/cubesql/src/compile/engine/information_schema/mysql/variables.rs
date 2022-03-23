use std::{any::Any, collections::HashMap, sync::Arc};

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

pub struct PerfSchemaVariablesProvider {
    variables: HashMap<String, String>,
}

impl PerfSchemaVariablesProvider {
    pub fn new() -> Self {
        let mut variables = HashMap::new();
        variables.insert("max_allowed_packet".to_string(), "67108864".to_string());
        variables.insert("sql_mode".to_string(), "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION".to_string());
        variables.insert("lower_case_table_names".to_string(), "0".to_string());

        Self { variables }
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
        _batch_size: usize,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let mut names = StringBuilder::new(100);
        let mut values = StringBuilder::new(100);

        for (key, value) in self.variables.iter() {
            names.append_value(key.clone()).unwrap();
            values.append_value(value.clone()).unwrap();
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
