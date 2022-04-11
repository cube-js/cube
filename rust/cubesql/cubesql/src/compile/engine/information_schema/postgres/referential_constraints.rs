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

struct InfoSchemaReferentialConstraintsBuilder {
    constraint_catalog: StringBuilder,
    constraint_schema: StringBuilder,
    constraint_name: StringBuilder,
    unique_constraint_catalog: StringBuilder,
    unique_constraint_schema: StringBuilder,
    unique_constraint_name: StringBuilder,
    match_option: StringBuilder,
    update_rule: StringBuilder,
    delete_rule: StringBuilder,
}

impl InfoSchemaReferentialConstraintsBuilder {
    fn new() -> Self {
        let capacity = 10;

        Self {
            constraint_catalog: StringBuilder::new(capacity),
            constraint_schema: StringBuilder::new(capacity),
            constraint_name: StringBuilder::new(capacity),
            unique_constraint_catalog: StringBuilder::new(capacity),
            unique_constraint_schema: StringBuilder::new(capacity),
            unique_constraint_name: StringBuilder::new(capacity),
            match_option: StringBuilder::new(capacity),
            update_rule: StringBuilder::new(capacity),
            delete_rule: StringBuilder::new(capacity),
        }
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];
        columns.push(Arc::new(self.constraint_catalog.finish()));
        columns.push(Arc::new(self.constraint_schema.finish()));
        columns.push(Arc::new(self.constraint_name.finish()));
        columns.push(Arc::new(self.unique_constraint_catalog.finish()));
        columns.push(Arc::new(self.unique_constraint_schema.finish()));
        columns.push(Arc::new(self.unique_constraint_name.finish()));
        columns.push(Arc::new(self.match_option.finish()));
        columns.push(Arc::new(self.update_rule.finish()));
        columns.push(Arc::new(self.delete_rule.finish()));

        columns
    }
}

pub struct InfoSchemaReferentialConstraintsProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl InfoSchemaReferentialConstraintsProvider {
    pub fn new() -> Self {
        let builder = InfoSchemaReferentialConstraintsBuilder::new();

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for InfoSchemaReferentialConstraintsProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("constraint_catalog", DataType::Utf8, false),
            Field::new("constraint_schema", DataType::Utf8, false),
            Field::new("constraint_name", DataType::Utf8, false),
            Field::new("unique_constraint_catalog", DataType::Utf8, false),
            Field::new("unique_constraint_schema", DataType::Utf8, false),
            Field::new("unique_constraint_name", DataType::Utf8, false),
            Field::new("match_option", DataType::Utf8, false),
            Field::new("update_rule", DataType::Utf8, false),
            Field::new("delete_rule", DataType::Utf8, false),
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
