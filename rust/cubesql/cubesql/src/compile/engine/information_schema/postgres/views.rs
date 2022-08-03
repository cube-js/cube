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

use super::utils::{ExtDataType, YesNoBuilder};

struct InfoSchemaViewsBuilder {
    table_catalog: StringBuilder,
    table_schema: StringBuilder,
    table_name: StringBuilder,
    view_definition: StringBuilder,
    check_option: StringBuilder,
    is_updatable: YesNoBuilder,
    is_insertable_into: YesNoBuilder,
    is_trigger_updatable: YesNoBuilder,
    is_trigger_deletable: YesNoBuilder,
    is_trigger_insertable_into: YesNoBuilder,
}

impl InfoSchemaViewsBuilder {
    fn new() -> Self {
        let capacity = 1;

        Self {
            table_catalog: StringBuilder::new(capacity),
            table_schema: StringBuilder::new(capacity),
            table_name: StringBuilder::new(capacity),
            view_definition: StringBuilder::new(capacity),
            check_option: StringBuilder::new(capacity),
            is_updatable: YesNoBuilder::new(capacity),
            is_insertable_into: YesNoBuilder::new(capacity),
            is_trigger_updatable: YesNoBuilder::new(capacity),
            is_trigger_deletable: YesNoBuilder::new(capacity),
            is_trigger_insertable_into: YesNoBuilder::new(capacity),
        }
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];
        columns.push(Arc::new(self.table_catalog.finish()));
        columns.push(Arc::new(self.table_schema.finish()));
        columns.push(Arc::new(self.table_name.finish()));
        columns.push(Arc::new(self.view_definition.finish()));
        columns.push(Arc::new(self.check_option.finish()));
        columns.push(Arc::new(self.is_updatable.finish()));
        columns.push(Arc::new(self.is_insertable_into.finish()));
        columns.push(Arc::new(self.is_trigger_updatable.finish()));
        columns.push(Arc::new(self.is_trigger_deletable.finish()));
        columns.push(Arc::new(self.is_trigger_insertable_into.finish()));

        columns
    }
}

pub struct InfoSchemaViewsProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl InfoSchemaViewsProvider {
    pub fn new() -> Self {
        let builder = InfoSchemaViewsBuilder::new();

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for InfoSchemaViewsProvider {
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
            Field::new("view_definition", DataType::Utf8, false),
            Field::new("check_option", DataType::Utf8, false),
            Field::new("is_updatable", ExtDataType::YesNo.into(), false),
            Field::new("is_insertable_into", ExtDataType::YesNo.into(), false),
            Field::new("is_trigger_updatable", ExtDataType::YesNo.into(), false),
            Field::new("is_trigger_deletable", ExtDataType::YesNo.into(), false),
            Field::new(
                "is_trigger_insertable_into",
                ExtDataType::YesNo.into(),
                false,
            ),
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
