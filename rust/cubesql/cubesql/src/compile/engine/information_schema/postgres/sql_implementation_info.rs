use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{Array, ArrayRef, StringBuilder, UInt32Builder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

struct InfoSchemaSqlImplementationInfoBuilder {
    implementation_info_id: StringBuilder,
    implementation_info_name: StringBuilder,
    integer_value: UInt32Builder,
    character_value: StringBuilder,
    comments: StringBuilder,
}

impl InfoSchemaSqlImplementationInfoBuilder {
    fn new(capacity: usize) -> Self {
        Self {
            implementation_info_id: StringBuilder::new(capacity),
            implementation_info_name: StringBuilder::new(capacity),
            integer_value: UInt32Builder::new(capacity),
            character_value: StringBuilder::new(capacity),
            comments: StringBuilder::new(capacity),
        }
    }

    fn add_info(
        &mut self,
        implementation_info_id: impl AsRef<str>,
        implementation_info_name: impl AsRef<str>,
        integer_value: Option<u32>,
        character_value: Option<&str>,
        comments: Option<&str>,
    ) {
        self.implementation_info_id
            .append_value(&implementation_info_id)
            .unwrap();
        self.implementation_info_name
            .append_value(&implementation_info_name)
            .unwrap();
        self.integer_value.append_option(integer_value).unwrap();
        self.character_value.append_option(character_value).unwrap();
        self.comments.append_option(comments).unwrap();
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(self.implementation_info_id.finish()),
            Arc::new(self.implementation_info_name.finish()),
            Arc::new(self.integer_value.finish()),
            Arc::new(self.character_value.finish()),
            Arc::new(self.comments.finish()),
        ];

        columns
    }
}

pub struct InfoSchemaSqlImplementationInfoProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl InfoSchemaSqlImplementationInfoProvider {
    pub fn new() -> Self {
        let mut builder = InfoSchemaSqlImplementationInfoBuilder::new(2);

        builder.add_info("17", "DBMS NAME", None, Some("PostgreSQL"), None);
        builder.add_info("18", "DBMS VERSION", None, Some("14.02.0000)"), None);

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for InfoSchemaSqlImplementationInfoProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("implementation_info_id", DataType::Utf8, false),
            Field::new("implementation_info_name", DataType::Utf8, false),
            Field::new("integer_value", DataType::UInt32, true),
            Field::new("character_value", DataType::Utf8, true),
            Field::new("comments", DataType::Utf8, true),
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
