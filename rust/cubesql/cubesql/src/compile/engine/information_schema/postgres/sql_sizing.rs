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

struct InfoSchemaSqlSizingBuilder {
    sizing_id: UInt32Builder,
    sizing_name: StringBuilder,
    supported_value: UInt32Builder,
    comments: StringBuilder,
}

impl InfoSchemaSqlSizingBuilder {
    fn new(capacity: usize) -> Self {
        Self {
            sizing_id: UInt32Builder::new(capacity),
            sizing_name: StringBuilder::new(capacity),
            supported_value: UInt32Builder::new(capacity),
            comments: StringBuilder::new(capacity),
        }
    }

    fn add_info(
        &mut self,
        sizing_id: u32,
        sizing_name: impl AsRef<str>,
        supported_value: Option<u32>,
        comments: Option<&str>,
    ) {
        self.sizing_id.append_value(sizing_id).unwrap();
        self.sizing_name.append_value(sizing_name).unwrap();
        self.supported_value.append_option(supported_value).unwrap();
        self.comments.append_option(comments).unwrap();
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(self.sizing_id.finish()),
            Arc::new(self.sizing_name.finish()),
            Arc::new(self.supported_value.finish()),
            Arc::new(self.comments.finish()),
        ];

        columns
    }
}

pub struct InfoSchemaSqlSizingProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl InfoSchemaSqlSizingProvider {
    pub fn new() -> Self {
        let mut builder = InfoSchemaSqlSizingBuilder::new(11);

        builder.add_info(97, "MAXIMUM COLUMNS IN GROUP BY", Some(0), None);
        builder.add_info(99, "MAXIMUM COLUMNS IN ORDER BY", Some(0), None);
        builder.add_info(100, "MAXIMUM COLUMNS IN SELECT", Some(1664), None);
        builder.add_info(101, "MAXIMUM COLUMNS IN TABLE", Some(1600), None);
        builder.add_info(
            34,
            "MAXIMUM CATALOG NAME LENGTH",
            Some(63),
            Some("Might be less, depending on character set."),
        );
        builder.add_info(
            30,
            "MAXIMUM COLUMN NAME LENGTH",
            Some(63),
            Some("Might be less, depending on character set."),
        );
        builder.add_info(
            31,
            "MAXIMUM CURSOR NAME LENGTH",
            Some(63),
            Some("Might be less, depending on character set."),
        );
        builder.add_info(
            10005,
            "MAXIMUM IDENTIFIER LENGTH",
            Some(63),
            Some("Might be less, depending on character set."),
        );
        builder.add_info(
            32,
            "MAXIMUM SCHEMA NAME LENGTH",
            Some(63),
            Some("Might be less, depending on character set."),
        );
        builder.add_info(
            35,
            "MAXIMUM TABLE NAME LENGTH",
            Some(63),
            Some("Might be less, depending on character set."),
        );
        builder.add_info(
            107,
            "MAXIMUM USER NAME LENGTH",
            Some(63),
            Some("Might be less, depending on character set."),
        );

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for InfoSchemaSqlSizingProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("sizing_id", DataType::UInt32, false),
            Field::new("sizing_name", DataType::Utf8, false),
            Field::new("supported_value", DataType::UInt32, true),
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
