use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{Array, ArrayRef, BooleanBuilder, StringBuilder, UInt16Builder, UInt32Builder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

struct PgCatalogIndexBuilder {
    indexrelid: UInt32Builder,
    indrelid: UInt32Builder,
    indnatts: UInt16Builder,
    indnkeyatts: UInt16Builder,
    indisunique: BooleanBuilder,
    indisprimary: BooleanBuilder,
    indisexclusion: BooleanBuilder,
    indimmediate: BooleanBuilder,
    indisclustered: BooleanBuilder,
    indisvalid: BooleanBuilder,
    indcheckxmin: BooleanBuilder,
    indisready: BooleanBuilder,
    indislive: BooleanBuilder,
    indisreplident: BooleanBuilder,
    indkey: StringBuilder,
    indcollation: StringBuilder,
    indclass: StringBuilder,
    indoption: StringBuilder,
    indexprs: StringBuilder,
    indpred: StringBuilder,
}

impl PgCatalogIndexBuilder {
    fn new() -> Self {
        let capacity = 10;

        Self {
            indexrelid: UInt32Builder::new(capacity),
            indrelid: UInt32Builder::new(capacity),
            indnatts: UInt16Builder::new(capacity),
            indnkeyatts: UInt16Builder::new(capacity),
            indisunique: BooleanBuilder::new(capacity),
            indisprimary: BooleanBuilder::new(capacity),
            indisexclusion: BooleanBuilder::new(capacity),
            indimmediate: BooleanBuilder::new(capacity),
            indisclustered: BooleanBuilder::new(capacity),
            indisvalid: BooleanBuilder::new(capacity),
            indcheckxmin: BooleanBuilder::new(capacity),
            indisready: BooleanBuilder::new(capacity),
            indislive: BooleanBuilder::new(capacity),
            indisreplident: BooleanBuilder::new(capacity),
            indkey: StringBuilder::new(capacity),
            indcollation: StringBuilder::new(capacity),
            indclass: StringBuilder::new(capacity),
            indoption: StringBuilder::new(capacity),
            indexprs: StringBuilder::new(capacity),
            indpred: StringBuilder::new(capacity),
        }
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];
        columns.push(Arc::new(self.indexrelid.finish()));
        columns.push(Arc::new(self.indrelid.finish()));
        columns.push(Arc::new(self.indnatts.finish()));
        columns.push(Arc::new(self.indnkeyatts.finish()));
        columns.push(Arc::new(self.indisunique.finish()));
        columns.push(Arc::new(self.indisprimary.finish()));
        columns.push(Arc::new(self.indisexclusion.finish()));
        columns.push(Arc::new(self.indimmediate.finish()));
        columns.push(Arc::new(self.indisclustered.finish()));
        columns.push(Arc::new(self.indisvalid.finish()));
        columns.push(Arc::new(self.indcheckxmin.finish()));
        columns.push(Arc::new(self.indisready.finish()));
        columns.push(Arc::new(self.indislive.finish()));
        columns.push(Arc::new(self.indisreplident.finish()));
        columns.push(Arc::new(self.indkey.finish()));
        columns.push(Arc::new(self.indcollation.finish()));
        columns.push(Arc::new(self.indclass.finish()));
        columns.push(Arc::new(self.indoption.finish()));
        columns.push(Arc::new(self.indexprs.finish()));
        columns.push(Arc::new(self.indpred.finish()));

        columns
    }
}

pub struct PgCatalogIndexProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl PgCatalogIndexProvider {
    pub fn new() -> Self {
        let builder = PgCatalogIndexBuilder::new();

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for PgCatalogIndexProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("indexrelid", DataType::UInt32, false),
            Field::new("indrelid", DataType::UInt32, false),
            Field::new("indnatts", DataType::UInt16, false),
            Field::new("indnkeyatts", DataType::UInt16, false),
            Field::new("indisunique", DataType::Boolean, false),
            Field::new("indisprimary", DataType::Boolean, false),
            Field::new("indisexclusion", DataType::Boolean, false),
            Field::new("indimmediate", DataType::Boolean, false),
            Field::new("indisclustered", DataType::Boolean, false),
            Field::new("indisvalid", DataType::Boolean, false),
            Field::new("indcheckxmin", DataType::Boolean, false),
            Field::new("indisready", DataType::Boolean, false),
            Field::new("indislive", DataType::Boolean, false),
            Field::new("indisreplident", DataType::Boolean, false),
            Field::new("indkey", DataType::Utf8, false),
            Field::new("indcollation", DataType::Utf8, false),
            Field::new("indclass", DataType::Utf8, false),
            Field::new("indoption", DataType::Utf8, false),
            Field::new("indexprs", DataType::Utf8, true),
            Field::new("indpred", DataType::Utf8, true),
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
