use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{Array, ArrayRef, BooleanBuilder, Int16Builder, ListBuilder, StringBuilder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

use super::utils::{ExtDataType, OidBuilder};

struct PgCatalogIndexBuilder {
    indexrelid: OidBuilder,
    indrelid: OidBuilder,
    indnatts: Int16Builder,
    indnkeyatts: Int16Builder,
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
    // TODO: int2vector has different text representation
    indkey: ListBuilder<Int16Builder>,
    // TODO: oidvector has different text representation
    indcollation: ListBuilder<OidBuilder>,
    // TODO: oidvector has different text representation
    indclass: ListBuilder<OidBuilder>,
    // TODO: int2vector has different text representation
    indoption: ListBuilder<Int16Builder>,
    // TODO: type pg_node_tree?
    indexprs: StringBuilder,
    // TODO: type pg_node_tree?
    indpred: StringBuilder,
}

impl PgCatalogIndexBuilder {
    fn new() -> Self {
        let capacity = 10;

        Self {
            indexrelid: OidBuilder::new(capacity),
            indrelid: OidBuilder::new(capacity),
            indnatts: Int16Builder::new(capacity),
            indnkeyatts: Int16Builder::new(capacity),
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
            indkey: ListBuilder::new(Int16Builder::new(capacity)),
            indcollation: ListBuilder::new(OidBuilder::new(capacity)),
            indclass: ListBuilder::new(OidBuilder::new(capacity)),
            indoption: ListBuilder::new(Int16Builder::new(capacity)),
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
            Field::new("indexrelid", ExtDataType::Oid.into(), false),
            Field::new("indrelid", ExtDataType::Oid.into(), false),
            Field::new("indnatts", DataType::Int16, false),
            Field::new("indnkeyatts", DataType::Int16, false),
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
            Field::new(
                "indkey",
                DataType::List(Box::new(Field::new("item", DataType::Int16, true))),
                false,
            ),
            Field::new(
                "indcollation",
                DataType::List(Box::new(Field::new("item", ExtDataType::Oid.into(), true))),
                false,
            ),
            Field::new(
                "indclass",
                DataType::List(Box::new(Field::new("item", ExtDataType::Oid.into(), true))),
                false,
            ),
            Field::new(
                "indoption",
                DataType::List(Box::new(Field::new("item", DataType::Int16, true))),
                false,
            ),
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
