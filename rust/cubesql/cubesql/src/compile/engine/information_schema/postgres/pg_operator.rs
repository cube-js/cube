use std::{any::Any, sync::Arc};

use async_trait::async_trait;

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, BooleanBuilder, StringBuilder, UInt32Builder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

struct PgCatalogOperatorBuilder {
    oid: UInt32Builder,
    oprname: StringBuilder,
    oprnamespace: UInt32Builder,
    oprowner: UInt32Builder,
    oprkind: StringBuilder,
    oprcanmerge: BooleanBuilder,
    oprcanhash: BooleanBuilder,
    oprleft: UInt32Builder,
    oprright: UInt32Builder,
    oprresult: UInt32Builder,
    oprcom: UInt32Builder,
    oprnegate: UInt32Builder,
    oprcode: StringBuilder,
    oprrest: StringBuilder,
    oprjoin: StringBuilder,
    xmin: UInt32Builder,
}

impl PgCatalogOperatorBuilder {
    fn new() -> Self {
        let capacity = 1;

        Self {
            oid: UInt32Builder::new(capacity),
            oprname: StringBuilder::new(capacity),
            oprnamespace: UInt32Builder::new(capacity),
            oprowner: UInt32Builder::new(capacity),
            oprkind: StringBuilder::new(capacity),
            oprcanmerge: BooleanBuilder::new(capacity),
            oprcanhash: BooleanBuilder::new(capacity),
            oprleft: UInt32Builder::new(capacity),
            oprright: UInt32Builder::new(capacity),
            oprresult: UInt32Builder::new(capacity),
            oprcom: UInt32Builder::new(capacity),
            oprnegate: UInt32Builder::new(capacity),
            oprcode: StringBuilder::new(capacity),
            oprrest: StringBuilder::new(capacity),
            oprjoin: StringBuilder::new(capacity),
            xmin: UInt32Builder::new(capacity),
        }
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(self.oid.finish()),
            Arc::new(self.oprname.finish()),
            Arc::new(self.oprnamespace.finish()),
            Arc::new(self.oprowner.finish()),
            Arc::new(self.oprkind.finish()),
            Arc::new(self.oprcanmerge.finish()),
            Arc::new(self.oprcanhash.finish()),
            Arc::new(self.oprleft.finish()),
            Arc::new(self.oprright.finish()),
            Arc::new(self.oprresult.finish()),
            Arc::new(self.oprcom.finish()),
            Arc::new(self.oprnegate.finish()),
            Arc::new(self.oprcode.finish()),
            Arc::new(self.oprrest.finish()),
            Arc::new(self.oprjoin.finish()),
            Arc::new(self.xmin.finish()),
        ];

        columns
    }
}

pub struct PgCatalogOperatorProvider {
    data: Arc<Vec<ArrayRef>>,
}

// https://www.postgresql.org/docs/14/catalog-pg-operator.html
impl PgCatalogOperatorProvider {
    pub fn new() -> Self {
        let builder = PgCatalogOperatorBuilder::new();

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for PgCatalogOperatorProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::UInt32, false),
            Field::new("oprname", DataType::Utf8, false),
            Field::new("oprnamespace", DataType::UInt32, false),
            Field::new("oprowner", DataType::UInt32, false),
            Field::new("oprkind", DataType::Utf8, false),
            Field::new("oprcanmerge", DataType::Boolean, false),
            Field::new("oprcanhash", DataType::Boolean, false),
            Field::new("oprleft", DataType::UInt32, false),
            Field::new("oprright", DataType::UInt32, false),
            Field::new("oprresult", DataType::UInt32, false),
            Field::new("oprcom", DataType::UInt32, false),
            Field::new("oprnegate", DataType::UInt32, false),
            Field::new("oprcode", DataType::Utf8, false),
            Field::new("oprrest", DataType::Utf8, false),
            Field::new("oprjoin", DataType::Utf8, false),
            Field::new("xmin", DataType::UInt32, false),
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
