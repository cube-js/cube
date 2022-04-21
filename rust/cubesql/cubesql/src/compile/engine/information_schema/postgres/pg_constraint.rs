use std::{any::Any, sync::Arc};

use async_trait::async_trait;

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, BooleanBuilder, Int32Builder, StringBuilder, UInt32Builder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

struct PgCatalogConstraintBuilder {
    oid: UInt32Builder,
    conname: StringBuilder,
    connamespace: UInt32Builder,
    contype: StringBuilder,
    condeferrable: BooleanBuilder,
    condeferred: BooleanBuilder,
    convalidated: BooleanBuilder,
    conrelid: UInt32Builder,
    contypid: UInt32Builder,
    conindid: UInt32Builder,
    conparentid: UInt32Builder,
    confrelid: UInt32Builder,
    confupdtype: StringBuilder,
    confdeltype: StringBuilder,
    confmatchtype: StringBuilder,
    conislocal: BooleanBuilder,
    coninhcount: Int32Builder,
    connoinherit: BooleanBuilder,
    conkey: StringBuilder,
    confkey: StringBuilder,
    conpfeqop: StringBuilder,
    conppeqop: StringBuilder,
    conffeqop: StringBuilder,
    conexclop: StringBuilder,
    conbin: StringBuilder,
}

impl PgCatalogConstraintBuilder {
    fn new() -> Self {
        let capacity = 10;

        Self {
            oid: UInt32Builder::new(capacity),
            conname: StringBuilder::new(capacity),
            connamespace: UInt32Builder::new(capacity),
            contype: StringBuilder::new(capacity),
            condeferrable: BooleanBuilder::new(capacity),
            condeferred: BooleanBuilder::new(capacity),
            convalidated: BooleanBuilder::new(capacity),
            conrelid: UInt32Builder::new(capacity),
            contypid: UInt32Builder::new(capacity),
            conindid: UInt32Builder::new(capacity),
            conparentid: UInt32Builder::new(capacity),
            confrelid: UInt32Builder::new(capacity),
            confupdtype: StringBuilder::new(capacity),
            confdeltype: StringBuilder::new(capacity),
            confmatchtype: StringBuilder::new(capacity),
            conislocal: BooleanBuilder::new(capacity),
            coninhcount: Int32Builder::new(capacity),
            connoinherit: BooleanBuilder::new(capacity),
            conkey: StringBuilder::new(capacity),
            confkey: StringBuilder::new(capacity),
            conpfeqop: StringBuilder::new(capacity),
            conppeqop: StringBuilder::new(capacity),
            conffeqop: StringBuilder::new(capacity),
            conexclop: StringBuilder::new(capacity),
            conbin: StringBuilder::new(capacity),
        }
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];

        columns.push(Arc::new(self.oid.finish()));
        columns.push(Arc::new(self.conname.finish()));
        columns.push(Arc::new(self.connamespace.finish()));
        columns.push(Arc::new(self.contype.finish()));
        columns.push(Arc::new(self.condeferrable.finish()));
        columns.push(Arc::new(self.condeferred.finish()));
        columns.push(Arc::new(self.convalidated.finish()));
        columns.push(Arc::new(self.conrelid.finish()));
        columns.push(Arc::new(self.contypid.finish()));
        columns.push(Arc::new(self.conindid.finish()));
        columns.push(Arc::new(self.conparentid.finish()));
        columns.push(Arc::new(self.confrelid.finish()));
        columns.push(Arc::new(self.confupdtype.finish()));
        columns.push(Arc::new(self.confdeltype.finish()));
        columns.push(Arc::new(self.confmatchtype.finish()));
        columns.push(Arc::new(self.conislocal.finish()));
        columns.push(Arc::new(self.coninhcount.finish()));
        columns.push(Arc::new(self.connoinherit.finish()));
        columns.push(Arc::new(self.conkey.finish()));
        columns.push(Arc::new(self.confkey.finish()));
        columns.push(Arc::new(self.conpfeqop.finish()));
        columns.push(Arc::new(self.conppeqop.finish()));
        columns.push(Arc::new(self.conffeqop.finish()));
        columns.push(Arc::new(self.conexclop.finish()));
        columns.push(Arc::new(self.conbin.finish()));

        columns
    }
}

pub struct PgCatalogConstraintProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl PgCatalogConstraintProvider {
    pub fn new() -> Self {
        let builder = PgCatalogConstraintBuilder::new();

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for PgCatalogConstraintProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::UInt32, false),
            Field::new("conname", DataType::Utf8, false),
            Field::new("connamespace", DataType::UInt32, false),
            Field::new("contype", DataType::Utf8, false),
            Field::new("condeferrable", DataType::Boolean, false),
            Field::new("condeferred", DataType::Boolean, false),
            Field::new("convalidated", DataType::Boolean, false),
            Field::new("conrelid", DataType::UInt32, false),
            Field::new("contypid", DataType::UInt32, false),
            Field::new("conindid", DataType::UInt32, false),
            Field::new("conparentid", DataType::UInt32, false),
            Field::new("confrelid", DataType::UInt32, false),
            Field::new("confupdtype", DataType::Utf8, false),
            Field::new("confdeltype", DataType::Utf8, false),
            Field::new("confmatchtype", DataType::Utf8, false),
            Field::new("conislocal", DataType::Boolean, false),
            Field::new("coninhcount", DataType::Int32, false),
            Field::new("connoinherit", DataType::Boolean, false),
            Field::new("conkey", DataType::Utf8, true),
            Field::new("confkey", DataType::Utf8, true),
            Field::new("conpfeqop", DataType::Utf8, true),
            Field::new("conppeqop", DataType::Utf8, true),
            Field::new("conffeqop", DataType::Utf8, true),
            Field::new("conexclop", DataType::Utf8, true),
            Field::new("conbin", DataType::Utf8, true),
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
