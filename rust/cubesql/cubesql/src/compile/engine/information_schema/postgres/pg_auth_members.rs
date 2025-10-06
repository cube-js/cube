use std::{any::Any, sync::Arc};

use async_trait::async_trait;

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, BooleanBuilder, UInt32Builder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

struct PgCatalogAuthMembersBuilder {
    roleid: UInt32Builder,
    member: UInt32Builder,
    grantor: UInt32Builder,
    admin_option: BooleanBuilder,
}

impl PgCatalogAuthMembersBuilder {
    fn new() -> Self {
        let capacity = 1;

        Self {
            roleid: UInt32Builder::new(capacity),
            member: UInt32Builder::new(capacity),
            grantor: UInt32Builder::new(capacity),
            admin_option: BooleanBuilder::new(capacity),
        }
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(self.roleid.finish()),
            Arc::new(self.member.finish()),
            Arc::new(self.grantor.finish()),
            Arc::new(self.admin_option.finish()),
        ];

        columns
    }
}

pub struct PgCatalogAuthMembersProvider {
    data: Arc<Vec<ArrayRef>>,
}

// https://www.postgresql.org/docs/14/catalog-pg-auth-members.html
impl PgCatalogAuthMembersProvider {
    pub fn new() -> Self {
        let builder = PgCatalogAuthMembersBuilder::new();

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for PgCatalogAuthMembersProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("roleid", DataType::UInt32, false),
            Field::new("member", DataType::UInt32, false),
            Field::new("grantor", DataType::UInt32, false),
            Field::new("admin_option", DataType::Boolean, false),
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
