use crate::metastore::{IdRow, MetaStoreTable, Schema};
use crate::queryplanner::{InfoSchemaTableDef, InfoSchemaTableDefContext};
use crate::CubeError;
use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::arrow::datatypes::{DataType, Field};
use std::sync::Arc;

pub struct SchemataInfoSchemaTableDef;

#[async_trait]
impl InfoSchemaTableDef for SchemataInfoSchemaTableDef {
    type T = IdRow<Schema>;

    async fn rows(
        &self,
        ctx: InfoSchemaTableDefContext,
        _limit: Option<usize>,
    ) -> Result<Arc<Vec<Self::T>>, CubeError> {
        Ok(Arc::new(ctx.meta_store.schemas_table().all_rows().await?))
    }

    fn schema(&self) -> Vec<Field> {
        vec![Field::new("schema_name", DataType::Utf8, false)]
    }

    fn columns(&self) -> Vec<Box<dyn Fn(Arc<Vec<Self::T>>) -> ArrayRef>> {
        vec![Box::new(|tables| {
            Arc::new(StringArray::from_iter_values(
                tables.iter().map(|row| row.get_row().get_name()),
            ))
        })]
    }
}

crate::base_info_schema_table_def!(SchemataInfoSchemaTableDef);
