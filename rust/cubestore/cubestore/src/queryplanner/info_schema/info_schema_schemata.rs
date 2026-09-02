use crate::metastore::{IdRow, MetaStoreTable, Schema};
use crate::queryplanner::{InfoSchemaTableDef, InfoSchemaTableDefContext};
use crate::CubeError;
use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, StringBuilder};
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
    ) -> Result<Vec<Self::T>, CubeError> {
        Ok(ctx.meta_store.schemas_table().all_rows().await?)
    }

    fn schema(&self) -> Vec<Field> {
        vec![Field::new("schema_name", DataType::Utf8, false)]
    }

    fn columns(&self, rows: Vec<Self::T>) -> Vec<ArrayRef> {
        let num_rows = rows.len();
        let mut name_builder = StringBuilder::with_capacity(num_rows, num_rows * 32);

        for row in rows.into_iter() {
            name_builder.append_value(row.get_row().get_name());
        }

        vec![Arc::new(name_builder.finish())]
    }
}

crate::base_info_schema_table_def!(SchemataInfoSchemaTableDef);
