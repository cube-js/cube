use crate::metastore::{IdRow, MetaStore, MetaStoreTable, Schema};
use crate::queryplanner::InfoSchemaTableDef;
use crate::CubeError;
use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field};
use async_trait::async_trait;
use std::sync::Arc;

pub struct SchemataInfoSchemaTableDef;

#[async_trait]
impl InfoSchemaTableDef for SchemataInfoSchemaTableDef {
    type T = IdRow<Schema>;

    async fn rows(&self, meta_store: Arc<dyn MetaStore>) -> Result<Arc<Vec<Self::T>>, CubeError> {
        Ok(Arc::new(meta_store.schemas_table().all_rows().await?))
    }

    fn columns(&self) -> Vec<(Field, Box<dyn Fn(Arc<Vec<Self::T>>) -> ArrayRef>)> {
        vec![(
            Field::new("schema_name", DataType::Utf8, false),
            Box::new(|tables| {
                Arc::new(StringArray::from(
                    tables
                        .iter()
                        .map(|row| row.get_row().get_name().as_str())
                        .collect::<Vec<_>>(),
                ))
            }),
        )]
    }
}

crate::base_info_schema_table_def!(SchemataInfoSchemaTableDef);
