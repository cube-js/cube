use crate::metastore::{IdRow, Index, MetaStore, MetaStoreTable};
use crate::queryplanner::InfoSchemaTableDef;
use crate::CubeError;
use arrow::array::{ArrayRef, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field};
use async_trait::async_trait;
use std::sync::Arc;

pub struct SystemIndexesTableDef;

#[async_trait]
impl InfoSchemaTableDef for SystemIndexesTableDef {
    type T = IdRow<Index>;

    async fn rows(&self, meta_store: Arc<dyn MetaStore>) -> Result<Arc<Vec<Self::T>>, CubeError> {
        Ok(Arc::new(meta_store.index_table().all_rows().await?))
    }

    fn columns(&self) -> Vec<(Field, Box<dyn Fn(Arc<Vec<Self::T>>) -> ArrayRef>)> {
        vec![
            (
                Field::new("id", DataType::UInt64, false),
                Box::new(|indexes| {
                    Arc::new(UInt64Array::from(
                        indexes.iter().map(|row| row.get_id()).collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("table_id", DataType::UInt64, false),
                Box::new(|indexes| {
                    Arc::new(UInt64Array::from(
                        indexes
                            .iter()
                            .map(|row| row.get_row().table_id())
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("name", DataType::Utf8, false),
                Box::new(|indexes| {
                    Arc::new(StringArray::from(
                        indexes
                            .iter()
                            .map(|row| row.get_row().get_name().as_str())
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("columns", DataType::Utf8, false),
                Box::new(|indexes| {
                    Arc::new(StringArray::from(
                        indexes
                            .iter()
                            .map(|row| format!("{:?}", row.get_row().get_columns()))
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("sort_key_size", DataType::UInt64, false),
                Box::new(|indexes| {
                    Arc::new(UInt64Array::from(
                        indexes
                            .iter()
                            .map(|row| row.get_row().sort_key_size())
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("partition_split_key_size", DataType::UInt64, true),
                Box::new(|indexes| {
                    Arc::new(UInt64Array::from(
                        indexes
                            .iter()
                            .map(|row| row.get_row().partition_split_key_size().clone())
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("multi_index_id", DataType::UInt64, true),
                Box::new(|indexes| {
                    Arc::new(UInt64Array::from(
                        indexes
                            .iter()
                            .map(|row| row.get_row().multi_index_id())
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
        ]
    }
}

crate::base_info_schema_table_def!(SystemIndexesTableDef);
