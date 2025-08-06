use crate::metastore::{IdRow, Index, MetaStoreTable};
use crate::queryplanner::{InfoSchemaTableDef, InfoSchemaTableDefContext};
use crate::CubeError;
use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, StringArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field};
use std::sync::Arc;

pub struct SystemIndexesTableDef;

#[async_trait]
impl InfoSchemaTableDef for SystemIndexesTableDef {
    type T = IdRow<Index>;

    async fn rows(
        &self,
        ctx: InfoSchemaTableDefContext,
        _limit: Option<usize>,
    ) -> Result<Arc<Vec<Self::T>>, CubeError> {
        Ok(Arc::new(ctx.meta_store.index_table().all_rows().await?))
    }

    fn schema(&self) -> Vec<Field> {
        vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("table_id", DataType::UInt64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("columns", DataType::Utf8, false),
            Field::new("sort_key_size", DataType::UInt64, false),
            Field::new("partition_split_key_size", DataType::UInt64, true),
            Field::new("multi_index_id", DataType::UInt64, true),
            Field::new("index_type", DataType::Utf8, false),
        ]
    }

    fn columns(&self) -> Vec<Box<dyn Fn(Arc<Vec<Self::T>>) -> ArrayRef>> {
        vec![
            Box::new(|indexes| {
                Arc::new(UInt64Array::from_iter_values(
                    indexes.iter().map(|row| row.get_id()),
                ))
            }),
            Box::new(|indexes| {
                Arc::new(UInt64Array::from_iter_values(
                    indexes.iter().map(|row| row.get_row().table_id()),
                ))
            }),
            Box::new(|indexes| {
                Arc::new(StringArray::from_iter_values(
                    indexes.iter().map(|row| row.get_row().get_name()),
                ))
            }),
            Box::new(|indexes| {
                Arc::new(StringArray::from_iter_values(
                    indexes
                        .iter()
                        .map(|row| format!("{:?}", row.get_row().get_columns())),
                ))
            }),
            Box::new(|indexes| {
                Arc::new(UInt64Array::from_iter_values(
                    indexes.iter().map(|row| row.get_row().sort_key_size()),
                ))
            }),
            Box::new(|indexes| {
                Arc::new(UInt64Array::from_iter(
                    indexes
                        .iter()
                        .map(|row| row.get_row().partition_split_key_size().clone()),
                ))
            }),
            Box::new(|indexes| {
                Arc::new(UInt64Array::from_iter(
                    indexes.iter().map(|row| row.get_row().multi_index_id()),
                ))
            }),
            Box::new(|indexes| {
                Arc::new(StringArray::from_iter_values(
                    indexes
                        .iter()
                        .map(|row| format!("{:?}", row.get_row().get_type())),
                ))
            }),
        ]
    }
}

crate::base_info_schema_table_def!(SystemIndexesTableDef);
