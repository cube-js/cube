use crate::metastore::partition::partition_file_name;
use crate::metastore::{IdRow, MetaStore, MetaStoreTable, Partition};
use crate::queryplanner::InfoSchemaTableDef;
use crate::CubeError;
use arrow::array::{ArrayRef, BooleanArray, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field};
use async_trait::async_trait;
use std::sync::Arc;

pub struct SystemPartitionsTableDef;

#[async_trait]
impl InfoSchemaTableDef for SystemPartitionsTableDef {
    type T = IdRow<Partition>;

    async fn rows(&self, meta_store: Arc<dyn MetaStore>) -> Result<Arc<Vec<Self::T>>, CubeError> {
        Ok(Arc::new(meta_store.partition_table().all_rows().await?))
    }

    fn columns(&self) -> Vec<(Field, Box<dyn Fn(Arc<Vec<Self::T>>) -> ArrayRef>)> {
        vec![
            (
                Field::new("id", DataType::UInt64, false),
                Box::new(|partitions| {
                    Arc::new(UInt64Array::from(
                        partitions
                            .iter()
                            .map(|row| row.get_id())
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("file_name", DataType::Utf8, false),
                Box::new(|partitions| {
                    Arc::new(StringArray::from(
                        partitions
                            .iter()
                            .map(|row| partition_file_name(row.get_id(), row.get_row().suffix()))
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("index_id", DataType::UInt64, false),
                Box::new(|partitions| {
                    Arc::new(UInt64Array::from(
                        partitions
                            .iter()
                            .map(|row| row.get_row().get_index_id())
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("parent_partition_id", DataType::UInt64, true),
                Box::new(|partitions| {
                    Arc::new(UInt64Array::from(
                        partitions
                            .iter()
                            .map(|row| row.get_row().parent_partition_id().clone())
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("multi_partition_id", DataType::UInt64, true),
                Box::new(|partitions| {
                    Arc::new(UInt64Array::from(
                        partitions
                            .iter()
                            .map(|row| row.get_row().multi_partition_id().clone())
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("min_value", DataType::Utf8, true),
                Box::new(|partitions| {
                    let min_array = partitions
                        .iter()
                        .map(|row| {
                            row.get_row()
                                .get_min_val()
                                .as_ref()
                                .map(|x| format!("{:?}", x))
                        })
                        .collect::<Vec<_>>();
                    Arc::new(StringArray::from(
                        min_array
                            .iter()
                            .map(|v| v.as_ref().map(|v| v.as_str()))
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("max_value", DataType::Utf8, true),
                Box::new(|partitions| {
                    let max_array = partitions
                        .iter()
                        .map(|row| {
                            row.get_row()
                                .get_max_val()
                                .as_ref()
                                .map(|x| format!("{:?}", x))
                        })
                        .collect::<Vec<_>>();
                    Arc::new(StringArray::from(
                        max_array
                            .iter()
                            .map(|v| v.as_ref().map(|v| v.as_str()))
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("min_row", DataType::Utf8, true),
                Box::new(|partitions| {
                    let min_array = partitions
                        .iter()
                        .map(|row| {
                            row.get_row()
                                .get_min()
                                .as_ref()
                                .map(|x| format!("{:?}", x))
                        })
                        .collect::<Vec<_>>();
                    Arc::new(StringArray::from(
                        min_array
                            .iter()
                            .map(|v| v.as_ref().map(|v| v.as_str()))
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("max_row", DataType::Utf8, true),
                Box::new(|partitions| {
                    let max_array = partitions
                        .iter()
                        .map(|row| {
                            row.get_row()
                                .get_max()
                                .as_ref()
                                .map(|x| format!("{:?}", x))
                        })
                        .collect::<Vec<_>>();
                    Arc::new(StringArray::from(
                        max_array
                            .iter()
                            .map(|v| v.as_ref().map(|v| v.as_str()))
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("active", DataType::Boolean, true),
                Box::new(|partitions| {
                    Arc::new(BooleanArray::from(
                        partitions
                            .iter()
                            .map(|row| row.get_row().is_active())
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("warmed_up", DataType::Boolean, true),
                Box::new(|partitions| {
                    Arc::new(BooleanArray::from(
                        partitions
                            .iter()
                            .map(|row| row.get_row().is_warmed_up())
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("main_table_row_count", DataType::UInt64, true),
                Box::new(|partitions| {
                    Arc::new(UInt64Array::from(
                        partitions
                            .iter()
                            .map(|row| row.get_row().main_table_row_count())
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("file_size", DataType::UInt64, true),
                Box::new(|partitions| {
                    Arc::new(UInt64Array::from(
                        partitions
                            .iter()
                            .map(|row| row.get_row().file_size())
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
        ]
    }
}

crate::base_info_schema_table_def!(SystemPartitionsTableDef);
