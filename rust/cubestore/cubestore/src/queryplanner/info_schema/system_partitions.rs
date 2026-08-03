use crate::metastore::partition::partition_file_name;
use crate::metastore::{IdRow, MetaStoreTable, Partition};
use crate::queryplanner::{InfoSchemaTableDef, InfoSchemaTableDefContext};
use crate::CubeError;
use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, BooleanBuilder, StringBuilder, UInt64Builder};
use datafusion::arrow::datatypes::{DataType, Field};
use std::sync::Arc;

pub struct SystemPartitionsTableDef;

#[async_trait]
impl InfoSchemaTableDef for SystemPartitionsTableDef {
    type T = IdRow<Partition>;

    async fn rows(
        &self,
        ctx: InfoSchemaTableDefContext,
        _limit: Option<usize>,
    ) -> Result<Vec<Self::T>, CubeError> {
        Ok(ctx.meta_store.partition_table().all_rows().await?)
    }

    fn schema(&self) -> Vec<Field> {
        vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("file_name", DataType::Utf8, false),
            Field::new("index_id", DataType::UInt64, false),
            Field::new("parent_partition_id", DataType::UInt64, true),
            Field::new("multi_partition_id", DataType::UInt64, true),
            Field::new("min_value", DataType::Utf8, true),
            Field::new("max_value", DataType::Utf8, true),
            Field::new("min_row", DataType::Utf8, true),
            Field::new("max_row", DataType::Utf8, true),
            Field::new("active", DataType::Boolean, true),
            Field::new("warmed_up", DataType::Boolean, true),
            Field::new("main_table_row_count", DataType::UInt64, true),
            Field::new("file_size", DataType::UInt64, true),
        ]
    }

    fn columns(&self, rows: Vec<Self::T>) -> Vec<ArrayRef> {
        let num_rows = rows.len();
        let mut id_builder = UInt64Builder::with_capacity(num_rows);
        let mut file_name_builder = StringBuilder::with_capacity(num_rows, num_rows * 64);
        let mut index_id_builder = UInt64Builder::with_capacity(num_rows);
        let mut parent_id_builder = UInt64Builder::with_capacity(num_rows);
        let mut multi_id_builder = UInt64Builder::with_capacity(num_rows);
        let mut min_val_builder = StringBuilder::with_capacity(num_rows, num_rows * 64);
        let mut max_val_builder = StringBuilder::with_capacity(num_rows, num_rows * 64);
        let mut min_row_builder = StringBuilder::with_capacity(num_rows, num_rows * 64);
        let mut max_row_builder = StringBuilder::with_capacity(num_rows, num_rows * 64);
        let mut active_builder = BooleanBuilder::with_capacity(num_rows);
        let mut warmed_builder = BooleanBuilder::with_capacity(num_rows);
        let mut row_count_builder = UInt64Builder::with_capacity(num_rows);
        let mut file_size_builder = UInt64Builder::with_capacity(num_rows);

        for row in rows.into_iter() {
            let id = row.get_id();
            let part = row.get_row();
            id_builder.append_value(id);
            file_name_builder.append_value(partition_file_name(id, part.suffix()));
            index_id_builder.append_value(part.get_index_id());
            parent_id_builder.append_option(*part.parent_partition_id());
            multi_id_builder.append_option(part.multi_partition_id());
            min_val_builder.append_option(part.get_min_val().as_ref().map(|v| format!("{:?}", v)));
            max_val_builder.append_option(part.get_max_val().as_ref().map(|v| format!("{:?}", v)));
            min_row_builder.append_option(part.get_min().as_ref().map(|v| format!("{:?}", v)));
            max_row_builder.append_option(part.get_max().as_ref().map(|v| format!("{:?}", v)));
            active_builder.append_value(part.is_active());
            warmed_builder.append_value(part.is_warmed_up());
            row_count_builder.append_value(part.main_table_row_count());
            file_size_builder.append_option(part.file_size());
        }

        vec![
            Arc::new(id_builder.finish()),
            Arc::new(file_name_builder.finish()),
            Arc::new(index_id_builder.finish()),
            Arc::new(parent_id_builder.finish()),
            Arc::new(multi_id_builder.finish()),
            Arc::new(min_val_builder.finish()),
            Arc::new(max_val_builder.finish()),
            Arc::new(min_row_builder.finish()),
            Arc::new(max_row_builder.finish()),
            Arc::new(active_builder.finish()),
            Arc::new(warmed_builder.finish()),
            Arc::new(row_count_builder.finish()),
            Arc::new(file_size_builder.finish()),
        ]
    }
}

crate::base_info_schema_table_def!(SystemPartitionsTableDef);
