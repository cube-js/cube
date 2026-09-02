use crate::metastore::{IdRow, Index, MetaStoreTable};
use crate::queryplanner::{InfoSchemaTableDef, InfoSchemaTableDefContext};
use crate::CubeError;
use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, StringBuilder, UInt64Builder};
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
    ) -> Result<Vec<Self::T>, CubeError> {
        Ok(ctx.meta_store.index_table().all_rows().await?)
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

    fn columns(&self, rows: Vec<Self::T>) -> Vec<ArrayRef> {
        let num_rows = rows.len();
        let mut id_builder = UInt64Builder::with_capacity(num_rows);
        let mut table_id_builder = UInt64Builder::with_capacity(num_rows);
        let mut name_builder = StringBuilder::with_capacity(num_rows, num_rows * 64);
        let mut columns_builder = StringBuilder::with_capacity(num_rows, num_rows * 128);
        let mut sort_key_builder = UInt64Builder::with_capacity(num_rows);
        let mut partition_split_builder = UInt64Builder::with_capacity(num_rows);
        let mut multi_index_builder = UInt64Builder::with_capacity(num_rows);
        let mut type_builder = StringBuilder::with_capacity(num_rows, num_rows * 32);

        for row in rows.into_iter() {
            id_builder.append_value(row.get_id());
            let index = row.get_row();
            table_id_builder.append_value(index.table_id());
            name_builder.append_value(index.get_name());
            columns_builder.append_value(format!("{:?}", index.get_columns()));
            sort_key_builder.append_value(index.sort_key_size());
            partition_split_builder.append_option(*index.partition_split_key_size());
            multi_index_builder.append_option(index.multi_index_id());
            type_builder.append_value(format!("{:?}", index.get_type()));
        }

        vec![
            Arc::new(id_builder.finish()),
            Arc::new(table_id_builder.finish()),
            Arc::new(name_builder.finish()),
            Arc::new(columns_builder.finish()),
            Arc::new(sort_key_builder.finish()),
            Arc::new(partition_split_builder.finish()),
            Arc::new(multi_index_builder.finish()),
            Arc::new(type_builder.finish()),
        ]
    }
}

crate::base_info_schema_table_def!(SystemIndexesTableDef);
