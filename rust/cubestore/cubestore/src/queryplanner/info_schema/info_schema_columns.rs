use crate::metastore::table::TablePath;
use crate::metastore::Column;
use crate::queryplanner::{InfoSchemaTableDef, InfoSchemaTableDefContext};
use crate::CubeError;
use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field};
use std::sync::Arc;

pub struct ColumnsInfoSchemaTableDef;

#[async_trait]
impl InfoSchemaTableDef for ColumnsInfoSchemaTableDef {
    type T = (Column, TablePath);

    async fn rows(
        &self,
        ctx: InfoSchemaTableDefContext,
        _limit: Option<usize>,
    ) -> Result<Vec<(Column, TablePath)>, CubeError> {
        let rows = ctx.meta_store.get_tables_with_path(false).await?;
        let mut res = Vec::new();

        for row in rows.iter() {
            let columns = row.table.get_row().get_columns();
            for column in columns {
                res.push((column.clone(), row.clone()));
            }
        }

        Ok(res)
    }

    fn schema(&self) -> Vec<Field> {
        vec![
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("column_name", DataType::Utf8, false),
            Field::new("data_type", DataType::Utf8, false),
        ]
    }

    fn columns(&self, rows: Vec<(Column, TablePath)>) -> Vec<ArrayRef> {
        let num_rows = rows.len();
        let mut schema_builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
        let mut table_builder = StringBuilder::with_capacity(num_rows, num_rows * 64);
        let mut column_builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
        let mut type_builder = StringBuilder::with_capacity(num_rows, num_rows * 16);

        for (column, table_path) in rows.into_iter() {
            schema_builder.append_value(table_path.schema.get_row().get_name());
            table_builder.append_value(table_path.table.get_row().get_table_name());
            column_builder.append_value(column.get_name());
            type_builder.append_value(column.get_column_type().to_string());
        }

        vec![
            Arc::new(schema_builder.finish()),
            Arc::new(table_builder.finish()),
            Arc::new(column_builder.finish()),
            Arc::new(type_builder.finish()),
        ]
    }
}

crate::base_info_schema_table_def!(ColumnsInfoSchemaTableDef);
