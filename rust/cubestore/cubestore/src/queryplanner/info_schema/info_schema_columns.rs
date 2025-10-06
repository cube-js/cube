use crate::metastore::table::TablePath;
use crate::metastore::Column;
use crate::queryplanner::{InfoSchemaTableDef, InfoSchemaTableDefContext};
use crate::CubeError;
use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, StringArray};
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
    ) -> Result<Arc<Vec<(Column, TablePath)>>, CubeError> {
        let rows = ctx.meta_store.get_tables_with_path(false).await?;
        let mut res = Vec::new();

        for row in rows.iter() {
            let columns = row.table.get_row().get_columns();
            for column in columns {
                res.push((column.clone(), row.clone()));
            }
        }

        Ok(Arc::new(res))
    }

    fn schema(&self) -> Vec<Field> {
        vec![
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("column_name", DataType::Utf8, false),
            Field::new("data_type", DataType::Utf8, false),
        ]
    }

    fn columns(&self) -> Vec<Box<dyn Fn(Arc<Vec<(Column, TablePath)>>) -> ArrayRef>> {
        vec![
            Box::new(|tables| {
                Arc::new(StringArray::from_iter_values(
                    tables
                        .iter()
                        .map(|(_, row)| row.schema.get_row().get_name()),
                ))
            }),
            Box::new(|tables| {
                Arc::new(StringArray::from_iter_values(
                    tables
                        .iter()
                        .map(|(_, row)| row.table.get_row().get_table_name()),
                ))
            }),
            Box::new(|tables| {
                Arc::new(StringArray::from_iter_values(
                    tables.iter().map(|(column, _)| column.get_name()),
                ))
            }),
            Box::new(|tables| {
                Arc::new(StringArray::from_iter_values(
                    tables
                        .iter()
                        .map(|(column, _)| column.get_column_type().to_string()),
                ))
            }),
        ]
    }
}

crate::base_info_schema_table_def!(ColumnsInfoSchemaTableDef);
