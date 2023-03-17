use crate::metastore::table::TablePath;
use crate::metastore::Column;
use crate::queryplanner::{InfoSchemaTableDef, InfoSchemaTableDefContext};
use crate::CubeError;
use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field};
use async_trait::async_trait;
use std::sync::Arc;

pub struct ColumnsInfoSchemaTableDef;

#[async_trait]
impl InfoSchemaTableDef for ColumnsInfoSchemaTableDef {
    type T = (Column, TablePath);

    async fn rows(
        &self,
        ctx: InfoSchemaTableDefContext,
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

    fn columns(
        &self,
    ) -> Vec<(
        Field,
        Box<dyn Fn(Arc<Vec<(Column, TablePath)>>) -> ArrayRef>,
    )> {
        vec![
            (
                Field::new("table_schema", DataType::Utf8, false),
                Box::new(|tables| {
                    Arc::new(StringArray::from(
                        tables
                            .iter()
                            .map(|(_, row)| row.schema.get_row().get_name().as_str())
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("table_name", DataType::Utf8, false),
                Box::new(|tables| {
                    Arc::new(StringArray::from(
                        tables
                            .iter()
                            .map(|(_, row)| row.table.get_row().get_table_name().as_str())
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("column_name", DataType::Utf8, false),
                Box::new(|tables| {
                    Arc::new(StringArray::from(
                        tables
                            .iter()
                            .map(|(column, _)| column.get_name().as_str())
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
            (
                Field::new("data_type", DataType::Utf8, false),
                Box::new(|tables| {
                    Arc::new(StringArray::from(
                        tables
                            .iter()
                            .map(|(column, _)| column.get_column_type().to_string())
                            .collect::<Vec<_>>(),
                    ))
                }),
            ),
        ]
    }
}

crate::base_info_schema_table_def!(ColumnsInfoSchemaTableDef);
