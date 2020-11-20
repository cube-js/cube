use async_trait::async_trait;
use crate::CubeError;
use crate::metastore::{MetaStore, Column, ColumnType, ImportFormat};
use std::sync::Arc;
use crate::store::{WALDataStore, DataFrame};
use crate::table::{Row, TableValue};
use tokio::fs::File;
use tokio::io::{BufReader, AsyncBufReadExt};
use tokio::stream::Stream;
use futures::StreamExt;
use core::mem;
use std::pin::Pin;
use mockall::automock;

impl ImportFormat {
    async fn row_stream(&self, location: String, columns: Vec<Column>) -> Result<Pin<Box<dyn Stream<Item = Result<Row, CubeError>> + Send>>, CubeError> {
        match self {
            ImportFormat::CSV => {
                let file = File::open(location).await?;
                let lines = BufReader::new(file).lines();
                let rows = lines.map(move |line| -> Result<Row, CubeError> {
                    let str = line?;
                    let mut remaining: &str = str.as_str();
                    let mut row = Vec::with_capacity(columns.len());
                    for column in columns.iter() {
                        let value = if remaining.chars().nth(0) == Some('"') {
                            let closing_index = remaining.find("\"")
                                .ok_or(CubeError::user(format!("Malformed CSV string: {}", str)))?;
                            let res: &str = remaining[1..closing_index].as_ref();
                            remaining = remaining[closing_index..].as_ref();
                            res
                        } else {
                            let next_comma = remaining.find(",").unwrap_or(remaining.len());
                            let res: &str = remaining[0..next_comma].as_ref();
                            remaining = remaining[next_comma..].as_ref();
                            res
                        };

                        row.push(match column.get_column_type() {
                            ColumnType::String => TableValue::String(value.to_string()),
                            ColumnType::Int => TableValue::Int(value.parse()?),
                            x => panic!("CSV import for {:?} is not implemented", x)
                        });

                        if remaining.chars().nth(0) == Some(',') {
                            remaining = remaining[1..].as_ref()
                        }
                    }
                    Ok(Row::new(row))
                });
                Ok(rows.boxed())
            }
        }
    }
}

#[automock]
#[async_trait]
pub trait ImportService: Send + Sync {
    async fn import_table(&self, table_id: u64) -> Result<(), CubeError>;
}

pub struct ImportServiceImpl {
    meta_store: Arc<dyn MetaStore>,
    wal_store: Arc<dyn WALDataStore>
}

impl ImportServiceImpl {
    pub fn new(meta_store: Arc<dyn MetaStore>,
               wal_store: Arc<dyn WALDataStore>) -> Arc<ImportServiceImpl> {
        Arc::new(ImportServiceImpl {
            meta_store, wal_store
        })
    }
}

#[async_trait]
impl ImportService for ImportServiceImpl {
    async fn import_table(&self, table_id: u64) -> Result<(), CubeError> {
        let table = self.meta_store.get_table_by_id(table_id).await?;
        let format = table.get_row().import_format().as_ref().ok_or(CubeError::internal(format!("Trying to import table without import format: {:?}", table)))?;
        let location = table.get_row().location().as_ref().ok_or(CubeError::internal(format!("Trying to import table without location: {:?}", table)))?;
        let mut row_stream = format.row_stream(location.to_string(), table.get_row().get_columns().clone()).await?;
        let mut rows = Vec::new();
        while let Some(row) = row_stream.next().await {
            rows.push(row?);
            if rows.len() >= 500000 {
                let mut to_add = Vec::new();
                mem::swap(&mut rows, &mut to_add);
                self.wal_store.add_wal(
                    table.clone(), DataFrame::new(table.get_row().get_columns().clone(), to_add)
                ).await?;
            }
        }

        self.wal_store.add_wal(
            table.clone(), DataFrame::new(table.get_row().get_columns().clone(), rows)
        ).await?;

        Ok(())
    }
}

