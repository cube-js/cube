use crate::metastore::{Column, ColumnType, ImportFormat, MetaStore};
use crate::sql::timestamp_from_string;
use crate::store::{DataFrame, WALDataStore};
use crate::table::{Row, TableValue};
use crate::CubeError;
use async_std::io::SeekFrom;
use async_trait::async_trait;
use bigdecimal::{BigDecimal, Num};
use core::mem;
use futures::{Stream, StreamExt};
use itertools::Itertools;
use mockall::automock;
use std::env;
use std::pin::Pin;
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};
use tokio_stream::wrappers::LinesStream;

impl ImportFormat {
    async fn row_stream(
        &self,
        location: String,
        columns: Vec<Column>,
        table_id: u64,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Option<Row>, CubeError>> + Send>>, CubeError> {
        let file = if location.starts_with("http") {
            let tmp_file = env::temp_dir().join(format!("{}", table_id));
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(tmp_file)
                .await?;
            let mut stream = reqwest::get(&location).await?.bytes_stream();
            while let Some(bytes) = stream.next().await {
                file.write_all(bytes?.as_ref()).await?;
            }
            file.seek(SeekFrom::Start(0)).await?;
            file
        } else {
            File::open(location).await?
        };
        match self {
            ImportFormat::CSV => {
                let lines = BufReader::new(file).lines();
                let mut header_mapping = None;
                let mut mapping_insert_indices = Vec::with_capacity(columns.len());
                let rows =
                    LinesStream::new(lines).map(move |line| -> Result<Option<Row>, CubeError> {
                        let str = line?;

                        let mut parser = CsvLineParser::new(str.as_str());

                        if header_mapping.is_none() {
                            let mut mapping = Vec::new();
                            for _ in 0..columns.len() {
                                let next_column = parser.next_value()?;
                                let (i, to_insert) = columns
                                    .iter()
                                    .find_position(|c| c.get_name() == &next_column)
                                    .map(|(i, c)| (i, c.clone()))
                                    .ok_or(CubeError::user(format!(
                                        "Column '{}' is not found during import in {:?}",
                                        next_column, columns
                                    )))?;
                                // This is tricky indices structure: it remembers indices of inserts
                                // with regards to moving element indices due to these inserts.
                                // It saves some column resorting trips.
                                let insert_pos = mapping
                                    .iter()
                                    .find_position(|(col_index, _)| *col_index > i)
                                    .map(|(insert_pos, _)| insert_pos)
                                    .unwrap_or_else(|| mapping.len());
                                mapping_insert_indices.push(insert_pos);
                                mapping.push((i, to_insert));
                                parser.advance()?;
                            }
                            header_mapping = Some(mapping);
                            return Ok(None);
                        }

                        let resolved_mapping = header_mapping.as_ref().ok_or(CubeError::user(
                            "Header is required for CSV import".to_string(),
                        ))?;

                        let mut row = Vec::with_capacity(columns.len());

                        for (i, (_, column)) in resolved_mapping.iter().enumerate() {
                            let value = parser.next_value()?;

                            if &value == "" {
                                row.insert(mapping_insert_indices[i], TableValue::Null);
                            } else {
                                row.insert(
                                    mapping_insert_indices[i],
                                    match column.get_column_type() {
                                        ColumnType::String => TableValue::String(value),
                                        ColumnType::Int => value
                                            .parse()
                                            .map(|v| TableValue::Int(v))
                                            .unwrap_or(TableValue::Null),
                                        ColumnType::Decimal { .. } => {
                                            BigDecimal::from_str_radix(value.as_str(), 10)
                                                .map(|d| TableValue::Decimal(d.to_string()))
                                                .unwrap_or(TableValue::Null)
                                        }
                                        ColumnType::Bytes => unimplemented!(),
                                        ColumnType::HyperLogLog(_) => unimplemented!(),
                                        ColumnType::Timestamp => {
                                            timestamp_from_string(value.as_str())?
                                        }
                                        ColumnType::Float => {
                                            TableValue::Float(value.parse::<f64>()?.to_string())
                                        }
                                        ColumnType::Boolean => {
                                            TableValue::Boolean(value.to_lowercase() == "true")
                                        }
                                    },
                                );
                            }

                            parser.advance()?;
                        }
                        Ok(Some(Row::new(row)))
                    });
                Ok(rows.boxed())
            }
        }
    }
}

struct CsvLineParser<'a> {
    line: &'a str,
    remaining: &'a str,
}

impl<'a> CsvLineParser<'a> {
    fn new(line: &'a str) -> Self {
        Self {
            line,
            remaining: line,
        }
    }

    fn next_value(&mut self) -> Result<String, CubeError> {
        Ok(if self.remaining.chars().nth(0) == Some('"') {
            let mut closing_index = None;
            let mut i = 1;
            while i < self.remaining.len() {
                if i < self.remaining.len() - 1 && &self.remaining[i..(i + 2)] == "\"\"" {
                    i += 1;
                } else if &self.remaining[i..(i + 1)] == "\"" {
                    closing_index = Some(i);
                    break;
                }
                i += 1;
            }
            let closing_index = closing_index.ok_or(CubeError::user(format!(
                "Malformed CSV string: {}",
                self.line
            )))?;
            let res: String = self.remaining[1..closing_index].replace("\"\"", "\"");
            self.remaining = self.remaining[(closing_index + 1)..].as_ref();
            res
        } else {
            let next_comma = self.remaining.find(",").unwrap_or(self.remaining.len());
            let res: String = self.remaining[0..next_comma].to_string();
            self.remaining = self.remaining[next_comma..].as_ref();
            res
        })
    }

    fn advance(&mut self) -> Result<(), CubeError> {
        if self.remaining.chars().nth(0) == Some(',') {
            self.remaining = self.remaining[1..].as_ref()
        }
        Ok(())
    }
}

#[automock]
#[async_trait]
pub trait ImportService: Send + Sync {
    async fn import_table(&self, table_id: u64) -> Result<(), CubeError>;
}

pub struct ImportServiceImpl {
    meta_store: Arc<dyn MetaStore>,
    wal_store: Arc<dyn WALDataStore>,
}

impl ImportServiceImpl {
    pub fn new(
        meta_store: Arc<dyn MetaStore>,
        wal_store: Arc<dyn WALDataStore>,
    ) -> Arc<ImportServiceImpl> {
        Arc::new(ImportServiceImpl {
            meta_store,
            wal_store,
        })
    }
}

#[async_trait]
impl ImportService for ImportServiceImpl {
    async fn import_table(&self, table_id: u64) -> Result<(), CubeError> {
        let table = self.meta_store.get_table_by_id(table_id).await?;
        let format = table
            .get_row()
            .import_format()
            .as_ref()
            .ok_or(CubeError::internal(format!(
                "Trying to import table without import format: {:?}",
                table
            )))?;
        let location = table
            .get_row()
            .location()
            .as_ref()
            .ok_or(CubeError::internal(format!(
                "Trying to import table without location: {:?}",
                table
            )))?;
        let mut row_stream = format
            .row_stream(
                location.to_string(),
                table.get_row().get_columns().clone(),
                table_id,
            )
            .await?;
        let mut rows = Vec::new();
        while let Some(row) = row_stream.next().await {
            if let Some(row) = row? {
                rows.push(row);
                if rows.len() >= 500000 {
                    let mut to_add = Vec::new();
                    mem::swap(&mut rows, &mut to_add);
                    self.wal_store
                        .add_wal(
                            table.clone(),
                            DataFrame::new(table.get_row().get_columns().clone(), to_add),
                        )
                        .await?;
                }
            }
        }

        self.wal_store
            .add_wal(
                table.clone(),
                DataFrame::new(table.get_row().get_columns().clone(), rows),
            )
            .await?;

        Ok(())
    }
}
