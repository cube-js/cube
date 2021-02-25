pub mod limits;

use crate::config::injection::DIService;
use crate::config::ConfigObj;
use crate::import::limits::ConcurrencyLimits;
use crate::metastore::table::Table;
use crate::metastore::{is_valid_hll, IdRow};
use crate::metastore::{Column, ColumnType, ImportFormat, MetaStore};
use crate::sql::timestamp_from_string;
use crate::store::ChunkDataStore;
use crate::sys::malloc::trim_allocs;
use crate::table::data::{MutRows, Rows};
use crate::table::{Row, TableValue};
use crate::util::maybe_owned::MaybeOwnedStr;
use crate::util::ordfloat::OrdF64;
use crate::CubeError;
use async_compression::tokio::bufread::GzipDecoder;
use async_std::io::SeekFrom;
use async_std::task::{Context, Poll};
use async_trait::async_trait;
use bigdecimal::{BigDecimal, Num};
use core::mem;
use core::slice::memchr;
use futures::future::join_all;
use futures::{Stream, StreamExt};
use itertools::Itertools;
use mockall::automock;
use pin_project_lite::pin_project;
use scopeguard::defer;
use std::fs;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufRead, AsyncSeekExt, AsyncWriteExt, BufReader};
use tokio::task::JoinHandle;

impl ImportFormat {
    async fn row_stream(
        &self,
        location: String,
        columns: Vec<Column>,
        table_id: u64,
        temp_files: &mut TempFiles,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Option<Row>, CubeError>> + Send>>, CubeError> {
        let file = if location.starts_with("http") {
            let tmp_file = temp_files.new_file(table_id);
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
            File::open(location.clone()).await?
        };
        match self {
            ImportFormat::CSV => {
                let lines_stream: Pin<Box<dyn Stream<Item = Result<String, CubeError>> + Send>> =
                    if location.contains(".csv.gz") {
                        let reader = BufReader::new(GzipDecoder::new(BufReader::new(file)));
                        Box::pin(CsvLineStream::new(reader))
                    } else {
                        let reader = BufReader::new(file);
                        Box::pin(CsvLineStream::new(reader))
                    };

                let mut header_mapping = None;
                let mut mapping_insert_indices = Vec::with_capacity(columns.len());
                let rows = lines_stream.map(move |line| -> Result<Option<Row>, CubeError> {
                    let str = line?;

                    let mut parser = CsvLineParser::new(str.as_str());

                    if header_mapping.is_none() {
                        let mut mapping = Vec::new();
                        for _ in 0..columns.len() {
                            let next_column_buf = parser.next_value()?;
                            let next_column = next_column_buf.as_ref();
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
                        let value_buf = parser.next_value()?;
                        let value = value_buf.as_ref();

                        if value == "" {
                            row.insert(mapping_insert_indices[i], TableValue::Null);
                        } else {
                            row.insert(
                                mapping_insert_indices[i],
                                match column.get_column_type() {
                                    ColumnType::String => {
                                        TableValue::String(value_buf.take_string())
                                    }
                                    ColumnType::Int => value
                                        .parse()
                                        .map(|v| TableValue::Int(v))
                                        .unwrap_or(TableValue::Null),
                                    ColumnType::Decimal { .. } => {
                                        BigDecimal::from_str_radix(value, 10)
                                            .map(|d| TableValue::Decimal(d.to_string()))
                                            .unwrap_or(TableValue::Null)
                                    }
                                    ColumnType::Bytes => TableValue::Bytes(base64::decode(value)?),
                                    ColumnType::HyperLogLog(f) => {
                                        let data = base64::decode(value)?;
                                        is_valid_hll(&data, *f)?;

                                        TableValue::Bytes(data)
                                    }
                                    ColumnType::Timestamp => {
                                        TableValue::Timestamp(timestamp_from_string(value)?)
                                    }
                                    ColumnType::Float => {
                                        TableValue::Float(OrdF64(value.parse::<f64>()?))
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

    fn next_value(&mut self) -> Result<MaybeOwnedStr, CubeError> {
        Ok(
            if let Some(b'"') = self.remaining.as_bytes().iter().nth(0) {
                let mut closing_index = None;
                let mut seen_escapes = false;
                self.remaining = &self.remaining[1..];
                let mut first_quote_index = None;
                for (i, c) in self.remaining.char_indices() {
                    if c == '"' && first_quote_index.is_some() {
                        seen_escapes = true;
                        first_quote_index = None;
                    } else if c == '"' {
                        first_quote_index = Some(i);
                    } else if first_quote_index.is_some() {
                        closing_index = first_quote_index.take();
                        break;
                    }
                }
                if first_quote_index.is_some() {
                    closing_index = first_quote_index.take();
                }
                let closing_index = closing_index.ok_or(CubeError::user(format!(
                    "Malformed CSV string: {}",
                    self.line
                )))?;
                let res;
                if seen_escapes {
                    let unescaped = self.remaining[0..closing_index].replace("\"\"", "\"");
                    res = MaybeOwnedStr::Owned(unescaped)
                } else {
                    res = MaybeOwnedStr::Borrowed(&self.remaining[0..closing_index])
                }
                self.remaining = self.remaining[(closing_index + 1)..].as_ref();
                res
            } else {
                let next_comma = self
                    .remaining
                    .as_bytes()
                    .iter()
                    .position(|c| *c == b',')
                    .unwrap_or(self.remaining.len());
                let res = &self.remaining[0..next_comma];
                self.remaining = self.remaining[next_comma..].as_ref();
                MaybeOwnedStr::Borrowed(res)
            },
        )
    }

    fn advance(&mut self) -> Result<(), CubeError> {
        if let Some(b',') = self.remaining.as_bytes().iter().nth(0) {
            self.remaining = self.remaining[1..].as_ref()
        }
        Ok(())
    }
}

pin_project! {
    struct CsvLineStream<R: AsyncBufRead> {
        #[pin]
        reader: R,
        buf: Vec<u8>,
        in_quotes: bool,
    }
}

impl<R: AsyncBufRead> CsvLineStream<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            buf: Vec::new(),
            in_quotes: false,
        }
    }
}

impl<R: AsyncBufRead> Stream for CsvLineStream<R> {
    type Item = Result<String, CubeError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut projected = self.project();
        let mut reader = projected.reader;
        loop {
            let (done, used) = {
                let available = match reader.as_mut().poll_fill_buf(cx) {
                    Poll::Ready(available) => available,
                    Poll::Pending => return Poll::Pending,
                };
                match available {
                    Err(err) => {
                        return Poll::Ready(Some(Err(CubeError::from_error(err))));
                    }
                    Ok(available) => {
                        if *projected.in_quotes {
                            let quote_pos = memchr::memchr(b'"', available);
                            if let Some(i) = quote_pos {
                                // It consumes every pair of quotes.
                                // Matching for escapes is unnecessary as it's double "" sequence
                                *projected.in_quotes = false;
                                projected.buf.extend_from_slice(&available[..=i]);
                                (false, i + 1)
                            } else {
                                projected.buf.extend_from_slice(available);
                                (false, available.len())
                            }
                        } else {
                            let new_line_pos = memchr::memchr(b'\n', available);
                            let quote_pos = memchr::memchr(b'"', available);
                            let in_quotes = quote_pos.is_some()
                                && (new_line_pos.is_some() && quote_pos < new_line_pos
                                    || new_line_pos.is_none());
                            if in_quotes {
                                if let Some(i) = quote_pos {
                                    projected.buf.extend_from_slice(&available[..=i]);
                                    *projected.in_quotes = in_quotes;
                                    (false, i + 1)
                                } else {
                                    unreachable!()
                                }
                            } else if let Some(i) = new_line_pos {
                                projected.buf.extend_from_slice(&available[..=i]);
                                (true, i + 1)
                            } else {
                                projected.buf.extend_from_slice(available);
                                (false, available.len())
                            }
                        }
                    }
                }
            };

            reader.as_mut().consume(used);

            if used == 0 {
                return Poll::Ready(None);
            } else if done {
                if projected.buf.ends_with(&[b'\n']) {
                    projected.buf.pop();

                    if projected.buf.ends_with(&[b'\r']) {
                        projected.buf.pop();
                    }
                }
                let str = String::from_utf8(mem::replace(&mut projected.buf, Vec::new()));
                let res = str.map_err(|e| CubeError::from_error(e));
                return Poll::Ready(Some(res));
            }
        }
    }
}

#[automock]
#[async_trait]
pub trait ImportService: DIService + Send + Sync {
    async fn import_table(&self, table_id: u64) -> Result<(), CubeError>;
}

crate::di_service!(MockImportService, [ImportService]);

pub struct ImportServiceImpl {
    meta_store: Arc<dyn MetaStore>,
    chunk_store: Arc<dyn ChunkDataStore>,
    config_obj: Arc<dyn ConfigObj>,
    limits: Arc<ConcurrencyLimits>,
}

crate::di_service!(ImportServiceImpl, [ImportService]);

impl ImportServiceImpl {
    pub fn new(
        meta_store: Arc<dyn MetaStore>,
        chunk_store: Arc<dyn ChunkDataStore>,
        config_obj: Arc<dyn ConfigObj>,
        limits: Arc<ConcurrencyLimits>,
    ) -> Arc<ImportServiceImpl> {
        Arc::new(ImportServiceImpl {
            meta_store,
            chunk_store,
            config_obj,
            limits,
        })
    }
}

pub struct TempFiles {
    temp_path: PathBuf,
    files: Vec<PathBuf>,
}

impl TempFiles {
    pub fn new(temp_path: PathBuf) -> Self {
        Self {
            temp_path,
            files: Vec::new(),
        }
    }

    pub fn new_file(&mut self, table_id: u64) -> PathBuf {
        let path = self.temp_path.join(format!("{}.csv", table_id));
        self.files.push(path.clone());
        path
    }
}

impl Drop for TempFiles {
    fn drop(&mut self) {
        for f in self.files.iter() {
            let _ = fs::remove_file(f);
        }
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
        let locations = table
            .get_row()
            .locations()
            .ok_or(CubeError::internal(format!(
                "Trying to import table without location: {:?}",
                table
            )))?;
        let temp_dir = self.config_obj.data_dir().join("tmp");
        tokio::fs::create_dir_all(temp_dir.clone()).await?;
        for location in locations.into_iter() {
            let mut temp_files = TempFiles::new(temp_dir.clone());
            let mut row_stream = format
                .row_stream(
                    location.to_string(),
                    table.get_row().get_columns().clone(),
                    table_id,
                    &mut temp_files,
                )
                .await?;

            defer!(trim_allocs());

            let mut ingestion = Ingestion::new(
                self.meta_store.clone(),
                self.chunk_store.clone(),
                self.limits.clone(),
                table.clone(),
            );
            let mut rows = MutRows::new(table.get_row().get_columns().len());
            while let Some(row) = row_stream.next().await {
                if let Some(row) = row? {
                    rows.add_row_heap_allocated(&row);
                    if rows.num_rows() >= self.config_obj.wal_split_threshold() as usize {
                        let mut to_add = MutRows::new(table.get_row().get_columns().len());
                        mem::swap(&mut rows, &mut to_add);
                        ingestion.queue_data_frame(to_add.freeze()).await?;
                    }
                }
            }

            mem::drop(temp_files);

            ingestion.queue_data_frame(rows.freeze()).await?;
            ingestion.wait_completion().await?;
        }

        Ok(())
    }
}

/// Handles row-based data ingestion, e.g. on CSV import and SQL insert.
pub struct Ingestion {
    meta_store: Arc<dyn MetaStore>,
    chunk_store: Arc<dyn ChunkDataStore>,
    limits: Arc<ConcurrencyLimits>,
    table: IdRow<Table>,

    partition_jobs: Vec<JoinHandle<Result<(), CubeError>>>,
}

impl Ingestion {
    pub fn new(
        meta_store: Arc<dyn MetaStore>,
        chunk_store: Arc<dyn ChunkDataStore>,
        limits: Arc<ConcurrencyLimits>,
        table: IdRow<Table>,
    ) -> Ingestion {
        Ingestion {
            meta_store,
            chunk_store,
            limits,
            table,
            partition_jobs: Vec::new(),
        }
    }

    pub async fn queue_data_frame(&mut self, rows: Rows) -> Result<(), CubeError> {
        let active_data_frame = self.limits.acquire_data_frame().await?;

        let meta_store = self.meta_store.clone();
        let chunk_store = self.chunk_store.clone();
        let columns = self.table.get_row().get_columns().clone().clone();
        let table_id = self.table.get_id();
        self.partition_jobs.push(tokio::spawn(async move {
            let new_chunks = chunk_store.partition_data(table_id, rows, &columns).await?;
            std::mem::drop(active_data_frame);

            // More data frame processing can proceed now as we dropped `active_data_frame`.
            // Time to wait to chunks to upload and activate them.
            let new_chunk_ids: Result<Vec<u64>, CubeError> = join_all(new_chunks)
                .await
                .into_iter()
                .map(|c| Ok(c??.get_id()))
                .collect();
            meta_store.activate_chunks(table_id, new_chunk_ids?).await
        }));

        Ok(())
    }

    pub async fn wait_completion(self) -> Result<(), CubeError> {
        for j in self.partition_jobs {
            j.await??;
        }

        Ok(())
    }
}
