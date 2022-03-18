use core::mem;
use core::slice::memchr;
use std::convert::TryFrom;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;

use arrow::array::{ArrayBuilder, ArrayRef};
use async_compression::tokio::bufread::GzipDecoder;
use async_std::io::SeekFrom;
use async_std::task::{Context, Poll};
use async_trait::async_trait;
use bigdecimal::{BigDecimal, Num};
use datafusion::cube_ext;
use futures::future::join_all;
use futures::{Stream, StreamExt};
use itertools::Itertools;
use mockall::automock;
use num::ToPrimitive;
use pin_project_lite::pin_project;
use tempfile::TempPath;
use tokio::fs::File;
use tokio::io::{AsyncBufRead, AsyncSeekExt, AsyncWriteExt, BufReader};
use tokio::task::JoinHandle;

use cubehll::HllSketch;

use crate::config::injection::DIService;
use crate::config::ConfigObj;
use crate::import::limits::ConcurrencyLimits;
use crate::metastore::table::Table;
use crate::metastore::{is_valid_plain_binary_hll, HllFlavour, IdRow};
use crate::metastore::{Column, ColumnType, ImportFormat, MetaStore};
use crate::remotefs::RemoteFs;
use crate::sql::timestamp_from_string;
use crate::store::ChunkDataStore;
use crate::streaming::StreamingService;
use crate::table::data::{append_row, create_array_builders};
use crate::table::{Row, TableValue};
use crate::util::decimal::Decimal;
use crate::util::maybe_owned::MaybeOwnedStr;
use crate::CubeError;
use datafusion::cube_ext::ordfloat::OrdF64;

pub mod limits;

impl ImportFormat {
    async fn row_stream(
        &self,
        file: File,
        location: String,
        columns: Vec<Column>,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Option<Row>, CubeError>> + Send>>, CubeError> {
        match self {
            ImportFormat::CSV | ImportFormat::CSVNoHeader => {
                let lines_stream: Pin<Box<dyn Stream<Item = Result<String, CubeError>> + Send>> =
                    if location.contains(".gz") {
                        let reader = BufReader::new(GzipDecoder::new(BufReader::new(file)));
                        Box::pin(CsvLineStream::new(reader))
                    } else {
                        let reader = BufReader::new(file);
                        Box::pin(CsvLineStream::new(reader))
                    };

                let mut header_mapping = match self {
                    ImportFormat::CSV => None,
                    ImportFormat::CSVNoHeader => Some(
                        columns
                            .iter()
                            .enumerate()
                            .map(|(i, c)| (i, c.clone()))
                            .collect(),
                    ),
                };

                let rows = lines_stream.map(move |line| -> Result<Option<Row>, CubeError> {
                    let str = line?;

                    let mut parser = CsvLineParser::new(str.as_str());

                    if header_mapping.is_none() {
                        let mut mapping = Vec::new();
                        for _ in 0..columns.len() {
                            let next_column_buf = parser.next_value()?;
                            let next_column = next_column_buf.as_ref();
                            let (insert_pos, to_insert) = columns
                                .iter()
                                .find_position(|c| c.get_name() == &next_column)
                                .map(|(i, c)| (i, c.clone()))
                                .ok_or(CubeError::user(format!(
                                    "Column '{}' is not found during import in {:?}",
                                    next_column, columns
                                )))?;
                            mapping.push((insert_pos, to_insert));
                            parser.advance()?;
                        }
                        header_mapping = Some(mapping);
                        return Ok(None);
                    }

                    let resolved_mapping = header_mapping.as_ref().ok_or(CubeError::user(
                        "Header is required for CSV import".to_string(),
                    ))?;

                    let mut row = vec![TableValue::Null; columns.len()];

                    for (insert_pos, column) in resolved_mapping.iter() {
                        let value_buf = parser.next_value()?;
                        let value = value_buf.as_ref();

                        if value == "" || value == "\\N" {
                            row[*insert_pos] = TableValue::Null;
                        } else {
                            let mut value_buf_opt = Some(value_buf);
                            row[*insert_pos] =
                                ImportFormat::parse_column_value(column, &mut value_buf_opt)
                                    .map_err(|e| {
                                        if let Some(value_buf) = value_buf_opt {
                                            CubeError::user(format!(
                                                "Can't parse '{}' column value for '{}' column: {}",
                                                value_buf.as_ref(),
                                                column.get_name(),
                                                e
                                            ))
                                        } else {
                                            CubeError::user(format!(
                                                "Can't parse column value for '{}' column: {}",
                                                column.get_name(),
                                                e
                                            ))
                                        }
                                    })?;
                        }

                        parser.advance()?;
                    }
                    Ok(Some(Row::new(row)))
                });
                Ok(rows.boxed())
            }
        }
    }

    fn parse_column_value(
        column: &Column,
        value_buf: &mut Option<MaybeOwnedStr>,
    ) -> Result<TableValue, CubeError> {
        let value = value_buf.as_ref().unwrap().as_ref();
        Ok(match column.get_column_type() {
            ColumnType::String => TableValue::String(value_buf.take().unwrap().take_string()),
            ColumnType::Int => value
                .parse()
                .map(|v| TableValue::Int(v))
                .unwrap_or(TableValue::Null),
            t @ ColumnType::Decimal { .. } => TableValue::Decimal(parse_decimal(
                value,
                u8::try_from(t.target_scale()).unwrap(),
            )?),
            ColumnType::Bytes => TableValue::Bytes(base64::decode(value)?),
            ColumnType::HyperLogLog(HllFlavour::Snowflake) => {
                let hll = HllSketch::read_snowflake(value)?;
                TableValue::Bytes(hll.write())
            }
            ColumnType::HyperLogLog(HllFlavour::Postgres) => {
                let data = base64::decode(value)?;
                let hll = HllSketch::read_hll_storage_spec(&data)?;
                TableValue::Bytes(hll.write())
            }
            ColumnType::HyperLogLog(f @ (HllFlavour::Airlift | HllFlavour::ZetaSketch)) => {
                let data = base64::decode(value)?;
                is_valid_plain_binary_hll(&data, *f)?;
                TableValue::Bytes(data)
            }
            ColumnType::Timestamp => TableValue::Timestamp(timestamp_from_string(value)?),
            ColumnType::Float => TableValue::Float(OrdF64(value.parse::<f64>()?)),
            ColumnType::Boolean => TableValue::Boolean(value.to_lowercase() == "true"),
        })
    }
}

pub(crate) fn parse_decimal(value: &str, scale: u8) -> Result<Decimal, CubeError> {
    // TODO: parse into Decimal directly.
    let bd = BigDecimal::from_str_radix(value, 10)?;
    let raw_value = match bd
        .with_scale(scale as i64)
        .into_bigint_and_exponent()
        .0
        .to_i64()
    {
        Some(d) => d,
        None => {
            return Err(CubeError::user(format!(
                "cannot represent '{}' with scale {} without loosing precision",
                value, scale
            )))
        }
    };
    Ok(Decimal::new(raw_value))
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
    async fn import_table_part(&self, table_id: u64, location: &str) -> Result<(), CubeError>;
    async fn estimate_location_row_count(&self, location: &str) -> Result<u64, CubeError>;
}

crate::di_service!(MockImportService, [ImportService]);

pub struct ImportServiceImpl {
    meta_store: Arc<dyn MetaStore>,
    streaming_service: Arc<dyn StreamingService>,
    chunk_store: Arc<dyn ChunkDataStore>,
    remote_fs: Arc<dyn RemoteFs>,
    config_obj: Arc<dyn ConfigObj>,
    limits: Arc<ConcurrencyLimits>,
}

crate::di_service!(ImportServiceImpl, [ImportService]);

impl ImportServiceImpl {
    pub fn new(
        meta_store: Arc<dyn MetaStore>,
        streaming_service: Arc<dyn StreamingService>,
        chunk_store: Arc<dyn ChunkDataStore>,
        remote_fs: Arc<dyn RemoteFs>,
        config_obj: Arc<dyn ConfigObj>,
        limits: Arc<ConcurrencyLimits>,
    ) -> Arc<ImportServiceImpl> {
        Arc::new(ImportServiceImpl {
            meta_store,
            streaming_service,
            chunk_store,
            remote_fs,
            config_obj,
            limits,
        })
    }

    pub async fn resolve_location(
        &self,
        location: &str,
        table_id: u64,
        temp_dir: &Path,
    ) -> Result<(File, Option<TempPath>), CubeError> {
        if location.starts_with("http") {
            let (file, path) = tempfile::Builder::new()
                .prefix(&table_id.to_string())
                .tempfile_in(temp_dir)?
                .into_parts();
            let mut file = File::from_std(file);
            let mut stream = reqwest::get(location).await?.bytes_stream();
            let mut size = 0;
            while let Some(bytes) = stream.next().await {
                let bytes = bytes?;
                let slice = bytes.as_ref();
                size += slice.len();
                file.write_all(slice).await?;
            }
            log::info!("Import downloaded {} ({} bytes)", location, size);
            self.meta_store
                .update_location_download_size(table_id, location.to_string(), size as u64)
                .await?;
            file.seek(SeekFrom::Start(0)).await?;
            Ok((file, Some(path)))
        } else if location.starts_with("temp://") {
            let temp_file = self.download_temp_file(location).await?;
            let size = temp_file.metadata().await?.len();
            log::info!("Import downloaded {} ({} bytes)", location, size);
            self.meta_store
                .update_location_download_size(table_id, location.to_string(), size as u64)
                .await?;
            Ok((temp_file, None))
        } else {
            Ok((File::open(location.clone()).await?, None))
        }
    }

    async fn download_temp_file(&self, location: &str) -> Result<File, CubeError> {
        let to_download = ImportServiceImpl::temp_uploads_path(location);
        // TODO check file size
        let local_file = self.remote_fs.download_file(&to_download, None).await?;
        Ok(File::open(local_file).await?)
    }

    fn temp_uploads_path(location: &str) -> String {
        location.replace("temp://", "temp-uploads/")
    }

    async fn drop_temp_uploads(&self, location: &str) -> Result<(), CubeError> {
        // TODO There also should be a process which collects orphaned uploads due to failed imports
        if location.starts_with("temp://") {
            self.remote_fs
                .delete_file(&ImportServiceImpl::temp_uploads_path(location))
                .await?;
        }
        Ok(())
    }

    async fn do_import(
        &self,
        table: &IdRow<Table>,
        format: ImportFormat,
        location: &str,
    ) -> Result<(), CubeError> {
        let temp_dir = self.config_obj.data_dir().join("tmp");
        tokio::fs::create_dir_all(temp_dir.clone()).await?;

        let (file, tmp_path) = self
            .resolve_location(location.clone(), table.get_id(), &temp_dir)
            .await?;
        let mut row_stream = format
            .row_stream(
                file,
                location.to_string(),
                table.get_row().get_columns().clone(),
            )
            .await?;

        let mut ingestion = Ingestion::new(
            self.meta_store.clone(),
            self.chunk_store.clone(),
            self.limits.clone(),
            table.clone(),
        );

        let finish = |builders: Vec<Box<dyn ArrayBuilder>>| {
            builders.into_iter().map(|mut b| b.finish()).collect_vec()
        };

        let table_cols = table.get_row().get_columns().as_slice();
        let mut builders = create_array_builders(table_cols);
        let mut num_rows = 0;
        while let Some(row) = row_stream.next().await {
            if let Some(row) = row? {
                append_row(&mut builders, table_cols, &row);
                num_rows += 1;

                if num_rows >= self.config_obj.wal_split_threshold() as usize {
                    let mut to_add = create_array_builders(table_cols);
                    mem::swap(&mut builders, &mut to_add);
                    num_rows = 0;

                    ingestion.queue_data_frame(finish(to_add)).await?;
                }
            }
        }

        mem::drop(tmp_path);

        ingestion.queue_data_frame(finish(builders)).await?;
        ingestion.wait_completion().await
    }

    fn estimate_rows(location: &str, size: Option<u64>) -> u64 {
        if let Some(size) = size {
            let uncompressed_size = if location.contains(".gz") {
                size * 5
            } else {
                size
            };
            let average_row_length = 256;
            uncompressed_size / average_row_length
        } else {
            7_000_000
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
        for location in locations.iter() {
            self.do_import(&table, *format, location).await?;
        }

        for location in locations.iter() {
            self.drop_temp_uploads(location).await?;
        }

        Ok(())
    }

    async fn import_table_part(&self, table_id: u64, location: &str) -> Result<(), CubeError> {
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

        if locations.iter().find(|l| **l == location).is_none() {
            return Err(CubeError::internal(format!(
                "Location not found in table spec: table = {:?}, location = {}",
                table, location
            )));
        }
        if Table::is_stream_location(location) {
            self.streaming_service.stream_table(table, location).await?;
        } else {
            self.do_import(&table, *format, location).await?;
            self.drop_temp_uploads(&location).await?;
        }

        Ok(())
    }

    async fn estimate_location_row_count(&self, location: &str) -> Result<u64, CubeError> {
        if location.starts_with("http") {
            let client = reqwest::Client::new();
            let res = client.head(location).send().await?;
            let length = res.headers().get(reqwest::header::CONTENT_LENGTH);

            let size = if let Some(length) = length {
                Some(length.to_str()?.parse::<u64>()?)
            } else {
                None
            };
            Ok(ImportServiceImpl::estimate_rows(location, size))
        } else if location.starts_with("temp://") {
            // TODO do the actual estimation
            Ok(ImportServiceImpl::estimate_rows(location, None))
        } else {
            Ok(ImportServiceImpl::estimate_rows(
                location,
                Some(tokio::fs::metadata(location).await?.len()),
            ))
        }
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

    pub async fn queue_data_frame(&mut self, rows: Vec<ArrayRef>) -> Result<(), CubeError> {
        let active_data_frame = self.limits.acquire_data_frame().await?;

        let meta_store = self.meta_store.clone();
        let chunk_store = self.chunk_store.clone();
        let columns = self.table.get_row().get_columns().clone().clone();
        let table_id = self.table.get_id();
        // TODO In fact it should be only for inserts. Batch imports should still go straight to disk.
        let in_memory = self.table.get_row().in_memory_ingest();
        self.partition_jobs.push(cube_ext::spawn(async move {
            let new_chunks = chunk_store
                .partition_data(table_id, rows, &columns, in_memory)
                .await?;
            std::mem::drop(active_data_frame);

            // More data frame processing can proceed now as we dropped `active_data_frame`.
            // Time to wait to chunks to upload and activate them.
            let new_chunk_ids: Result<Vec<(u64, Option<u64>)>, CubeError> = join_all(new_chunks)
                .await
                .into_iter()
                .map(|c| {
                    let (c, file_size) = c??;
                    Ok((c.get_id(), file_size))
                })
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

#[cfg(test)]
mod tests {
    extern crate test;

    use crate::import::parse_decimal;

    #[test]
    fn parse_decimal_test() {
        assert_eq!(
            parse_decimal("-0.12345", 5).unwrap().to_string(5),
            "-0.12345",
        );
        assert_eq!(
            parse_decimal("-0.002694881400", 5).unwrap().to_string(5),
            "-0.00269",
        );
        assert_eq!(parse_decimal("-0.01", 5).unwrap().to_string(5), "-0.01",);
        assert_eq!(parse_decimal("200", 5).unwrap().to_string(5), "200",);
        assert_eq!(parse_decimal("200.35", 5).unwrap().to_string(5), "200.35",);
        assert_eq!(parse_decimal("-200.4", 5).unwrap().to_string(5), "-200.4",);
        assert_eq!(
            parse_decimal("-200.040000", 5).unwrap().to_string(5),
            "-200.04",
        );
    }
}
