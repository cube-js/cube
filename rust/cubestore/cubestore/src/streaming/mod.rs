mod buffered_stream;
use crate::config::injection::DIService;
use crate::config::ConfigObj;
use crate::metastore::replay_handle::{ReplayHandle, SeqPointer, SeqPointerForLocation};
use crate::metastore::source::SourceCredentials;
use crate::metastore::table::{StreamOffset, Table};
use crate::metastore::{Column, ColumnType, IdRow, MetaStore};
use crate::sql::timestamp_from_string;
use crate::store::ChunkDataStore;
use crate::table::data::{append_row, create_array_builders};
use crate::table::{Row, TableValue};
use crate::util::decimal::Decimal;
use crate::CubeError;
use arrow::array::ArrayBuilder;
use async_trait::async_trait;
use buffered_stream::BufferedStream;
use chrono::Utc;
use datafusion::cube_ext::ordfloat::OrdF64;
use futures::future::join_all;
use futures::stream::StreamExt;
use futures::Stream;
use futures_util::stream;
use itertools::Itertools;
use json::JsonValue;
use log::debug;
use reqwest::{Response, Url};
use serde::{Deserialize, Serialize};
use std::io::{Cursor, Write};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
#[cfg(debug_assertions)]
use stream_debug::MockStreamingSource;
use warp::hyper::body::Bytes;

#[async_trait]
pub trait StreamingService: DIService + Send + Sync {
    async fn stream_table(&self, table: IdRow<Table>, location: &str) -> Result<(), CubeError>;

    async fn validate_table_location(
        &self,
        table: IdRow<Table>,
        location: &str,
    ) -> Result<(), CubeError>;
}

pub struct StreamingServiceImpl {
    config_obj: Arc<dyn ConfigObj>,
    meta_store: Arc<dyn MetaStore>,
    chunk_store: Arc<dyn ChunkDataStore>,
    ksql_client: Arc<dyn KsqlClient>,
}

crate::di_service!(StreamingServiceImpl, [StreamingService]);

impl StreamingServiceImpl {
    pub fn new(
        config_obj: Arc<dyn ConfigObj>,
        meta_store: Arc<dyn MetaStore>,
        chunk_store: Arc<dyn ChunkDataStore>,
        ksql_client: Arc<dyn KsqlClient>,
    ) -> Arc<Self> {
        Arc::new(Self {
            config_obj,
            meta_store,
            chunk_store,
            ksql_client,
        })
    }

    async fn source_by(
        &self,
        table: &IdRow<Table>,
        location: &str,
    ) -> Result<Arc<dyn StreamingSource>, CubeError> {
        let location_url = Url::parse(location)?;
        if location_url.scheme() != "stream" {
            return Err(CubeError::internal(format!(
                "Non stream location received: {}",
                location
            )));
        }

        #[cfg(debug_assertions)]
        if location_url.host_str() == Some("mockstream") {
            return Ok(Arc::new(MockStreamingSource {}));
        }

        let meta_source = self
            .meta_store
            .get_source_by_name(
                location_url
                    .host_str()
                    .ok_or(CubeError::user(format!(
                "stream://<source_name>/<table_name> is expected as location but '{}' found",
                location
            )))?
                    .to_string(),
            )
            .await?;
        let path = location_url.path().split("/").collect::<Vec<_>>();
        let mut table_name = path[0..path.len() - 1].join("");
        let partition = path[path.len() - 1].parse::<usize>().ok();
        if partition.is_none() {
            table_name = location_url.path().to_string().replace("/", "");
        }
        match meta_source.get_row().source_type() {
            SourceCredentials::KSql {
                user,
                password,
                url,
            } => Ok(Arc::new(KSqlStreamingSource {
                user: user.clone(),
                password: password.clone(),
                table: table_name,
                endpoint_url: url.to_string(),
                select_statement: table.get_row().select_statement().clone(),
                partition,
                ksql_client: self.ksql_client.clone(),
                offset: table.get_row().stream_offset().clone(),
            })),
        }
    }

    async fn try_seal_table(&self, table: &IdRow<Table>) -> Result<bool, CubeError> {
        if let Some(seal_at) = table.get_row().seal_at() {
            if seal_at < &Utc::now() {
                self.meta_store.seal_table(table.get_id()).await?;
                Ok(true)
            } else {
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }

    async fn initial_seq_for(
        &self,
        table: &IdRow<Table>,
        location: &str,
    ) -> Result<Option<i64>, CubeError> {
        let replay_handles = self
            .meta_store
            .get_replay_handles_by_table(table.get_id())
            .await?;
        let (with_failed, without_failed) = replay_handles
            .iter()
            .partition::<Vec<_>, _>(|h| h.get_row().has_failed_to_persist_chunks());

        fn vec_union(
            table: &IdRow<Table>,
            location: &str,
            vec: &Vec<&IdRow<ReplayHandle>>,
        ) -> Result<SeqPointer, CubeError> {
            Ok(vec
                .iter()
                .map(|h| h.get_row().seq_pointer_for_location(table, location))
                .collect::<Result<Vec<_>, _>>()?
                .iter()
                .fold(SeqPointer::new(None, None), |mut a, b| {
                    if let Some(b) = b {
                        a.union(b);
                    }
                    a
                }))
        }

        let failed_seq_pointer = vec_union(&table, location, &with_failed)?;
        let mut initial_seq_pointer = vec_union(&table, location, &without_failed)?;
        initial_seq_pointer.subtract_from_right(&failed_seq_pointer);

        Ok(initial_seq_pointer.end_seq().clone())
    }
}

#[async_trait]
impl StreamingService for StreamingServiceImpl {
    async fn stream_table(&self, table: IdRow<Table>, location: &str) -> Result<(), CubeError> {
        if table.get_row().sealed() {
            return Ok(());
        }

        let source = self.source_by(&table, location).await?;
        let seq_column = table.get_row().seq_column().ok_or_else(|| {
            CubeError::internal(format!(
                "Seq column is not defined for streaming table '{}'",
                table.get_row().get_table_name()
            ))
        })?;
        let location_index = table.get_row().location_index(location)?;
        let initial_seq_value = self.initial_seq_for(&table, location).await?;
        let stream = source
            .row_stream(
                table.get_row().get_columns().clone(),
                seq_column.clone(),
                initial_seq_value.clone(),
            )
            .await?;
        let mut stream = BufferedStream::new(
            stream,
            self.config_obj.wal_split_threshold() as usize,
            Duration::from_millis(1000),
        );

        let finish = |builders: Vec<Box<dyn ArrayBuilder>>| {
            builders.into_iter().map(|mut b| b.finish()).collect_vec()
        };

        let mut sealed = false;

        let seq_column_index = table
            .get_row()
            .seq_column()
            .expect(&format!(
                "Streaming table {:?} with undefined seq column",
                table
            ))
            .get_index();

        let mut last_init_seq_check = SystemTime::now();

        while !sealed {
            let new_rows = match tokio::time::timeout(
                Duration::from_secs(self.config_obj.stale_stream_timeout()),
                stream.next(),
            )
            .await
            {
                Ok(Some(rows)) => rows,
                Ok(None) => {
                    self.try_seal_table(&table).await?;
                    break;
                }
                Err(e) => {
                    self.try_seal_table(&table).await?;

                    return Err(CubeError::user(format!("Stale stream timeout: {}", e)));
                }
            };

            if last_init_seq_check.elapsed().unwrap().as_secs()
                > self.config_obj.stream_replay_check_interval_secs()
            {
                let new_initial_seq = self.initial_seq_for(&table, location).await?;
                if new_initial_seq < initial_seq_value {
                    return Err(CubeError::user(format!(
                        "Stream requires replay: initial seq was {:?} but new is {:?}",
                        initial_seq_value, new_initial_seq
                    )));
                }
                last_init_seq_check = SystemTime::now();
            }

            let rows = new_rows;
            debug!("Received {} rows for {}", rows.len(), location);
            let table_cols = table.get_row().get_columns().as_slice();
            let mut builders = create_array_builders(table_cols);

            let mut start_seq: Option<i64> = None;
            let mut end_seq: Option<i64> = None;

            for row in rows {
                let row = row?;
                append_row(&mut builders, table_cols, &row);
                match &row.values()[seq_column_index] {
                    TableValue::Int(new_last_seq) => {
                        if let Some(start_seq) = &mut start_seq {
                            *start_seq = (*start_seq).min(*new_last_seq);
                        } else {
                            start_seq = Some(*new_last_seq);
                        }

                        if let Some(end_seq) = &mut end_seq {
                            *end_seq = (*end_seq).max(*new_last_seq);
                        } else {
                            end_seq = Some(*new_last_seq);
                        }
                    }
                    x => panic!("Unexpected type for sequence column: {:?}", x),
                }
            }
            let seq_pointer = SeqPointer::new(start_seq, end_seq);
            let replay_handle = self
                .meta_store
                .create_replay_handle(table.get_id(), location_index, seq_pointer)
                .await?;
            let new_chunks = self
                .chunk_store
                .partition_data(
                    table.get_id(),
                    finish(builders),
                    table.get_row().get_columns().as_slice(),
                    true,
                )
                .await?;

            let new_chunk_ids: Result<Vec<(u64, Option<u64>)>, CubeError> = join_all(new_chunks)
                .await
                .into_iter()
                .map(|c| {
                    let (c, file_size) = c??;
                    Ok((c.get_id(), file_size))
                })
                .collect();
            self.meta_store
                .activate_chunks(table.get_id(), new_chunk_ids?, Some(replay_handle.get_id()))
                .await?;

            sealed = self.try_seal_table(&table).await?;
        }

        Ok(())
    }

    async fn validate_table_location(
        &self,
        table: IdRow<Table>,
        location: &str,
    ) -> Result<(), CubeError> {
        let source = self.source_by(&table, location).await?;
        source.validate_table_location()?;
        Ok(())
    }
}

#[async_trait]
pub trait StreamingSource: Send + Sync {
    async fn row_stream(
        &self,
        columns: Vec<Column>,
        seq_column: Column,
        initial_seq_value: Option<i64>,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Row, CubeError>> + Send>>, CubeError>;

    fn validate_table_location(&self) -> Result<(), CubeError>;
}

#[derive(Clone)]
pub struct KSqlStreamingSource {
    user: Option<String>,
    password: Option<String>,
    table: String,
    endpoint_url: String,
    select_statement: Option<String>,
    offset: Option<StreamOffset>,
    partition: Option<usize>,
    ksql_client: Arc<dyn KsqlClient>,
}

#[derive(Serialize, Deserialize)]
pub struct KSqlError {
    message: String,
}

#[derive(Serialize, Deserialize)]
pub struct KSqlQuery {
    pub sql: String,
    pub properties: KSqlStreamsProperties,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct KSqlStreamsProperties {
    #[serde(rename = "ksql.streams.auto.offset.reset")]
    offset: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct KSqlQuerySchema {
    #[serde(rename = "queryId")]
    pub query_id: String,
    #[serde(rename = "columnNames")]
    pub column_names: Vec<String>,
    #[serde(rename = "columnTypes")]
    pub column_types: Vec<String>,
}

impl KSqlStreamingSource {
    fn query(&self, seq_value: Option<i64>) -> Result<String, CubeError> {
        let mut sql = self.select_statement.as_ref().map_or(
            format!("SELECT * FROM `{}` EMIT CHANGES;", self.table),
            |sql| format!("{} EMIT CHANGES;", sql),
        );
        if let Some(from_pos) = sql.to_lowercase().find("from") {
            if let Some(right_pos) = sql.to_lowercase().rfind("from") {
                if from_pos == right_pos {
                    sql.insert_str(from_pos, ", ROWOFFSET as `__seq` ");
                } else {
                    return Err(CubeError::user(format!(
                        "multiple FROM are found in select SQL: {}",
                        sql
                    )));
                }
            }
        } else {
            return Err(CubeError::user(format!(
                "FROM is not found in select SQL: {}",
                sql
            )));
        }
        let mut filters_to_add = Vec::new();
        if let Some(partition) = self.partition {
            filters_to_add.push(format!("ROWPARTITION = {}", partition));
        }
        if let Some(seq_value) = seq_value {
            filters_to_add.push(format!("ROWOFFSET >= {}", seq_value));
        }
        if !filters_to_add.is_empty() {
            let filters_to_add = filters_to_add.iter().join(" AND ");
            if let Some(from_pos) = sql.to_lowercase().find("where") {
                if let Some(right_pos) = sql.to_lowercase().rfind("where") {
                    if from_pos == right_pos {
                        sql.insert_str(
                            from_pos + "where".len(),
                            &format!(" {} AND (", filters_to_add),
                        );
                        if let Some(from_pos) = sql.to_lowercase().find("emit") {
                            sql.insert_str(from_pos, ") ");
                        } else {
                            return Err(CubeError::user(format!("emit not found in SQL: {}", sql)));
                        }
                    } else {
                        return Err(CubeError::user(format!(
                            "multiple WHERE are found in select SQL: {}",
                            sql
                        )));
                    }
                }
            } else {
                if let Some(from_pos) = sql.to_lowercase().find("emit") {
                    sql.insert_str(from_pos, &format!("WHERE {} ", filters_to_add));
                }
            }
        }
        Ok(sql)
    }

    fn parse_lines(
        tail_bytes: &mut Bytes,
        bytes: Result<Bytes, reqwest::Error>,
        columns: Vec<Column>,
    ) -> Result<Vec<Row>, CubeError> {
        let mut rows = Vec::new();
        let b = bytes?;
        let string = String::from_utf8_lossy(&b);
        let mut concat = Cursor::new(Vec::new());
        concat.write_all(tail_bytes)?;
        let last_separator = string.rfind("\n");
        if last_separator.is_none() {
            concat.write_all(&b)?;
            *tail_bytes = Bytes::from(concat.into_inner());
            return Ok(rows);
        }
        let last_separator_index = last_separator.unwrap();
        concat.write_all(&b[0..last_separator_index])?;

        let buf = concat.into_inner();
        let lines_str = String::from_utf8_lossy(buf.as_slice());

        *tail_bytes = if last_separator_index == b.len() || last_separator_index == b.len() - 1 {
            Bytes::from(Vec::new())
        } else {
            Bytes::from(b[(last_separator_index + 1)..].to_vec())
        };

        for line in lines_str.split("\n") {
            let res = json::parse(line)?;
            if res.has_key("queryId") {
                let schema: KSqlQuerySchema = serde_json::from_str(line)?;
                let schema_column_names = columns
                    .iter()
                    .map(|c| c.get_name().to_string())
                    .collect::<Vec<_>>();
                let ksql_column_names = schema
                    .column_names
                    .iter()
                    .map(|c| c.to_string())
                    .collect::<Vec<_>>();
                if ksql_column_names != schema_column_names {
                    return Err(CubeError::user(format!(
                        "Column names of ksql stream and table doesn't match: {:?} and {:?}",
                        ksql_column_names, schema_column_names
                    )));
                }
                continue;
            }
            let row_values = match res {
                JsonValue::Array(values) => values
                    .into_iter()
                    .zip_eq(columns.iter())
                    .map(|(value, col)| {
                        match col.get_column_type() {
                            ColumnType::String => {
                                match value {
                                    JsonValue::Short(v) => Ok(TableValue::String(v.to_string())),
                                    JsonValue::String(v) => Ok(TableValue::String(v.to_string())),
                                    JsonValue::Number(v) => Ok(TableValue::String(v.to_string())),
                                    JsonValue::Boolean(v) => Ok(TableValue::String(v.to_string())),
                                    JsonValue::Null => Ok(TableValue::Null),
                                    x => Err(CubeError::internal(format!(
                                        "ksql source returned {:?} as row value but only primitive values are supported",
                                        x
                                    ))),
                                }
                            }
                            ColumnType::Int => {
                                match value {
                                    JsonValue::Number(v) => Ok(TableValue::Int(v.as_fixed_point_i64(0).ok_or(CubeError::user(format!("Can't convert {:?} to int", v)))?)),
                                    JsonValue::Null => Ok(TableValue::Null),
                                    x => Err(CubeError::internal(format!(
                                        "ksql source returned {:?} as row value but int expected",
                                        x
                                    ))),
                                }
                            }
                            ColumnType::Bytes => {
                                match value {
                                    _ => Err(CubeError::internal(format!(
                                        "ksql source bytes import isn't supported"
                                    ))),
                                }
                            }
                            ColumnType::HyperLogLog(_) => {
                                match value {
                                    _ => Err(CubeError::internal(format!(
                                        "ksql source HLL import isn't supported"
                                    ))),
                                }
                            }
                            ColumnType::Timestamp => {
                                match value {
                                    JsonValue::Short(v) => Ok(TableValue::Timestamp(timestamp_from_string(v.as_str())?)),
                                    JsonValue::String(v) => Ok(TableValue::Timestamp(timestamp_from_string(v.as_str())?)),
                                    JsonValue::Null => Ok(TableValue::Null),
                                    x => Err(CubeError::internal(format!(
                                        "ksql source returned {:?} as row value but only primitive values are supported",
                                        x
                                    ))),
                                }
                            }
                            ColumnType::Decimal { scale, .. } => {
                                match value {
                                    JsonValue::Number(v) => Ok(TableValue::Decimal(Decimal::new(v.as_fixed_point_i64(*scale as u16).ok_or(CubeError::user(format!("Can't convert {:?} to decimal", v)))?))),
                                    JsonValue::Null => Ok(TableValue::Null),
                                    x => Err(CubeError::internal(format!(
                                        "ksql source returned {:?} as row value but only number values are supported",
                                        x
                                    ))),
                                }
                            }
                            ColumnType::Float => {
                                match value {
                                    JsonValue::Number(v) => Ok(TableValue::Float(OrdF64(v.into()))),
                                    JsonValue::Null => Ok(TableValue::Null),
                                    x => Err(CubeError::internal(format!(
                                        "ksql source returned {:?} as row value but only number values are supported",
                                        x
                                    ))),
                                }
                            }
                            ColumnType::Boolean => {
                                match value {
                                    JsonValue::Boolean(v) => Ok(TableValue::Boolean(v)),
                                    JsonValue::Null => Ok(TableValue::Null),
                                    x => Err(CubeError::internal(format!(
                                        "ksql source returned {:?} as row value but only boolean values are supported",
                                        x
                                    ))),
                                }
                            }
                        }
                    })
                    .collect::<Result<Vec<TableValue>, CubeError>>(),
                x => Err(CubeError::internal(format!(
                    "ksql source returned {:?} but array was expected",
                    x
                ))),
            };
            rows.push(Row::new(row_values?));
        }

        Ok(rows)
    }

    async fn post_req<T: Serialize + ?Sized>(
        &self,
        url: &str,
        json: &T,
    ) -> Result<KsqlResponse, CubeError> {
        self.ksql_client
            .post_req(
                url,
                serde_json::to_value(json)?,
                &self.endpoint_url,
                &self.user,
                &self.password,
            )
            .await
    }
}

pub enum KsqlResponse {
    ReqwestResponse { response: Response },
    JsonNl { values: Vec<serde_json::Value> },
}

impl KsqlResponse {
    pub fn bytes_stream(self) -> Pin<Box<dyn Stream<Item = Result<Bytes, reqwest::Error>> + Send>> {
        match self {
            KsqlResponse::ReqwestResponse { response } => Box::pin(response.bytes_stream()),
            KsqlResponse::JsonNl { values } => {
                let result = values
                    .into_iter()
                    .map(|v| {
                        Ok(Bytes::copy_from_slice(
                            format!("{}\n", serde_json::to_string(&v).unwrap()).as_bytes(),
                        ))
                    })
                    .collect::<Vec<_>>();
                Box::pin(stream::iter(result))
            }
        }
    }
}

#[async_trait]
pub trait KsqlClient: DIService + Send + Sync {
    async fn post_req(
        &self,
        url: &str,
        json: serde_json::Value,
        endpoint_url: &String,
        user: &Option<String>,
        password: &Option<String>,
    ) -> Result<KsqlResponse, CubeError>;
}

pub struct KsqlClientImpl {}

#[async_trait]
impl KsqlClient for KsqlClientImpl {
    async fn post_req(
        &self,
        url: &str,
        json: serde_json::Value,
        endpoint_url: &String,
        user: &Option<String>,
        password: &Option<String>,
    ) -> Result<KsqlResponse, CubeError> {
        let client = reqwest::ClientBuilder::new()
            .http2_prior_knowledge()
            .use_rustls_tls()
            .user_agent("cubestore")
            .build()
            .unwrap();
        let mut builder = client.post(format!("{}{}", endpoint_url, url));
        if let Some(user) = user {
            builder = builder.basic_auth(user.to_string(), password.clone())
        }
        log::trace!(
            "Sending ksql API request: {}",
            serde_json::to_string(&json).unwrap_or("Can't serialize".to_string())
        );
        let res = builder.json(&json).send().await?;
        if res.status() != 200 {
            let error = res.json::<KSqlError>().await?;
            if error.message.contains("does not exist") {
                return Err(CubeError::corrupt_data(format!(
                    "ksql api error: {}",
                    error.message
                )));
            }
            return Err(CubeError::user(format!(
                "ksql api error: {}",
                error.message
            )));
        }
        Ok(KsqlResponse::ReqwestResponse { response: res })
    }
}

impl KsqlClientImpl {
    pub fn new() -> Arc<Self> {
        Arc::new(KsqlClientImpl {})
    }
}

crate::di_service!(KsqlClientImpl, [KsqlClient]);

#[async_trait]
impl StreamingSource for KSqlStreamingSource {
    async fn row_stream(
        &self,
        columns: Vec<Column>,
        _seq_column: Column,
        initial_seq_value: Option<i64>,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Row, CubeError>> + Send>>, CubeError> {
        let res = self
            .post_req(
                "/query-stream",
                &KSqlQuery {
                    sql: self.query(initial_seq_value)?, //format!("SELECT * FROM `{}` EMIT CHANGES;", self.table),
                    properties: KSqlStreamsProperties {
                        offset: self
                            .offset
                            .as_ref()
                            .map(|o| match o {
                                StreamOffset::Earliest => "earliest".to_string(),
                                StreamOffset::Latest => {
                                    if let Some(_) = initial_seq_value {
                                        "earliest".to_string()
                                    } else {
                                        "latest".to_string()
                                    }
                                }
                            })
                            .unwrap_or("latest".to_string()),
                    },
                },
            )
            .await?;
        let column_to_move = columns.clone();
        Ok(Box::pin(
            res.bytes_stream()
                .scan(
                    Bytes::new(),
                    move |tail_bytes,
                          bytes: Result<_, _>|
                          -> futures_util::future::Ready<
                        Option<Pin<Box<dyn Stream<Item = Result<Row, CubeError>> + Send>>>,
                    > {
                        let rows = Self::parse_lines(tail_bytes, bytes, column_to_move.clone())
                            .map_err(|e| {
                                CubeError::internal(format!(
                                    "Error during parsing ksql response: {}",
                                    e
                                ))
                            });
                        futures_util::future::ready(Some(Box::pin(stream::iter(match rows {
                            Ok(rows) => rows.into_iter().map(|r| Ok(r)).collect::<Vec<_>>(),
                            Err(e) => vec![Err(e)],
                        }))))
                    },
                )
                .flatten(),
        ))
    }

    fn validate_table_location(&self) -> Result<(), CubeError> {
        self.query(None)?;
        Ok(())
    }
}

#[cfg(debug_assertions)]
mod stream_debug {
    use super::*;
    use crate::table::TimestampValue;
    use async_std::task::{Context, Poll};
    use chrono::{DateTime, Utc};

    struct MockRowStream {
        last_id: i64,
        last_readed: DateTime<Utc>,
    }

    impl MockRowStream {
        fn new(last_id: i64) -> Self {
            Self {
                last_id,
                last_readed: Utc::now(),
            }
        }
    }

    impl Stream for MockRowStream {
        type Item = Result<Row, CubeError>;

        fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            /* if Utc::now().signed_duration_since(self.last_readed).num_milliseconds() < 10 {
            return Poll::Pending;
            } */

            let mut last_id = self.last_id;
            last_id += 1;
            let row = Row::new(vec![
                TableValue::Int(last_id),
                TableValue::Int(last_id % 10),
                TableValue::Timestamp(TimestampValue::new(Utc::now().timestamp_nanos())),
                TableValue::Int(last_id),
            ]);
            unsafe {
                let self_mut = self.get_unchecked_mut();

                self_mut.last_id = last_id;
                self_mut.last_readed = Utc::now();
            }
            std::thread::sleep(Duration::from_millis(if rand::random::<u64>() % 200 == 7 {
                500
            } else {
                0
            }));
            Poll::Ready(Some(Ok(row)))
        }
    }

    pub struct MockStreamingSource {}

    #[async_trait]
    impl StreamingSource for MockStreamingSource {
        async fn row_stream(
            &self,
            _columns: Vec<Column>,
            _seq_column: Column,
            initial_seq_value: Option<i64>,
        ) -> Result<Pin<Box<dyn Stream<Item = Result<Row, CubeError>> + Send>>, CubeError> {
            Ok(Box::pin(MockRowStream::new(initial_seq_value.unwrap_or(0))))
        }

        fn validate_table_location(&self) -> Result<(), CubeError> {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use futures_timer::Delay;
    use std::time::Duration;

    use pretty_assertions::assert_eq;

    use crate::cluster::Cluster;
    use crate::config::Config;
    use crate::metastore::{MetaStoreTable, RowKey};

    use super::*;
    use crate::metastore::job::JobType;
    use crate::scheduler::SchedulerImpl;
    use crate::sql::MySqlDialectWithBackTicks;
    use crate::streaming::{KSqlQuery, KSqlQuerySchema, KsqlClient, KsqlResponse};
    use crate::TableId;
    use sqlparser::ast::{BinaryOperator, Expr, SetExpr, Statement, Value};
    use sqlparser::parser::Parser;
    use sqlparser::tokenizer::Tokenizer;
    use tokio::time::timeout;

    pub struct MockKsqlClient;

    crate::di_service!(MockKsqlClient, [KsqlClient]);

    #[async_trait::async_trait]
    impl KsqlClient for MockKsqlClient {
        async fn post_req(
            &self,
            _url: &str,
            json: serde_json::Value,
            _endpoint_url: &String,
            _user: &Option<String>,
            _password: &Option<String>,
        ) -> Result<KsqlResponse, CubeError> {
            println!("KSQL post_req: {:?}", serde_json::to_string(&json));
            let query = serde_json::from_value::<KSqlQuery>(json)?;

            let dialect = &MySqlDialectWithBackTicks {};
            let mut tokenizer = Tokenizer::new(dialect, query.sql.as_str());
            let tokens = tokenizer.tokenize().unwrap();
            let statement = Parser::new(tokens, dialect).parse_statement()?;

            fn find_filter(expr: &Expr, col: &str, binary_op: &BinaryOperator) -> Option<String> {
                match expr {
                    Expr::BinaryOp { left, right, op } => {
                        let mut value = None;
                        if let Expr::Identifier(id) = left.as_ref() {
                            if id.value == col && op == binary_op {
                                if let Expr::Value(v) = right.as_ref() {
                                    value = Some(v);
                                }
                            }
                        }
                        if let Expr::Identifier(id) = right.as_ref() {
                            if id.value == col && op == binary_op {
                                if let Expr::Value(v) = left.as_ref() {
                                    value = Some(v);
                                }
                            }
                        }
                        if let Some(v) = value {
                            Some(match v {
                                Value::SingleQuotedString(s) => s.to_string(),
                                Value::DoubleQuotedString(s) => s.to_string(),
                                Value::Number(s, _) => s.to_string(),
                                x => panic!("Unsupported value: {:?}", x),
                            })
                        } else {
                            if op == &BinaryOperator::And || op == &BinaryOperator::Or {
                                if let Some(res) = find_filter(left, col, binary_op) {
                                    return Some(res);
                                }
                                if let Some(res) = find_filter(right, col, binary_op) {
                                    return Some(res);
                                }
                            }
                            None
                        }
                    }
                    Expr::Nested(e) => find_filter(&e, col, binary_op),
                    _ => None,
                }
            }

            let mut partition = None;
            let mut offset = 0;
            if let Statement::Query(q) = statement {
                if let SetExpr::Select(s) = q.body {
                    if let Some(s) = s.selection {
                        if let Some(p) = find_filter(&s, "ROWPARTITION", &BinaryOperator::Eq) {
                            partition = Some(p.parse::<u64>().unwrap());
                        }
                        if let Some(o) = find_filter(&s, "ROWOFFSET", &BinaryOperator::GtEq) {
                            offset = o.parse::<u64>().unwrap();
                        }
                    }
                }
            }

            let mut values = Vec::new();
            values.push(
                serde_json::to_value(KSqlQuerySchema {
                    query_id: "42".to_string(),
                    column_names: vec![
                        "ANONYMOUSID".to_string(),
                        "MESSAGEID".to_string(),
                        "__seq".to_string(),
                    ],
                    column_types: vec![
                        "STRING".to_string(),
                        "STRING".to_string(),
                        "BIGINT".to_string(),
                    ],
                })
                .unwrap(),
            );

            if &query.properties.offset == "latest" {
                return Ok(KsqlResponse::JsonNl { values });
            }

            for i in offset..50000 {
                for j in 0..2 {
                    if let Some(p) = &partition {
                        if *p != j {
                            continue;
                        }
                    }

                    values.push(serde_json::json!([j.to_string(), i.to_string(), i]));
                }
            }

            Ok(KsqlResponse::JsonNl { values })
        }
    }

    #[tokio::test]
    async fn streaming_replay() {
        Config::test("streaming_replay").update_config(|mut c| {
            c.stream_replay_check_interval_secs = 1;
            c.compaction_in_memory_chunks_max_lifetime_threshold = 8;
            c.partition_split_threshold = 1000000;
            c.max_partition_split_threshold = 1000000;
            c.compaction_chunks_count_threshold = 100;
            c.compaction_chunks_total_size_threshold = 100000;
            c.stale_stream_timeout = 1;
            c.wal_split_threshold = 16384;
            c
        }).start_with_injector_override(async move |injector| {
            injector.register_typed::<dyn KsqlClient, _, _, _>(async move |_| {
                Arc::new(MockKsqlClient)
            })
                .await
        }, async move |services| {
            let chunk_store = services.injector.get_service_typed::<dyn ChunkDataStore>().await;
            let scheduler = services.injector.get_service_typed::<SchedulerImpl>().await;
            let service = services.sql_service;
            let meta_store = services.meta_store;

            let _ = service.exec_query("CREATE SCHEMA test").await.unwrap();

            service
                .exec_query("CREATE SOURCE OR UPDATE ksql AS 'ksql' VALUES (user = 'foo', password = 'bar', url = 'http://foo.com')")
                .await
                .unwrap();

            let listener = services.cluster.job_result_listener();

            let _ = service
                .exec_query("CREATE TABLE test.events_by_type_1 (`ANONYMOUSID` text, `MESSAGEID` text) WITH (select_statement = 'SELECT * FROM EVENTS_BY_TYPE WHERE time >= \\'2022-01-01\\' AND time < \\'2022-02-01\\'', stream_offset = 'earliest') unique key (`ANONYMOUSID`, `MESSAGEID`) INDEX by_anonymous(`ANONYMOUSID`) location 'stream://ksql/EVENTS_BY_TYPE/0', 'stream://ksql/EVENTS_BY_TYPE/1'")
                .await
                .unwrap();

            let wait = listener.wait_for_job_results(vec![
                (RowKey::Table(TableId::Tables, 1), JobType::TableImportCSV("stream://ksql/EVENTS_BY_TYPE/0".to_string())),
                (RowKey::Table(TableId::Tables, 1), JobType::TableImportCSV("stream://ksql/EVENTS_BY_TYPE/1".to_string())),
            ]);
            timeout(Duration::from_secs(15), wait).await.unwrap().unwrap();

            let result = service
                .exec_query("SELECT COUNT(*) FROM test.events_by_type_1")
                .await
                .unwrap();
            assert_eq!(result.get_rows(), &vec![Row::new(vec![TableValue::Int(100000)])]);

            let listener = services.cluster.job_result_listener();
            let chunks = meta_store.chunks_table().all_rows().await.unwrap();
            let replay_handles = meta_store.get_replay_handles_by_ids(chunks.iter().filter_map(|c| c.get_row().replay_handle_id().clone()).collect()).await.unwrap();
            let mut middle_chunk = None;
            for chunk in chunks.iter() {
                if let Some(handle_id) = chunk.get_row().replay_handle_id() {
                    let handle = replay_handles.iter().find(|h| h.get_id() == *handle_id).unwrap();
                    if let Some(seq_pointers) = handle.get_row().seq_pointers_by_location() {
                        if seq_pointers.iter().any(|p| p.as_ref().map(|p| p.start_seq().as_ref().zip(p.end_seq().as_ref()).map(|(a, b)| *a > 0 && *b <= 32768).unwrap_or(false)).unwrap_or(false)) {
                            chunk_store.free_memory_chunk(chunk.get_id()).await.unwrap();
                            middle_chunk = Some(chunk.clone());
                            break;
                        }
                    }
                }
            }
            Delay::new(Duration::from_millis(10000)).await;
            scheduler.schedule_compaction_in_memory_chunks_if_needed(&meta_store.get_partition(middle_chunk.unwrap().get_row().get_partition_id()).await.unwrap()).await.unwrap();

            let wait = listener.wait_for_job_results(vec![
                (RowKey::Table(TableId::Partitions, 1), JobType::InMemoryChunksCompaction),
            ]);
            timeout(Duration::from_secs(10), wait).await.unwrap().unwrap();

            println!("chunks: {:#?}", service
                .exec_query("SELECT * FROM system.chunks")
                .await
                .unwrap()
            );
            println!("replay handles: {:#?}", service
                .exec_query("SELECT * FROM system.replay_handles")
                .await
                .unwrap()
            );

            let result = service
                .exec_query("SELECT COUNT(*) FROM test.events_by_type_1")
                .await
                .unwrap();
            assert_eq!(result.get_rows(), &vec![Row::new(vec![TableValue::Int(100000 - 16384)])]);

            let listener = services.cluster.job_result_listener();

            scheduler.reconcile_table_imports().await.unwrap();

            let wait = listener.wait_for_job_results(vec![
                (RowKey::Table(TableId::Tables, 1), JobType::TableImportCSV("stream://ksql/EVENTS_BY_TYPE/0".to_string())),
                (RowKey::Table(TableId::Tables, 1), JobType::TableImportCSV("stream://ksql/EVENTS_BY_TYPE/1".to_string())),
            ]);
            timeout(Duration::from_secs(10), wait).await.unwrap().unwrap();
            Delay::new(Duration::from_millis(10000)).await;

            let result = service
                .exec_query("SELECT COUNT(*) FROM test.events_by_type_1")
                .await
                .unwrap();
            assert_eq!(result.get_rows(), &vec![Row::new(vec![TableValue::Int(100000)])]);

            println!("replay handles pre merge: {:#?}", service
                .exec_query("SELECT * FROM system.replay_handles")
                .await
                .unwrap()
            );

            scheduler.merge_replay_handles().await.unwrap();

            let result = service
                .exec_query("SELECT * FROM system.replay_handles WHERE has_failed_to_persist_chunks = true")
                .await
                .unwrap();
            assert_eq!(result.get_rows().len(), 0);

            println!("replay handles after merge: {:#?}", service
                .exec_query("SELECT * FROM system.replay_handles")
                .await
                .unwrap()
            );

            service
                .exec_query("DROP TABLE test.events_by_type_1")
                .await
                .unwrap();

            let result = service
                .exec_query("SELECT * FROM system.replay_handles")
                .await
                .unwrap();
            assert_eq!(result.get_rows().len(), 0);
        })
            .await;
    }
}
