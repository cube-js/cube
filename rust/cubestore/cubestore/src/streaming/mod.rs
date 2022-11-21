use crate::config::injection::DIService;
use crate::config::ConfigObj;
use crate::metastore::replay_handle::{ReplayHandle, SeqPointer, SeqPointerForLocation};
use crate::metastore::source::SourceCredentials;
use crate::metastore::table::Table;
use crate::metastore::{Column, ColumnType, IdRow, MetaStore};
use crate::sql::timestamp_from_string;
use crate::store::ChunkDataStore;
use crate::table::data::{append_row, create_array_builders};
use crate::table::{Row, TableValue};
use crate::util::decimal::Decimal;
use crate::CubeError;
use arrow::array::ArrayBuilder;
use async_trait::async_trait;
use chrono::Utc;
use datafusion::cube_ext::ordfloat::OrdF64;
use futures::future::join_all;
use futures::stream::StreamExt;
use futures::Stream;
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
}

crate::di_service!(StreamingServiceImpl, [StreamingService]);

impl StreamingServiceImpl {
    pub fn new(
        config_obj: Arc<dyn ConfigObj>,
        meta_store: Arc<dyn MetaStore>,
        chunk_store: Arc<dyn ChunkDataStore>,
    ) -> Arc<Self> {
        Arc::new(Self {
            config_obj,
            meta_store,
            chunk_store,
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
        let mut stream = source
            .row_stream(
                table.get_row().get_columns().clone(),
                seq_column.clone(),
                initial_seq_value.clone(),
            )
            .await?;

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

            let rows = new_rows?;
            debug!("Received {} rows for {}", rows.len(), location);
            let table_cols = table.get_row().get_columns().as_slice();
            let mut builders = create_array_builders(table_cols);

            let mut start_seq: Option<i64> = None;
            let mut end_seq: Option<i64> = None;

            for row in rows {
                append_row(&mut builders, table_cols, &row);
                match &row.values()[seq_column_index] {
                    TableValue::Int(new_last_seq) => {
                        if let Some(start_seq) = &mut start_seq {
                            *start_seq = (*start_seq).min(*new_last_seq);
                        } else {
                            start_seq = Some(*new_last_seq);
                        }

                        if let Some(end_seq) = &mut end_seq {
                            if *new_last_seq - *end_seq != 1 {
                                return Err(CubeError::internal(format!(
                                    "Unexpected sequence increase gap from {} to {}. Back filling with jumping sequence numbers isn't supported.",
                                    new_last_seq, end_seq
                                )));
                            }
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
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Vec<Row>, CubeError>> + Send>>, CubeError>;

    fn validate_table_location(&self) -> Result<(), CubeError>;
}

#[derive(Clone)]
pub struct KSqlStreamingSource {
    user: Option<String>,
    password: Option<String>,
    table: String,
    endpoint_url: String,
    select_statement: Option<String>,
    partition: Option<usize>,
}

#[derive(Serialize, Deserialize)]
pub struct KSqlError {
    message: String,
}

#[derive(Serialize, Deserialize)]
pub struct KSqlQuery {
    sql: String,
    properties: KSqlStreamsProperties,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct KSqlStreamsProperties {
    #[serde(rename = "ksql.streams.auto.offset.reset")]
    offset: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct KSqlQuerySchema {
    #[serde(rename = "queryId")]
    query_id: String,
    #[serde(rename = "columnNames")]
    column_names: Vec<String>,
    #[serde(rename = "columnTypes")]
    column_types: Vec<String>,
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
    ) -> Result<Response, CubeError> {
        let client = reqwest::ClientBuilder::new()
            .http2_prior_knowledge()
            .use_rustls_tls()
            .user_agent("cubestore")
            .build()
            .unwrap();
        let mut builder = client.post(format!("{}{}", self.endpoint_url, url));
        if let Some(user) = &self.user {
            builder = builder.basic_auth(user.to_string(), self.password.clone())
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
        Ok(res)
    }
}

#[async_trait]
impl StreamingSource for KSqlStreamingSource {
    async fn row_stream(
        &self,
        columns: Vec<Column>,
        _seq_column: Column,
        initial_seq_value: Option<i64>,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Vec<Row>, CubeError>> + Send>>, CubeError> {
        let res = self
            .post_req(
                "/query-stream",
                &KSqlQuery {
                    sql: self.query(initial_seq_value)?, //format!("SELECT * FROM `{}` EMIT CHANGES;", self.table),
                    properties: KSqlStreamsProperties {
                        offset: "earliest".to_string(),
                    },
                },
            )
            .await?;
        let column_to_move = columns.clone();
        Ok(
            Box::pin(
                res.bytes_stream()
                    .scan(
                        Bytes::new(),
                        move |tail_bytes,
                              bytes: Result<_, _>|
                              -> futures_util::future::Ready<
                            Option<Result<Vec<Row>, CubeError>>,
                        > {
                            let rows = Self::parse_lines(tail_bytes, bytes, column_to_move.clone())
                                .map_err(|e| {
                                    CubeError::internal(format!(
                                        "Error during parsing ksql response: {}",
                                        e
                                    ))
                                });
                            futures_util::future::ready(Some(rows))
                        },
                    )
                    .ready_chunks(16384)
                    .map(move |chunks| -> Result<Vec<Row>, CubeError> {
                        let mut rows = Vec::new();
                        for chunk in chunks.into_iter() {
                            match chunk {
                                Ok(mut vec) => {
                                    rows.append(&mut vec);
                                }
                                Err(e) => {
                                    return Err(e);
                                }
                            }
                        }
                        Ok(rows)
                    }),
            ),
        )
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
        type Item = Result<Vec<Row>, CubeError>;

        fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            /* if Utc::now().signed_duration_since(self.last_readed).num_milliseconds() < 10 {
            return Poll::Pending;
            } */

            let mut res = Vec::new();

            let mut last_id = self.last_id;
            let count = rand::random::<u64>() % 200;
            for _ in 0..count {
                last_id += 1;
                let row = Row::new(vec![
                    TableValue::Int(last_id),
                    TableValue::Int(last_id % 10),
                    TableValue::Timestamp(TimestampValue::new(Utc::now().timestamp_nanos())),
                    TableValue::Int(last_id),
                ]);
                res.push(row);
            }
            unsafe {
                let self_mut = self.get_unchecked_mut();

                self_mut.last_id = last_id;
                self_mut.last_readed = Utc::now();
            }
            std::thread::sleep(Duration::from_millis(500));
            Poll::Ready(Some(Ok(res)))
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
        ) -> Result<Pin<Box<dyn Stream<Item = Result<Vec<Row>, CubeError>> + Send>>, CubeError>
        {
            Ok(Box::pin(MockRowStream::new(initial_seq_value.unwrap_or(0))))
        }

        fn validate_table_location(&self) -> Result<(), CubeError> {
            Ok(())
        }
    }
}
