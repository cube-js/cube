use crate::config::injection::DIService;
use crate::config::ConfigObj;
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
use datafusion::cube_ext::ordfloat::OrdF64;
use futures::future::join_all;
use futures::stream::StreamExt;
use futures::Stream;
use itertools::{EitherOrBoth, Itertools};
use json::JsonValue;
use log::debug;
use reqwest::{Response, Url};
use serde::{Deserialize, Serialize};
use std::io::{Cursor, Write};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use warp::hyper::body::Bytes;

#[async_trait]
pub trait StreamingService: DIService + Send + Sync {
    async fn stream_table(&self, table: IdRow<Table>, location: &str) -> Result<(), CubeError>;
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

    async fn source_by(&self, location: &str) -> Result<Arc<dyn StreamingSource>, CubeError> {
        let location_url = Url::parse(location)?;
        if location_url.scheme() != "stream" {
            return Err(CubeError::internal(format!(
                "Non stream location received: {}",
                location
            )));
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
        match meta_source.get_row().source_type() {
            SourceCredentials::KSql {
                user,
                password,
                url,
            } => Ok(Arc::new(KSqlStreamingSource {
                user: user.clone(),
                password: password.clone(),
                table: location_url.path().to_string().replace("/", ""),
                endpoint_url: url.to_string(),
            })),
        }
    }
}

#[async_trait]
impl StreamingService for StreamingServiceImpl {
    async fn stream_table(&self, table: IdRow<Table>, location: &str) -> Result<(), CubeError> {
        let source = self.source_by(location).await?;
        let seq_column = table.get_row().seq_column().ok_or_else(|| {
            CubeError::internal(format!(
                "Seq column is not defined for streaming table '{}'",
                table.get_row().get_table_name()
            ))
        })?;
        let mut stream = source
            .row_stream(
                table.get_row().get_columns().clone(),
                seq_column.clone(),
                (SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_millis()
                    * 1000) as u64, // TODO store initial sequence number
            )
            .await?;

        let finish = |builders: Vec<Box<dyn ArrayBuilder>>| {
            builders.into_iter().map(|mut b| b.finish()).collect_vec()
        };

        // TODO support sealing streaming tables through ALTER TABLE
        while let Some(new_rows) = tokio::time::timeout(
            Duration::from_secs(self.config_obj.stale_stream_timeout()),
            stream.next(),
        )
        .await?
        {
            let rows = new_rows?;
            debug!("Received {} rows for {}", rows.len(), location);
            let table_cols = table.get_row().get_columns().as_slice();
            let mut builders = create_array_builders(table_cols);
            for row in rows {
                append_row(&mut builders, table_cols, &row);
            }
            let new_chunks = self
                .chunk_store
                .partition_data(
                    table.get_id(),
                    finish(builders),
                    table.get_row().get_columns().as_slice(),
                    true,
                )
                .await?;

            let new_chunk_ids: Result<Vec<u64>, CubeError> = join_all(new_chunks)
                .await
                .into_iter()
                .map(|c| Ok(c??.get_id()))
                .collect();
            self.meta_store
                .activate_chunks(table.get_id(), new_chunk_ids?)
                .await?;
        }
        Ok(())
    }
}

#[async_trait]
pub trait StreamingSource: Send + Sync {
    async fn row_stream(
        &self,
        columns: Vec<Column>,
        seq_column: Column,
        initial_seq_value: u64,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Vec<Row>, CubeError>> + Send>>, CubeError>;
}

#[derive(Clone)]
pub struct KSqlStreamingSource {
    user: Option<String>,
    password: Option<String>,
    table: String,
    endpoint_url: String,
}

#[derive(Serialize, Deserialize)]
pub struct KSqlError {
    message: String,
}

#[derive(Serialize, Deserialize)]
pub struct KSqlQuery {
    sql: String,
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
    fn parse_lines(
        tail_bytes: &mut Bytes,
        seq_value: &mut u64,
        bytes: Result<Bytes, reqwest::Error>,
        columns: Vec<Column>,
        seq_column: Column,
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
                    .filter(|c| c.get_name() != seq_column.get_name())
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
                    .zip_longest(columns.iter())
                    .map(|zip| {
                        match zip {
                            EitherOrBoth::Both(value, col) => {
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
                            }
                            EitherOrBoth::Right(col) => {
                                if col.get_name() == seq_column.get_name() {
                                    let res = TableValue::Int(*seq_value as i64);
                                    *seq_value += 1;
                                    Ok(res)
                                } else {
                                    Err(CubeError::internal(format!(
                                        "Sequence column is expected but {:?} is found",
                                        col
                                    )))
                                }
                            }
                            EitherOrBoth::Left(v) => {
                                Err(CubeError::internal(format!(
                                    "ksql source returned value {:?} that doesn't match schema columns",
                                    v
                                )))
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
            .use_rustls_tls()
            .user_agent("cubestore")
            .build()
            .unwrap();
        let mut builder = client.post(format!("{}{}", self.endpoint_url, url));
        if let Some(user) = &self.user {
            builder = builder.basic_auth(user.to_string(), self.password.clone())
        }
        let res = builder.json(&json).send().await?;
        if res.status() != 200 {
            let error = res.json::<KSqlError>().await?;
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
        seq_column: Column,
        initial_seq_value: u64,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Vec<Row>, CubeError>> + Send>>, CubeError> {
        let res = self
            .post_req(
                "/query-stream",
                &KSqlQuery {
                    sql: format!("SELECT * FROM `{}` EMIT CHANGES;", self.table),
                },
            )
            .await?;
        let column_to_move = columns.clone();
        let seq_column_to_move = seq_column.clone();
        Ok(
            Box::pin(
                res.bytes_stream()
                    .scan(
                        (Bytes::new(), initial_seq_value),
                        move |(tail_bytes, seq_value),
                              bytes: Result<_, _>|
                              -> futures_util::future::Ready<
                            Option<Result<Vec<Row>, CubeError>>,
                        > {
                            let rows = Self::parse_lines(
                                tail_bytes,
                                seq_value,
                                bytes,
                                column_to_move.clone(),
                                seq_column_to_move.clone(),
                            )
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
}
