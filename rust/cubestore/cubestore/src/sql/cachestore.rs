use crate::cachestore::{CacheItem, CacheStore, QueueItem};
use crate::metastore::{Column, ColumnType};

use crate::queryplanner::{QueryPlan, QueryPlanner};
use crate::sql::parser::{
    CacheCommand, CacheStoreCommand, CubeStoreParser, QueueCommand,
    Statement as CubeStoreStatement, SystemCommand,
};
use crate::sql::{QueryPlans, SqlQueryContext, SqlService};
use crate::store::DataFrame;
use crate::table::{Row, TableValue};
use crate::{app_metrics, CubeError};
use async_trait::async_trait;
use datafusion::sql::parser::Statement as DFStatement;
use log::debug;
use sqlparser::ast::Statement;
use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;

pub struct CacheStoreSqlService {
    cachestore: Arc<dyn CacheStore>,
    query_planner: Arc<dyn QueryPlanner>,
}

crate::di_service!(CacheStoreSqlService, [SqlService]);

impl CacheStoreSqlService {
    pub fn new(cachestore: Arc<dyn CacheStore>, query_planner: Arc<dyn QueryPlanner>) -> Self {
        Self {
            cachestore,
            query_planner,
        }
    }

    pub async fn exec_system_command_with_context(
        &self,
        _context: SqlQueryContext,
        command: CacheStoreCommand,
    ) -> Result<Arc<DataFrame>, CubeError> {
        match command {
            CacheStoreCommand::Compaction => {
                self.cachestore.compaction().await?;
                Ok(Arc::new(DataFrame::new(vec![], vec![])))
            }
            CacheStoreCommand::Healthcheck => {
                self.cachestore.healthcheck().await?;
                Ok(Arc::new(DataFrame::new(vec![], vec![])))
            }
        }
    }

    pub async fn exec_cache_command_with_context(
        &self,
        _context: SqlQueryContext,
        command: CacheCommand,
    ) -> Result<Arc<DataFrame>, CubeError> {
        app_metrics::CACHE_QUERIES.increment();
        let execution_time = SystemTime::now();

        let (result, track_time) = match command {
            CacheCommand::Set {
                key,
                value,
                ttl,
                nx,
            } => {
                let key = key.value;

                let success = self
                    .cachestore
                    .cache_set(CacheItem::new(key, ttl, value), nx)
                    .await?;

                (
                    Arc::new(DataFrame::new(
                        vec![Column::new("success".to_string(), ColumnType::Boolean, 0)],
                        vec![Row::new(vec![TableValue::Boolean(success)])],
                    )),
                    true,
                )
            }
            CacheCommand::Get { key } => {
                let result = self.cachestore.cache_get(key.value).await?;
                let value = if let Some(result) = result {
                    TableValue::String(result.into_row().value)
                } else {
                    TableValue::Null
                };

                (
                    Arc::new(DataFrame::new(
                        vec![Column::new("value".to_string(), ColumnType::String, 0)],
                        vec![Row::new(vec![value])],
                    )),
                    true,
                )
            }
            CacheCommand::Keys { prefix } => {
                let rows = self.cachestore.cache_keys(prefix.value).await?;
                (
                    Arc::new(DataFrame::new(
                        vec![Column::new("key".to_string(), ColumnType::String, 0)],
                        rows.iter()
                            .map(|i| Row::new(vec![TableValue::String(i.get_row().get_path())]))
                            .collect(),
                    )),
                    true,
                )
            }
            CacheCommand::Remove { key } => {
                self.cachestore.cache_delete(key.value).await?;

                (Arc::new(DataFrame::new(vec![], vec![])), true)
            }
            CacheCommand::Truncate {} => {
                self.cachestore.cache_truncate().await?;

                (Arc::new(DataFrame::new(vec![], vec![])), false)
            }
            CacheCommand::Incr { path } => {
                let row = self.cachestore.cache_incr(path.value).await?;

                (
                    Arc::new(DataFrame::new(
                        vec![Column::new("value".to_string(), ColumnType::String, 0)],
                        vec![Row::new(vec![TableValue::String(
                            row.get_row().get_value().clone(),
                        )])],
                    )),
                    true,
                )
            }
        };

        let execution_time = execution_time.elapsed()?;

        if track_time {
            app_metrics::CACHE_QUERY_TIME_MS.report(execution_time.as_millis() as i64);
        }

        debug!("Cache command processing time: {:?}", execution_time,);

        Ok(result)
    }

    pub async fn exec_queue_command_with_context(
        &self,
        _context: SqlQueryContext,
        command: QueueCommand,
    ) -> Result<Arc<DataFrame>, CubeError> {
        app_metrics::QUEUE_QUERIES.increment();
        let execution_time = SystemTime::now();

        let (result, track_time) = match command {
            QueueCommand::Add {
                key,
                priority,
                orphaned,
                value,
            } => {
                let response = self
                    .cachestore
                    .queue_add(QueueItem::new(
                        key.value,
                        value,
                        QueueItem::status_default(),
                        priority,
                        orphaned,
                    ))
                    .await?;

                (
                    Arc::new(DataFrame::new(
                        vec![
                            Column::new("id".to_string(), ColumnType::String, 0),
                            Column::new("added".to_string(), ColumnType::Boolean, 1),
                            Column::new("pending".to_string(), ColumnType::Int, 2),
                        ],
                        vec![Row::new(vec![
                            TableValue::String(response.id.to_string()),
                            TableValue::Boolean(response.added),
                            TableValue::Int(response.pending as i64),
                        ])],
                    )),
                    true,
                )
            }
            QueueCommand::Truncate {} => {
                self.cachestore.queue_truncate().await?;

                (Arc::new(DataFrame::new(vec![], vec![])), false)
            }
            QueueCommand::Cancel { key } => {
                let columns = vec![
                    Column::new("payload".to_string(), ColumnType::String, 0),
                    Column::new("extra".to_string(), ColumnType::String, 1),
                ];

                let result = self.cachestore.queue_cancel(key).await?;
                let rows = if let Some(result) = result {
                    vec![result.into_row().into_queue_cancel_row()]
                } else {
                    vec![]
                };

                (Arc::new(DataFrame::new(columns, rows)), true)
            }
            QueueCommand::Heartbeat { key } => {
                self.cachestore.queue_heartbeat(key).await?;

                (Arc::new(DataFrame::new(vec![], vec![])), true)
            }
            QueueCommand::MergeExtra { key, payload } => {
                self.cachestore.queue_merge_extra(key, payload).await?;

                (Arc::new(DataFrame::new(vec![], vec![])), true)
            }
            QueueCommand::Ack { key, result } => {
                let success = self.cachestore.queue_ack(key, result).await?;

                (
                    Arc::new(DataFrame::new(
                        vec![Column::new("success".to_string(), ColumnType::Boolean, 0)],
                        vec![Row::new(vec![TableValue::Boolean(success)])],
                    )),
                    true,
                )
            }
            QueueCommand::Get { key } => {
                let result = self.cachestore.queue_get(key).await?;
                let rows = if let Some(result) = result {
                    vec![result.into_row().into_queue_get_row()]
                } else {
                    vec![]
                };

                (
                    Arc::new(DataFrame::new(
                        vec![
                            Column::new("payload".to_string(), ColumnType::String, 0),
                            Column::new("extra".to_string(), ColumnType::String, 1),
                        ],
                        rows,
                    )),
                    true,
                )
            }
            QueueCommand::ToCancel {
                prefix,
                heartbeat_timeout,
                orphaned_timeout,
            } => {
                let rows = self
                    .cachestore
                    .queue_to_cancel(prefix.value, orphaned_timeout, heartbeat_timeout)
                    .await?;

                let columns = vec![Column::new("id".to_string(), ColumnType::String, 0)];

                (
                    Arc::new(DataFrame::new(
                        columns,
                        rows.into_iter()
                            .map(|item| {
                                Row::new(vec![TableValue::String(item.get_row().get_key().clone())])
                            })
                            .collect(),
                    )),
                    true,
                )
            }
            QueueCommand::List {
                prefix,
                with_payload,
                status_filter,
                sort_by_priority,
            } => {
                let rows = self
                    .cachestore
                    .queue_list(prefix.value, status_filter, sort_by_priority)
                    .await?;

                let mut columns = vec![
                    Column::new("id".to_string(), ColumnType::String, 0),
                    Column::new("status".to_string(), ColumnType::String, 1),
                    Column::new("extra".to_string(), ColumnType::String, 2),
                ];

                if with_payload {
                    columns.push(Column::new("payload".to_string(), ColumnType::String, 3));
                }

                (
                    Arc::new(DataFrame::new(
                        columns,
                        rows.into_iter()
                            .map(|item| item.into_row().into_queue_list_row(with_payload))
                            .collect(),
                    )),
                    true,
                )
            }
            QueueCommand::Retrieve {
                key,
                concurrency,
                extended,
            } => {
                let result = self
                    .cachestore
                    .queue_retrieve_by_path(key.value, concurrency)
                    .await?;

                (
                    Arc::new(DataFrame::new(
                        vec![
                            Column::new("payload".to_string(), ColumnType::String, 0),
                            Column::new("extra".to_string(), ColumnType::String, 1),
                            Column::new("pending".to_string(), ColumnType::Int, 2),
                            Column::new("active".to_string(), ColumnType::String, 3),
                            Column::new("id".to_string(), ColumnType::String, 4),
                        ],
                        result.into_queue_retrieve_rows(extended),
                    )),
                    true,
                )
            }
            QueueCommand::Result { key } => {
                let ack_result = self.cachestore.queue_result_by_path(key.value).await?;
                let rows = if let Some(ack_result) = ack_result {
                    vec![ack_result.into_queue_result_row()]
                } else {
                    vec![]
                };

                (
                    Arc::new(DataFrame::new(
                        vec![
                            Column::new("payload".to_string(), ColumnType::String, 0),
                            Column::new("type".to_string(), ColumnType::String, 1),
                        ],
                        rows,
                    )),
                    true,
                )
            }
            QueueCommand::ResultBlocking { timeout, key } => {
                let ack_result = self.cachestore.queue_result_blocking(key, timeout).await?;

                let rows = if let Some(ack_result) = ack_result {
                    vec![ack_result.into_queue_result_row()]
                } else {
                    vec![]
                };

                (
                    Arc::new(DataFrame::new(
                        vec![
                            Column::new("payload".to_string(), ColumnType::String, 0),
                            Column::new("type".to_string(), ColumnType::String, 1),
                        ],
                        rows,
                    )),
                    false,
                )
            }
        };

        let execution_time = execution_time.elapsed()?;

        if track_time {
            app_metrics::QUEUE_QUERY_TIME_MS.report(execution_time.as_millis() as i64);
        }

        debug!("Queue command processing time: {:?}", execution_time,);

        Ok(result)
    }
}

#[async_trait]
impl SqlService for CacheStoreSqlService {
    async fn exec_query(&self, q: &str) -> Result<Arc<DataFrame>, CubeError> {
        self.exec_query_with_context(SqlQueryContext::default(), q)
            .await
    }

    async fn exec_query_with_context(
        &self,
        ctx: SqlQueryContext,
        query: &str,
    ) -> Result<Arc<DataFrame>, CubeError> {
        let stmt = {
            let mut parser = CubeStoreParser::new(query)?;
            parser.parse_statement()?
        };

        match stmt {
            CubeStoreStatement::Statement(Statement::Query(q)) => {
                let logical_plan = self
                    .query_planner
                    .logical_plan(
                        DFStatement::Statement(Statement::Query(q)),
                        &ctx.inline_tables,
                    )
                    .await?;

                match logical_plan {
                    QueryPlan::Meta(logical_plan) => {
                        app_metrics::META_QUERIES.increment();
                        Ok(Arc::new(
                            self.query_planner.execute_meta_plan(logical_plan).await?,
                        ))
                    }
                    _ => Err(CubeError::user(format!("Unsupported SQL: '{}'", query))),
                }
            }
            CubeStoreStatement::System(command) => match command {
                SystemCommand::CacheStore(command) => {
                    self.exec_system_command_with_context(ctx, command).await
                }
                _ => Err(CubeError::user(format!("Unsupported SQL: '{}'", query))),
            },
            CubeStoreStatement::Queue(command) => {
                self.exec_queue_command_with_context(ctx, command).await
            }
            CubeStoreStatement::Cache(command) => {
                self.exec_cache_command_with_context(ctx, command).await
            }
            _ => Err(CubeError::user(format!("Unsupported SQL: '{}'", query))),
        }
    }

    async fn plan_query(&self, q: &str) -> Result<QueryPlans, CubeError> {
        self.plan_query_with_context(SqlQueryContext::default(), q)
            .await
    }

    async fn plan_query_with_context(
        &self,
        _context: SqlQueryContext,
        _query: &str,
    ) -> Result<QueryPlans, CubeError> {
        Err(CubeError::internal(
            "CacheStoreSqlService is not allowed to handle plan_query_with_context".to_string(),
        ))
    }

    async fn upload_temp_file(
        &self,
        _context: SqlQueryContext,
        _name: String,
        _file_path: &Path,
    ) -> Result<(), CubeError> {
        Err(CubeError::internal(
            "CacheStoreSqlService is not allowed to handle upload_temp_file".to_string(),
        ))
    }

    async fn temp_uploads_dir(&self, _context: SqlQueryContext) -> Result<String, CubeError> {
        Err(CubeError::internal(
            "CacheStoreSqlService is not allowed to handle temp_uploads_dir".to_string(),
        ))
    }
}
