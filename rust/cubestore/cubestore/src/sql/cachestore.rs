use crate::cachestore::{CacheItem, CacheStore, EvictionResult, QueueAddPayload, QueueItem};
use crate::metastore::{Column, ColumnType};

use crate::cluster::rate_limiter::{ProcessRateLimiter, TaskType, TraceIndex};
use crate::queryplanner::{QueryPlan, QueryPlanner};
use crate::sql::parser::{
    CacheCommand, CacheStoreCommand, CubeStoreParser, QueueCommand,
    Statement as CubeStoreStatement, SystemCommand,
};
use crate::sql::{QueryPlans, SqlQueryContext, SqlService};
use crate::store::DataFrame;
use crate::table::{Row, TableValue};
use crate::util::metrics;
use crate::{app_metrics, CubeError};
use async_trait::async_trait;
use datafusion::sql::parser::Statement as DFStatement;
use deepsize::DeepSizeOf;
use sqlparser::ast::Statement;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

pub struct CacheStoreSqlService {
    cachestore: Arc<dyn CacheStore>,
    query_planner: Arc<dyn QueryPlanner>,
    process_rate_limiter: Arc<dyn ProcessRateLimiter>,
}

crate::di_service!(CacheStoreSqlService, [SqlService]);

impl CacheStoreSqlService {
    pub fn new(
        cachestore: Arc<dyn CacheStore>,
        query_planner: Arc<dyn QueryPlanner>,
        process_rate_limiter: Arc<dyn ProcessRateLimiter>,
    ) -> Self {
        Self {
            cachestore,
            query_planner,
            process_rate_limiter,
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
            CacheStoreCommand::Info => {
                let result = self.cachestore.info().await?;
                let mut rows = vec![];

                for table in result.tables {
                    rows.push(Row::new(vec![
                        TableValue::String(format!("{}.keys_total", table.table_name)),
                        TableValue::String(table.keys_total.to_string()),
                        TableValue::Null,
                    ]));
                    rows.push(Row::new(vec![
                        TableValue::String(format!("{}.size_total", table.table_name)),
                        TableValue::String(humansize::format_size(
                            table.size_total,
                            humansize::DECIMAL,
                        )),
                        TableValue::Null,
                    ]));

                    rows.push(Row::new(vec![
                        TableValue::String(format!("{}.expired_keys_total", table.table_name)),
                        TableValue::String(table.expired_keys_total.to_string()),
                        TableValue::String("Total number of keys that expired but were not truncated via compaction.".to_string()),
                    ]));
                    rows.push(Row::new(vec![
                        TableValue::String(format!("{}.expired_size_total", table.table_name)),
                        TableValue::String(humansize::format_size(table.expired_size_total, humansize::DECIMAL)),
                        TableValue::String("Total size of keys that expired but were not truncated via compaction.".to_string()),
                    ]));

                    rows.push(Row::new(vec![
                        TableValue::String(format!("{}.min_row_size", table.table_name)),
                        TableValue::String(humansize::format_size(
                            table.min_row_size,
                            humansize::DECIMAL,
                        )),
                        TableValue::Null,
                    ]));

                    rows.push(Row::new(vec![
                        TableValue::String(format!("{}.max_row_size", table.table_name)),
                        TableValue::String(humansize::format_size(
                            table.max_row_size,
                            humansize::DECIMAL,
                        )),
                        TableValue::Null,
                    ]));

                    rows.push(Row::new(vec![
                        TableValue::String(format!("{}.avg_row_size", table.table_name)),
                        TableValue::String(humansize::format_size(
                            table.avg_row_size,
                            humansize::DECIMAL,
                        )),
                        TableValue::Null,
                    ]));
                }

                Ok(Arc::new(DataFrame::new(
                    vec![
                        Column::new("name".to_string(), ColumnType::String, 0),
                        Column::new("value".to_string(), ColumnType::String, 1),
                        Column::new("description".to_string(), ColumnType::String, 2),
                    ],
                    rows,
                )))
            }
            CacheStoreCommand::Eviction => {
                let result = self.cachestore.eviction().await?;

                Ok(Arc::new(DataFrame::new(
                    vec![
                        Column::new("name".to_string(), ColumnType::String, 0),
                        Column::new("value".to_string(), ColumnType::String, 1),
                        Column::new("description".to_string(), ColumnType::String, 2),
                    ],
                    match result {
                        EvictionResult::InProgress(status) => {
                            vec![Row::new(vec![
                                TableValue::String("status".to_string()),
                                TableValue::String(status),
                                TableValue::Null,
                            ])]
                        }
                        EvictionResult::Finished(stats) => {
                            vec![
                                Row::new(vec![
                                    TableValue::String("stats_total_keys".to_string()),
                                    TableValue::String(stats.stats_total_keys.to_string()),
                                    TableValue::Null,
                                ]),
                                Row::new(vec![
                                    TableValue::String("stats_total_raw_size".to_string()),
                                    TableValue::String(humansize::format_size(stats.stats_total_raw_size, humansize::DECIMAL)),
                                    TableValue::Null,
                                ]),
                                Row::new(vec![
                                    TableValue::String("total_keys_removed".to_string()),
                                    TableValue::String(stats.total_keys_removed.to_string()),
                                    TableValue::Null,
                                ]),
                                Row::new(vec![
                                    TableValue::String("total_size_removed".to_string()),
                                    TableValue::String(humansize::format_size(stats.total_size_removed, humansize::DECIMAL)),
                                    TableValue::Null,
                                ]),
                                Row::new(vec![
                                    TableValue::String("total_delete_skipped".to_string()),
                                    TableValue::String(stats.total_delete_skipped.to_string()),
                                    TableValue::String("Number of rows which was scheduled for deletion (from eviction), but were deleted by another process (compaction / delete)".to_string()),
                                ]),
                            ]
                        }
                    },
                )))
            }
            CacheStoreCommand::Persist => {
                self.cachestore.persist().await?;
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
        let command_tag = command.as_tag_command();
        app_metrics::CACHE_QUERIES
            .add_with_tags(1, Some(&vec![metrics::format_tag("command", command_tag)]));

        let timeout = Some(Duration::from_secs(90));
        let wait_ms = self
            .process_rate_limiter
            .wait_for_allow(TaskType::Cache, timeout)
            .await?;

        let execution_time = SystemTime::now();

        let (result, additional_traffic, track_time) = match command {
            CacheCommand::Set {
                key,
                value,
                ttl,
                nx,
            } => {
                let value_size = key.value.deep_size_of() + value.deep_size_of();
                let success = self
                    .cachestore
                    .cache_set(CacheItem::new(key.value, ttl, value), nx)
                    .await?;

                (
                    Arc::new(DataFrame::new(
                        vec![Column::new("success".to_string(), ColumnType::Boolean, 0)],
                        vec![Row::new(vec![TableValue::Boolean(success)])],
                    )),
                    Some(value_size),
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
                    None,
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
                    None,
                    true,
                )
            }
            CacheCommand::Remove { key } => {
                self.cachestore.cache_delete(key.value).await?;

                (Arc::new(DataFrame::new(vec![], vec![])), None, true)
            }
            CacheCommand::Truncate {} => {
                self.cachestore.cache_truncate().await?;

                (Arc::new(DataFrame::new(vec![], vec![])), None, false)
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
                    None,
                    true,
                )
            }
        };

        let trace_index = TraceIndex {
            // Important, it is used to aggregate all stats for cache by id
            table_id: Some(1),
            trace_obj: None,
        };
        self.process_rate_limiter
            .commit_task_usage(
                TaskType::Cache,
                (result.deep_size_of() + additional_traffic.unwrap_or(0)) as i64,
                wait_ms,
                trace_index,
            )
            .await;

        let execution_time = execution_time.elapsed()?;

        if track_time {
            app_metrics::CACHE_QUERY_TIME_MS.report(execution_time.as_millis() as i64);
        }

        log::trace!(
            "Cache {} processing time: {:?}",
            command_tag,
            execution_time
        );

        Ok(result)
    }

    pub async fn exec_queue_command_with_context(
        &self,
        _context: SqlQueryContext,
        command: QueueCommand,
    ) -> Result<Arc<DataFrame>, CubeError> {
        let command_tag = command.as_tag_command();
        app_metrics::QUEUE_QUERIES
            .add_with_tags(1, Some(&vec![metrics::format_tag("command", command_tag)]));

        let timeout = Some(Duration::from_secs(90));
        let wait_ms = self
            .process_rate_limiter
            .wait_for_allow(TaskType::Queue, timeout)
            .await?;

        let execution_time = SystemTime::now();

        let (result, additional_traffic, track_time) = match command {
            QueueCommand::Add {
                key,
                priority,
                orphaned,
                value,
            } => {
                let value_size = key.value.deep_size_of() + value.deep_size_of();
                let response = self
                    .cachestore
                    .queue_add(QueueAddPayload {
                        path: key.value,
                        value,
                        priority,
                        orphaned,
                    })
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
                    Some(value_size),
                    true,
                )
            }
            QueueCommand::Truncate {} => {
                self.cachestore.queue_truncate().await?;

                (Arc::new(DataFrame::new(vec![], vec![])), None, false)
            }
            QueueCommand::Cancel { key } => {
                let columns = vec![
                    Column::new("payload".to_string(), ColumnType::String, 0),
                    Column::new("extra".to_string(), ColumnType::String, 1),
                ];

                let result = self.cachestore.queue_cancel(key).await?;
                let rows = if let Some(result) = result {
                    vec![result.into_queue_cancel_row()]
                } else {
                    vec![]
                };

                (Arc::new(DataFrame::new(columns, rows)), None, true)
            }
            QueueCommand::Heartbeat { key } => {
                self.cachestore.queue_heartbeat(key).await?;

                (Arc::new(DataFrame::new(vec![], vec![])), None, true)
            }
            QueueCommand::MergeExtra { key, payload } => {
                let payload_size = payload.deep_size_of();
                self.cachestore.queue_merge_extra(key, payload).await?;

                (
                    Arc::new(DataFrame::new(vec![], vec![])),
                    Some(payload_size),
                    true,
                )
            }
            QueueCommand::Ack { key, result } => {
                let result_size = result.as_ref().map(|r| r.deep_size_of());
                let success = self.cachestore.queue_ack(key, result).await?;

                (
                    Arc::new(DataFrame::new(
                        vec![Column::new("success".to_string(), ColumnType::Boolean, 0)],
                        vec![Row::new(vec![TableValue::Boolean(success)])],
                    )),
                    result_size,
                    true,
                )
            }
            QueueCommand::Get { key } => {
                let result = self.cachestore.queue_get(key).await?;
                let rows = if let Some(result) = result {
                    vec![result.into_queue_get_row()]
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
                    None,
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

                let columns = vec![
                    // id is a path, we cannot change it, because it's breaking change
                    Column::new("id".to_string(), ColumnType::String, 0),
                    Column::new("queue_id".to_string(), ColumnType::String, 1),
                ];

                (
                    Arc::new(DataFrame::new(
                        columns,
                        rows.into_iter()
                            .map(|item| QueueItem::queue_to_cancel_row(item))
                            .collect(),
                    )),
                    None,
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
                    .queue_list(prefix.value, status_filter, sort_by_priority, with_payload)
                    .await?;

                let mut columns = vec![
                    // id is a path, we cannot change it, because it's breaking change
                    Column::new("id".to_string(), ColumnType::String, 0),
                    Column::new("queue_id".to_string(), ColumnType::String, 1),
                    Column::new("status".to_string(), ColumnType::String, 2),
                    Column::new("extra".to_string(), ColumnType::String, 3),
                ];

                if with_payload {
                    columns.push(Column::new("payload".to_string(), ColumnType::String, 4));
                }

                (
                    Arc::new(DataFrame::new(
                        columns,
                        rows.into_iter()
                            .map(|item| item.into_queue_list_row())
                            .collect(),
                    )),
                    None,
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
                    None,
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
                    None,
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
                    None,
                    false,
                )
            }
        };

        let trace_index = TraceIndex {
            // Important, it is used to aggregate all stats for queue by id
            table_id: Some(1),
            trace_obj: None,
        };
        self.process_rate_limiter
            .commit_task_usage(
                TaskType::Queue,
                (result.deep_size_of() + additional_traffic.unwrap_or(0)) as i64,
                wait_ms,
                trace_index,
            )
            .await;

        let execution_time = execution_time.elapsed()?;

        if track_time {
            app_metrics::QUEUE_QUERY_TIME_MS.report(execution_time.as_millis() as i64);
        }

        log::debug!(
            "Queue {} processing time: {:?}",
            command_tag,
            execution_time
        );

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
                        DFStatement::Statement(Box::new(Statement::Query(q))),
                        &ctx.inline_tables,
                        None,
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
