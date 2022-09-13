use std::collections::HashMap;
use std::convert::TryFrom;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::*;
use arrow::compute::kernels::cast_utils::string_to_timestamp_nanos;
use async_trait::async_trait;
use chrono::format::Fixed::Nanosecond3;
use chrono::format::Item::{Fixed, Literal, Numeric, Space};
use chrono::format::Numeric::{Day, Hour, Minute, Month, Second, Year};
use chrono::format::Pad::Zero;
use chrono::format::Parsed;
use chrono::{DateTime, ParseResult, TimeZone, Utc};
use datafusion::cube_ext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::sql::parser::Statement as DFStatement;
use futures::future::join_all;
use hex::FromHex;
use itertools::Itertools;
use log::trace;
use rand::distributions::Uniform;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use sqlparser::ast::*;
use sqlparser::dialect::Dialect;
use tempfile::TempDir;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::time::timeout;
use tracing::instrument;
use tracing_futures::WithSubscriber;

use cubehll::HllSketch;
use parser::Statement as CubeStoreStatement;

use crate::cluster::{Cluster, JobEvent, JobResultListener};
use crate::config::injection::DIService;
use crate::config::ConfigObj;
use crate::import::limits::ConcurrencyLimits;
use crate::import::{parse_space_separated_binstring, ImportService, Ingestion};
use crate::metastore::job::JobType;
use crate::metastore::multi_index::MultiIndex;
use crate::metastore::source::SourceCredentials;
use crate::metastore::{
    is_valid_plain_binary_hll, table::Table, HllFlavour, IdRow, ImportFormat, Index, IndexDef,
    IndexType, MetaStoreTable, RowKey, Schema, TableId,
};
use crate::queryplanner::panic::PanicWorkerNode;
use crate::queryplanner::pretty_printers::{pp_phys_plan, pp_plan};
use crate::queryplanner::query_executor::{batch_to_dataframe, ClusterSendExec, QueryExecutor};
use crate::queryplanner::serialized_plan::{RowFilter, SerializedPlan};
use crate::queryplanner::{PlanningMeta, QueryPlan, QueryPlanner};
use crate::remotefs::RemoteFs;
use crate::sql::cache::SqlResultCache;
use crate::sql::parser::{CubeStoreParser, PartitionedIndexRef, SystemCommand};
use crate::store::ChunkDataStore;
use crate::table::{data, Row, TableValue, TimestampValue};
use crate::telemetry::incoming_traffic_agent_event;
use crate::util::decimal::Decimal;
use crate::util::strings::path_to_string;
use crate::CubeError;
use crate::{
    app_metrics,
    metastore::{Column, ColumnType, MetaStore},
    store::DataFrame,
};
use data::create_array_builder;
use datafusion::cube_ext::catch_unwind::async_try_with_catch_unwind;
use datafusion::physical_plan::parquet::NoopParquetMetadataCache;
use std::mem::take;

pub mod cache;
pub(crate) mod parser;

#[async_trait]
pub trait SqlService: DIService + Send + Sync {
    async fn exec_query(&self, query: &str) -> Result<Arc<DataFrame>, CubeError>;

    async fn exec_query_with_context(
        &self,
        context: SqlQueryContext,
        query: &str,
    ) -> Result<Arc<DataFrame>, CubeError>;

    /// Exposed only for tests. Worker plan created as if all partitions are on the same worker.
    async fn plan_query(&self, query: &str) -> Result<QueryPlans, CubeError>;

    /// Exposed only for tests. Worker plan created as if all partitions are on the same worker.
    async fn plan_query_with_context(
        &self,
        context: SqlQueryContext,
        query: &str,
    ) -> Result<QueryPlans, CubeError>;

    async fn upload_temp_file(
        &self,
        context: SqlQueryContext,
        name: String,
        file_path: &Path,
    ) -> Result<(), CubeError>;

    async fn temp_uploads_dir(&self, context: SqlQueryContext) -> Result<String, CubeError>;
}

pub struct QueryPlans {
    pub router: Arc<dyn ExecutionPlan>,
    pub worker: Arc<dyn ExecutionPlan>,
}

#[derive(Serialize, Deserialize, Clone, Hash, Eq, PartialEq, Debug)]
pub struct InlineTable {
    pub id: u64,
    pub name: String,
    pub data: Arc<DataFrame>,
}
pub type InlineTables = Vec<InlineTable>;

impl InlineTable {
    pub fn new(id: u64, name: String, data: Arc<DataFrame>) -> Self {
        Self { id, name, data }
    }
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct SqlQueryContext {
    pub user: Option<String>,
    pub inline_tables: InlineTables,
    pub trace_obj: Option<String>,
}

impl SqlQueryContext {
    pub fn with_user(&self, user: Option<String>) -> Self {
        let mut res = self.clone();
        res.user = user;
        res
    }

    pub fn with_inline_tables(&self, inline_tables: &InlineTables) -> Self {
        let mut res = self.clone();
        res.inline_tables = inline_tables.clone();
        res
    }

    pub fn with_trace_obj(&self, trace_obj: Option<String>) -> Self {
        let mut res = self.clone();
        res.trace_obj = trace_obj;
        res
    }
}

pub struct SqlServiceImpl {
    db: Arc<dyn MetaStore>,
    chunk_store: Arc<dyn ChunkDataStore>,
    remote_fs: Arc<dyn RemoteFs>,
    limits: Arc<ConcurrencyLimits>,
    query_planner: Arc<dyn QueryPlanner>,
    query_executor: Arc<dyn QueryExecutor>,
    cluster: Arc<dyn Cluster>,
    import_service: Arc<dyn ImportService>,
    config_obj: Arc<dyn ConfigObj>,
    rows_per_chunk: usize,
    query_timeout: Duration,
    create_table_timeout: Duration,
    cache: SqlResultCache,
}

crate::di_service!(SqlServiceImpl, [SqlService]);

impl SqlServiceImpl {
    pub fn new(
        db: Arc<dyn MetaStore>,
        chunk_store: Arc<dyn ChunkDataStore>,
        limits: Arc<ConcurrencyLimits>,
        query_planner: Arc<dyn QueryPlanner>,
        query_executor: Arc<dyn QueryExecutor>,
        cluster: Arc<dyn Cluster>,
        import_service: Arc<dyn ImportService>,
        config_obj: Arc<dyn ConfigObj>,
        remote_fs: Arc<dyn RemoteFs>,
        rows_per_chunk: usize,
        query_timeout: Duration,
        create_table_timeout: Duration,
        max_cached_queries: usize,
    ) -> Arc<SqlServiceImpl> {
        Arc::new(SqlServiceImpl {
            db,
            chunk_store,
            limits,
            query_planner,
            query_executor,
            cluster,
            import_service,
            config_obj,
            rows_per_chunk,
            query_timeout,
            create_table_timeout,
            remote_fs,
            cache: SqlResultCache::new(max_cached_queries),
        })
    }

    async fn create_schema(
        &self,
        name: String,
        if_not_exists: bool,
    ) -> Result<IdRow<Schema>, CubeError> {
        self.db.create_schema(name, if_not_exists).await
    }

    async fn create_table(
        &self,
        schema_name: String,
        table_name: String,
        columns: &Vec<ColumnDef>,
        external: bool,
        locations: Option<Vec<String>>,
        import_format: Option<ImportFormat>,
        build_range_end: Option<DateTime<Utc>>,
        indexes: Vec<Statement>,
        unique_key: Option<Vec<Ident>>,
        aggregates: Option<Vec<(Ident, Ident)>>,
        partitioned_index: Option<PartitionedIndexRef>,
        trace_obj: &Option<String>,
    ) -> Result<IdRow<Table>, CubeError> {
        let columns_to_set = convert_columns_type(columns)?;
        let mut indexes_to_create = Vec::new();
        if let Some(mut p) = partitioned_index {
            let part_index_name = match p.name.0.as_mut_slice() {
                &mut [ref schema, ref mut name] => {
                    if schema.value != schema_name {
                        return Err(CubeError::user(format!("CREATE TABLE in schema '{}' cannot reference PARTITIONED INDEX from schema '{}'", schema_name, schema)));
                    }
                    take(&mut name.value)
                }
                &mut [ref mut name] => take(&mut name.value),
                _ => {
                    return Err(CubeError::user(format!(
                        "PARTITIONED INDEX must consist of 1 or 2 identifiers, got '{}'",
                        p.name
                    )))
                }
            };

            let mut columns = Vec::new();
            for mut c in p.columns {
                columns.push(take(&mut c.value));
            }

            indexes_to_create.push(IndexDef {
                name: "#mi0".to_string(),
                columns,
                multi_index: Some(part_index_name),
                index_type: IndexType::Regular,
            });
        }

        for index in indexes.iter() {
            if let Statement::CreateIndex {
                name,
                columns,
                unique,
                ..
            } = index
            {
                indexes_to_create.push(IndexDef {
                    name: name.to_string(),
                    multi_index: None,
                    columns: columns
                        .iter()
                        .map(|c| {
                            if let Expr::Identifier(ident) = &c.expr {
                                Ok(ident.value.to_string())
                            } else {
                                Err(CubeError::internal(format!(
                                    "Unexpected column expression: {:?}",
                                    c.expr
                                )))
                            }
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                    index_type: if *unique {
                        IndexType::Aggregate
                    } else {
                        IndexType::Regular
                    },
                });
            }
        }

        if !external {
            return self
                .db
                .create_table(
                    schema_name,
                    table_name,
                    columns_to_set,
                    None,
                    None,
                    indexes_to_create,
                    true,
                    build_range_end,
                    unique_key.map(|keys| keys.iter().map(|c| c.value.to_string()).collect()),
                    aggregates.map(|keys| {
                        keys.iter()
                            .map(|c| (c.0.value.to_string(), c.1.value.to_string()))
                            .collect()
                    }),
                    None,
                )
                .await;
        }

        let listener = self.cluster.job_result_listener();

        let partition_split_threshold = if let Some(locations) = locations.as_ref() {
            let size = join_all(
                locations
                    .iter()
                    .map(|location| {
                        let location = location.to_string();
                        let import_service = self.import_service.clone();
                        return async move {
                            import_service.estimate_location_row_count(&location).await
                        };
                    })
                    .collect::<Vec<_>>(),
            )
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .sum::<u64>();

            let mut sel_workers_count = self.config_obj.select_workers().len() as u64;
            if sel_workers_count == 0 {
                sel_workers_count = 1;
            }
            let threshold = (size / sel_workers_count)
                .min(self.config_obj.max_partition_split_threshold())
                .max(self.config_obj.partition_split_threshold());

            Some(threshold)
        } else {
            None
        };

        let table = self
            .db
            .create_table(
                schema_name,
                table_name,
                columns_to_set,
                locations,
                import_format,
                indexes_to_create,
                false,
                build_range_end,
                unique_key.map(|keys| keys.iter().map(|c| c.value.to_string()).collect()),
                aggregates.map(|keys| {
                    keys.iter()
                        .map(|c| (c.0.value.to_string(), c.1.value.to_string()))
                        .collect()
                }),
                partition_split_threshold,
            )
            .await?;

        let finalize_res = tokio::time::timeout(
            self.create_table_timeout,
            self.finalize_external_table(&table, listener, trace_obj),
        )
        .await
        .map_err(|_| {
            CubeError::internal(format!(
                "Timeout during create table finalization: {:?}",
                table
            ))
        })
        .flatten();
        if let Err(e) = finalize_res {
            if let Err(inner) = self.db.drop_table(table.get_id()).await {
                log::error!(
                    "Drop table ({}) after error failed: {}",
                    table.get_id(),
                    inner
                );
            }
            return Err(e);
        }
        Ok(table)
    }

    async fn create_partitioned_index(
        &self,
        schema: String,
        name: String,
        columns: Vec<ColumnDef>,
        if_not_exists: bool,
    ) -> Result<IdRow<MultiIndex>, CubeError> {
        let columns = convert_columns_type(&columns)?;
        self.db
            .create_partitioned_index(schema, name, columns, if_not_exists)
            .await
    }

    async fn finalize_external_table(
        &self,
        table: &IdRow<Table>,
        listener: JobResultListener,
        trace_obj: &Option<String>,
    ) -> Result<(), CubeError> {
        let wait_for = table
            .get_row()
            .locations()
            .unwrap()
            .iter()
            .filter(|&l| !Table::is_stream_location(l))
            .map(|&l| {
                (
                    RowKey::Table(TableId::Tables, table.get_id()),
                    JobType::TableImportCSV(l.clone()),
                )
            })
            .collect();
        let imports = listener.wait_for_job_results(wait_for).await?;
        for r in imports {
            if let JobEvent::Error(_, _, e) = r {
                return Err(CubeError::user(format!("Create table failed: {}", e)));
            }
        }

        let mut futures = Vec::new();
        let indexes = self.db.get_table_indexes(table.get_id()).await?;
        let partitions = self
            .db
            .get_active_partitions_and_chunks_by_index_id_for_select(
                indexes.iter().map(|i| i.get_id()).collect(),
            )
            .await?;
        // Omit warming up chunks as those shouldn't affect select times much however will affect
        // warming up time a lot in case of big tables when a lot of chunks pending for repartition
        for (partition, _) in partitions.into_iter().flatten() {
            futures.push(self.cluster.warmup_partition(partition, Vec::new()));
        }
        join_all(futures)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

        let ready_table = self.db.table_ready(table.get_id(), true).await?;

        if let Some(trace_obj) = trace_obj.as_ref() {
            incoming_traffic_agent_event(trace_obj, ready_table.get_row().total_download_size())?;
        }

        Ok(())
    }

    async fn create_index(
        &self,
        schema_name: String,
        table_name: String,
        name: String,
        columns: &Vec<Ident>,
    ) -> Result<IdRow<Index>, CubeError> {
        Ok(self
            .db
            .create_index(
                schema_name,
                table_name,
                IndexDef {
                    name,
                    multi_index: None,
                    columns: columns.iter().map(|c| c.value.to_string()).collect(),
                    index_type: IndexType::Regular, //TODO realize aggregate index here too
                },
            )
            .await?)
    }

    async fn insert_data<'a>(
        &'a self,
        schema_name: String,
        table_name: String,
        columns: &'a Vec<Ident>,
        data: &'a Vec<Vec<Expr>>,
    ) -> Result<u64, CubeError> {
        let table = self
            .db
            .get_table(schema_name.clone(), table_name.clone())
            .await?;
        let table_columns = table.get_row().clone();
        let table_columns = table_columns.get_columns();
        let mut real_col: Vec<&Column> = Vec::new();
        for column in columns {
            let c = if let Some(item) = table_columns
                .iter()
                .find(|voc| *voc.get_name() == column.value)
            {
                item
            } else {
                return Err(CubeError::user(format!(
                    "Column {} is not present in table {}.{}.",
                    column.value, schema_name, table_name
                )));
            };
            real_col.push(c);
        }

        let mut ingestion = Ingestion::new(
            self.db.clone(),
            self.chunk_store.clone(),
            self.limits.clone(),
            table.clone(),
        );
        for rows_chunk in data.chunks(self.rows_per_chunk) {
            let rows = parse_chunk(rows_chunk, &real_col)?;
            ingestion.queue_data_frame(rows).await?;
        }
        ingestion.wait_completion().await?;
        Ok(data.len() as u64)
    }

    async fn dump_select_inputs(
        &self,
        query: &str,
        q: Box<Query>,
    ) -> Result<Arc<DataFrame>, CubeError> {
        // TODO: metastore snapshot must be consistent wrt the dumped data.
        let logical_plan = self
            .query_planner
            .logical_plan(
                DFStatement::Statement(Statement::Query(q)),
                &InlineTables::new(),
            )
            .await?;

        let mut dump_dir = PathBuf::from(&self.remote_fs.local_path().await);
        dump_dir.push("dumps");
        tokio::fs::create_dir_all(&dump_dir).await?;

        let dump_dir = TempDir::new_in(&dump_dir)?.into_path();
        let meta_dir = path_to_string(dump_dir.join("metastore-backup"))?;

        log::debug!("Dumping metastore to {}", meta_dir);
        self.db.debug_dump(meta_dir).await?;

        match logical_plan {
            QueryPlan::Select(p, _) => {
                let data_dir = dump_dir.join("data");
                tokio::fs::create_dir(&data_dir).await?;
                log::debug!("Dumping data files to {:?}", data_dir);
                // TODO: download in parallel.
                for (_, f, size) in p.all_required_files() {
                    let f = self.remote_fs.download_file(&f, size).await?;
                    let name = Path::new(&f).file_name().ok_or_else(|| {
                        CubeError::internal(format!("Could not get filename of '{}'", f))
                    })?;
                    tokio::fs::copy(&f, data_dir.join(&name)).await?;
                }
            }
            QueryPlan::Meta(_) => {}
        }

        let query_file = dump_dir.join("query.sql");
        File::create(query_file)
            .await?
            .write_all(query.as_bytes())
            .await?;

        log::debug!("Wrote debug dump to {:?}", dump_dir);

        let dump_dir = path_to_string(dump_dir)?;
        let columns = vec![Column::new("dump_path".to_string(), ColumnType::String, 0)];
        Ok(Arc::new(DataFrame::new(
            columns,
            vec![Row::new(vec![TableValue::String(dump_dir)])],
        )))
    }

    async fn explain(
        &self,
        statement: Statement,
        analyze: bool,
    ) -> Result<Arc<DataFrame>, CubeError> {
        fn extract_worker_plans(
            p: &Arc<dyn ExecutionPlan>,
        ) -> Option<Vec<(String, SerializedPlan)>> {
            if let Some(p) = p.as_any().downcast_ref::<ClusterSendExec>() {
                Some(p.worker_plans())
            } else {
                for c in p.children() {
                    let res = extract_worker_plans(&c);
                    if res.is_some() {
                        return res;
                    }
                }
                None
            }
        }

        let query_plan = self
            .query_planner
            .logical_plan(DFStatement::Statement(statement), &InlineTables::new())
            .await?;
        let res = match query_plan {
            QueryPlan::Select(serialized, _) => {
                let res = if !analyze {
                    let logical_plan = serialized.logical_plan(
                        HashMap::new(),
                        HashMap::new(),
                        NoopParquetMetadataCache::new(),
                    )?;

                    DataFrame::new(
                        vec![Column::new(
                            "logical plan".to_string(),
                            ColumnType::String,
                            0,
                        )],
                        vec![Row::new(vec![TableValue::String(pp_plan(&logical_plan))])],
                    )
                } else {
                    let cluster = self.cluster.clone();
                    let executor = self.query_executor.clone();
                    let headers: Vec<Column> = vec![
                        Column::new("node type".to_string(), ColumnType::String, 0),
                        Column::new("node name".to_string(), ColumnType::String, 1),
                        Column::new("physical plan".to_string(), ColumnType::String, 2),
                    ];
                    let mut rows = Vec::new();

                    let router_plan = executor.router_plan(serialized.clone(), cluster).await?.0;
                    rows.push(Row::new(vec![
                        TableValue::String("router".to_string()),
                        TableValue::String("".to_string()),
                        TableValue::String(pp_phys_plan(router_plan.as_ref())),
                    ]));

                    if let Some(worker_plans) = extract_worker_plans(&router_plan) {
                        let worker_futures = worker_plans
                            .into_iter()
                            .map(|(name, plan)| async move {
                                self.cluster
                                    .run_explain_analyze(&name, plan.clone())
                                    .await
                                    .map(|p| (name, p))
                            })
                            .collect::<Vec<_>>();
                        join_all(worker_futures)
                            .await
                            .into_iter()
                            .collect::<Result<Vec<_>, _>>()?
                            .into_iter()
                            .for_each(|(name, pp_plan)| {
                                rows.push(Row::new(vec![
                                    TableValue::String("worker".to_string()),
                                    TableValue::String(name.to_string()),
                                    TableValue::String(pp_plan),
                                ]));
                            });
                    }

                    DataFrame::new(headers, rows)
                };
                Ok(res)
            }
            _ => Err(CubeError::user(
                "Explain not supported for selects from system tables".to_string(),
            )),
        }?;
        Ok(Arc::new(res))
    }
}

#[derive(Debug)]
pub struct MySqlDialectWithBackTicks {}

impl Dialect for MySqlDialectWithBackTicks {
    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '"' || ch == '`'
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        // See https://dev.mysql.com/doc/refman/8.0/en/identifiers.html.
        // We don't yet support identifiers beginning with numbers, as that
        // makes it hard to distinguish numeric literals.
        (ch >= 'a' && ch <= 'z')
            || (ch >= 'A' && ch <= 'Z')
            || ch == '_'
            || ch == '$'
            || (ch >= '\u{0080}' && ch <= '\u{ffff}')
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        self.is_identifier_start(ch) || (ch >= '0' && ch <= '9')
    }
}

#[async_trait]
impl SqlService for SqlServiceImpl {
    async fn exec_query(&self, q: &str) -> Result<Arc<DataFrame>, CubeError> {
        self.exec_query_with_context(SqlQueryContext::default(), q)
            .await
    }

    #[instrument(level = "trace", skip(self))]
    async fn exec_query_with_context(
        &self,
        context: SqlQueryContext,
        query: &str,
    ) -> Result<Arc<DataFrame>, CubeError> {
        if !query.to_lowercase().starts_with("insert") && !query.to_lowercase().contains("password")
        {
            trace!("Query: '{}'", query);
        }
        if let Some(data_frame) = SqlServiceImpl::handle_workbench_queries(query) {
            return Ok(Arc::new(data_frame));
        }
        let ast = {
            let mut parser = CubeStoreParser::new(query)?;
            parser.parse_statement()?
        };
        // trace!("AST is: {:?}", ast);
        match ast {
            CubeStoreStatement::Statement(Statement::ShowVariable { variable }) => {
                if variable.len() != 1 {
                    return Err(CubeError::user(format!(
                        "Only one variable supported in SHOW, but got {}",
                        variable.len()
                    )));
                }
                match variable[0].value.to_lowercase() {
                    s if s == "schemas" => {
                        Ok(Arc::new(DataFrame::from(self.db.get_schemas().await?)))
                    }
                    s if s == "tables" => {
                        Ok(Arc::new(DataFrame::from(self.db.get_tables().await?)))
                    }
                    s if s == "chunks" => Ok(Arc::new(DataFrame::from(
                        self.db.chunks_table().all_rows().await?,
                    ))),
                    s if s == "indexes" => Ok(Arc::new(DataFrame::from(
                        self.db.index_table().all_rows().await?,
                    ))),
                    s if s == "partitions" => Ok(Arc::new(DataFrame::from(
                        self.db.partition_table().all_rows().await?,
                    ))),
                    x => Err(CubeError::user(format!("Unknown SHOW: {}", x))),
                }
            }
            CubeStoreStatement::System(command) => match command {
                SystemCommand::KillAllJobs => {
                    self.db.delete_all_jobs().await?;
                    Ok(Arc::new(DataFrame::new(vec![], vec![])))
                }
                SystemCommand::Repartition { partition_id } => {
                    let partition = self.db.get_partition(partition_id).await?;
                    self.cluster.schedule_repartition(&partition).await?;
                    Ok(Arc::new(DataFrame::new(vec![], vec![])))
                }
                SystemCommand::PanicWorker => {
                    let cluster = self.cluster.clone();
                    let workers = self.config_obj.select_workers();
                    let plan = SerializedPlan::try_new(
                        PanicWorkerNode {}.into_plan(),
                        PlanningMeta {
                            indices: Vec::new(),
                            multi_part_subtree: HashMap::new(),
                        },
                    )
                    .await?;
                    if workers.len() == 0 {
                        let executor = self.query_executor.clone();
                        match async_try_with_catch_unwind(
                            executor.execute_router_plan(plan, cluster),
                        )
                        .await
                        {
                            Ok(result) => result,
                            Err(panic) => Err(CubeError::from(panic)),
                        }?;
                    } else {
                        let worker = &workers[0];
                        cluster.run_select(worker, plan).await?;
                    }
                    panic!("worker did not panic")
                }
            },
            CubeStoreStatement::Statement(Statement::SetVariable { .. }) => {
                Ok(Arc::new(DataFrame::new(vec![], vec![])))
            }
            CubeStoreStatement::CreateSchema {
                schema_name,
                if_not_exists,
            } => {
                let name = schema_name.to_string();
                let res = self.create_schema(name, if_not_exists).await?;
                Ok(Arc::new(DataFrame::from(vec![res])))
            }
            CubeStoreStatement::CreateTable {
                create_table:
                    Statement::CreateTable {
                        name,
                        columns,
                        external,
                        with_options,
                        ..
                    },
                indexes,
                aggregates,
                locations,
                unique_key,
                partitioned_index,
            } => {
                let nv = &name.0;
                if nv.len() != 2 {
                    return Err(CubeError::user(format!(
                        "Schema's name should be present in table name but found: {}",
                        name
                    )));
                }
                let schema_name = &nv[0].value;
                let table_name = &nv[1].value;
                let import_format = with_options
                    .iter()
                    .find(|&opt| opt.name.value == "input_format")
                    .map_or(Result::Ok(ImportFormat::CSV), |option| {
                        match &option.value {
                            Value::SingleQuotedString(input_format) => {
                                match input_format.as_str() {
                                    "csv" => Result::Ok(ImportFormat::CSV),
                                    "csv_no_header" => Result::Ok(ImportFormat::CSVNoHeader),
                                    _ => Result::Err(CubeError::user(format!(
                                        "Bad input_format {}",
                                        option.value
                                    ))),
                                }
                            }
                            _ => Result::Err(CubeError::user(format!(
                                "Bad input format {}",
                                option.value
                            ))),
                        }
                    })?;
                let build_range_end = with_options
                    .iter()
                    .find(|&opt| opt.name.value == "build_range_end")
                    .map_or(Result::Ok(None), |option| match &option.value {
                        Value::SingleQuotedString(build_range_end) => {
                            let ts = timestamp_from_string(build_range_end)?;
                            let utc = Utc.timestamp_nanos(ts.get_time_stamp());
                            Result::Ok(Some(utc))
                        }
                        _ => Result::Err(CubeError::user(format!(
                            "Bad build_range_end {}",
                            option.value
                        ))),
                    })?;

                let res = self
                    .create_table(
                        schema_name.clone(),
                        table_name.clone(),
                        &columns,
                        external,
                        locations,
                        Some(import_format),
                        build_range_end,
                        indexes,
                        unique_key,
                        aggregates,
                        partitioned_index,
                        &context.trace_obj,
                    )
                    .await?;
                Ok(Arc::new(DataFrame::from(vec![res])))
            }
            CubeStoreStatement::Statement(Statement::CreateIndex {
                name,
                table_name,
                columns,
                ..
            }) => {
                if table_name.0.len() != 2 {
                    return Err(CubeError::user(format!(
                        "Schema's name should be present in table name but found: {}",
                        table_name
                    )));
                }
                let schema_name = &table_name.0[0].value;
                let table_name = &table_name.0[1].value;
                let res = self
                    .create_index(
                        schema_name.to_string(),
                        table_name.to_string(),
                        name.to_string(),
                        &columns
                            .iter()
                            .map(|c| -> Result<_, _> {
                                if let Expr::Identifier(ident) = &c.expr {
                                    Ok(ident.clone())
                                } else {
                                    Err(CubeError::user(format!(
                                        "Unsupported column expression in index: {:?}",
                                        c.expr
                                    )))
                                }
                            })
                            .collect::<Result<Vec<_>, _>>()?,
                    )
                    .await?;
                Ok(Arc::new(DataFrame::from(vec![res])))
            }
            CubeStoreStatement::CreateSource {
                name,
                source_type,
                credentials,
                or_update,
            } => {
                if or_update {
                    let creds = match source_type.as_str() {
                        "ksql" => {
                            let user = credentials
                                .iter()
                                .find(|o| o.name.value == "user")
                                .and_then(|x| {
                                    if let Value::SingleQuotedString(v) = &x.value {
                                        Some(v.to_string())
                                    } else {
                                        None
                                    }
                                });
                            let password = credentials
                                .iter()
                                .find(|o| o.name.value == "password")
                                .and_then(|x| {
                                    if let Value::SingleQuotedString(v) = &x.value {
                                        Some(v.to_string())
                                    } else {
                                        None
                                    }
                                });
                            let url =
                                credentials
                                    .iter()
                                    .find(|o| o.name.value == "url")
                                    .and_then(|x| {
                                        if let Value::SingleQuotedString(v) = &x.value {
                                            Some(v.to_string())
                                        } else {
                                            None
                                        }
                                    });
                            Ok(SourceCredentials::KSql {
                                user,
                                password,
                                url: url.ok_or(CubeError::user(
                                    "url is required as credential for ksql source".to_string(),
                                ))?,
                            })
                        }
                        x => Err(CubeError::user(format!("Not supported stream type: {}", x))),
                    };
                    let source = self
                        .db
                        .create_or_update_source(name.value.to_string(), creds?)
                        .await?;
                    Ok(Arc::new(DataFrame::from(vec![source])))
                } else {
                    Err(CubeError::user(
                        "CREATE SOURCE OR UPDATE should be used instead".to_string(),
                    ))
                }
            }
            CubeStoreStatement::Statement(Statement::CreatePartitionedIndex {
                name,
                columns,
                if_not_exists,
            }) => {
                if name.0.len() != 2 {
                    return Err(CubeError::user(format!(
                        "Expected name for PARTITIONED INDEX in the form '<SCHEMA>.<INDEX>', found: {}",
                        name
                    )));
                }
                let schema = &name.0[0].value;
                let index = &name.0[1].value;
                let res = self
                    .create_partitioned_index(
                        schema.to_string(),
                        index.to_string(),
                        columns,
                        if_not_exists,
                    )
                    .await?;
                Ok(Arc::new(DataFrame::from(vec![res])))
            }
            CubeStoreStatement::Statement(Statement::Drop {
                object_type, names, ..
            }) => {
                match object_type {
                    ObjectType::Schema => {
                        self.db.delete_schema(names[0].to_string()).await?;
                    }
                    ObjectType::Table => {
                        let table = self
                            .db
                            .get_table(names[0].0[0].to_string(), names[0].0[1].to_string())
                            .await?;
                        self.db.drop_table(table.get_id()).await?;
                    }
                    ObjectType::PartitionedIndex => {
                        let schema = names[0].0[0].value.clone();
                        let name = names[0].0[1].value.clone();
                        self.db.drop_partitioned_index(schema, name).await?;
                    }
                    _ => return Err(CubeError::user("Unsupported drop operation".to_string())),
                }
                Ok(Arc::new(DataFrame::new(vec![], vec![])))
            }
            CubeStoreStatement::Statement(Statement::Insert {
                table_name,
                columns,
                source,
                ..
            }) => {
                let data = if let SetExpr::Values(Values(data_series)) = &source.body {
                    data_series
                } else {
                    return Err(CubeError::user(format!(
                        "Data should be present in query. Your query was '{}'",
                        query
                    )));
                };

                let nv = &table_name.0;
                if nv.len() != 2 {
                    return Err(CubeError::user(format!("Schema's name should be present in query (boo.table1). Your query was '{}'", query)));
                }
                let schema_name = &nv[0].value;
                let table_name = &nv[1].value;

                self.insert_data(schema_name.clone(), table_name.clone(), &columns, data)
                    .await?;
                Ok(Arc::new(DataFrame::new(vec![], vec![])))
            }
            CubeStoreStatement::Statement(Statement::Query(q)) => {
                let logical_plan = self
                    .query_planner
                    .logical_plan(
                        DFStatement::Statement(Statement::Query(q)),
                        &context.inline_tables,
                    )
                    .await?;
                // TODO distribute and combine
                let res = match logical_plan {
                    QueryPlan::Meta(logical_plan) => {
                        app_metrics::META_QUERIES.increment();
                        Arc::new(self.query_planner.execute_meta_plan(logical_plan).await?)
                    }
                    QueryPlan::Select(serialized, workers) => {
                        app_metrics::DATA_QUERIES.increment();
                        let cluster = self.cluster.clone();
                        let executor = self.query_executor.clone();
                        timeout(
                            self.query_timeout,
                            self.cache
                                .get(
                                    query,
                                    &context.inline_tables,
                                    serialized,
                                    async move |plan| {
                                        let records;
                                        if workers.len() == 0 {
                                            records = executor
                                                .execute_router_plan(plan, cluster)
                                                .await?
                                                .1;
                                        } else {
                                            // Pick one of the workers to run as main for the request.
                                            let i =
                                                thread_rng().sample(Uniform::new(0, workers.len()));
                                            let rs =
                                                cluster.route_select(&workers[i], plan).await?.1;
                                            records = rs
                                                .into_iter()
                                                .map(|r| r.read())
                                                .collect::<Result<Vec<_>, _>>()?;
                                        }
                                        Ok(cube_ext::spawn_blocking(
                                            move || -> Result<DataFrame, CubeError> {
                                                let df = batch_to_dataframe(&records)?;
                                                Ok(df)
                                            },
                                        )
                                        .await??)
                                    },
                                )
                                .with_current_subscriber(),
                        )
                        .await??
                    }
                };
                Ok(res)
            }
            CubeStoreStatement::Statement(Statement::Explain {
                analyze,
                verbose: _,
                statement,
            }) => match *statement {
                Statement::Query(q) => self.explain(Statement::Query(q.clone()), analyze).await,
                _ => Err(CubeError::user(format!(
                    "Unsupported explain request: '{}'",
                    query
                ))),
            },

            CubeStoreStatement::Dump(q) => self.dump_select_inputs(query, q).await,
            _ => Err(CubeError::user(format!("Unsupported SQL: '{}'", query))),
        }
    }

    async fn plan_query(&self, q: &str) -> Result<QueryPlans, CubeError> {
        self.plan_query_with_context(SqlQueryContext::default(), q)
            .await
    }

    async fn plan_query_with_context(
        &self,
        context: SqlQueryContext,
        q: &str,
    ) -> Result<QueryPlans, CubeError> {
        let ast = {
            let replaced_quote = q.replace("\\'", "''");
            let mut parser = CubeStoreParser::new(&replaced_quote)?;
            parser.parse_statement()?
        };
        match ast {
            CubeStoreStatement::Statement(Statement::Query(q)) => {
                let logical_plan = self
                    .query_planner
                    .logical_plan(
                        DFStatement::Statement(Statement::Query(q)),
                        &context.inline_tables,
                    )
                    .await?;
                match logical_plan {
                    QueryPlan::Select(router_plan, _) => {
                        // For tests, pretend we have all partitions on the same worker.
                        let worker_plan = router_plan.with_partition_id_to_execute(
                            router_plan
                                .index_snapshots()
                                .iter()
                                .flat_map(|i| {
                                    i.partitions
                                        .iter()
                                        .map(|p| (p.partition.get_id(), RowFilter::default()))
                                })
                                .collect(),
                            context.inline_tables.into_iter().map(|i| i.id).collect(),
                        );
                        let mut mocked_names = HashMap::new();
                        for (_, f, _) in worker_plan.files_to_download() {
                            let name = self.remote_fs.local_file(&f).await?;
                            mocked_names.insert(f, name);
                        }
                        let chunk_ids_to_batches = worker_plan
                            .in_memory_chunks_to_load()
                            .into_iter()
                            .map(|c| (c.get_id(), Vec::new()))
                            .collect();
                        return Ok(QueryPlans {
                            router: self
                                .query_executor
                                .router_plan(router_plan, self.cluster.clone())
                                .await?
                                .0,
                            worker: self
                                .query_executor
                                .worker_plan(worker_plan, mocked_names, chunk_ids_to_batches)
                                .await?
                                .0,
                        });
                    }
                    QueryPlan::Meta(_) => {
                        return Err(CubeError::internal(
                            "plan_query only works for data selects".to_string(),
                        ))
                    }
                };
            }
            _ => {
                return Err(CubeError::internal(
                    "plan_query only works for data selects".to_string(),
                ))
            }
        }
    }

    async fn upload_temp_file(
        &self,
        _context: SqlQueryContext,
        name: String,
        file_path: &Path,
    ) -> Result<(), CubeError> {
        // TODO persist file size
        self.remote_fs
            .upload_file(
                file_path.to_string_lossy().as_ref(),
                &format!("temp-uploads/{}", name),
            )
            .await?;
        Ok(())
    }

    async fn temp_uploads_dir(&self, _context: SqlQueryContext) -> Result<String, CubeError> {
        self.remote_fs.uploads_dir().await
    }
}

fn convert_columns_type(columns: &Vec<ColumnDef>) -> Result<Vec<Column>, CubeError> {
    let mut rolupdb_columns = Vec::new();

    for (i, col) in columns.iter().enumerate() {
        let cube_col = Column::new(
            col.name.value.clone(),
            match &col.data_type {
                DataType::Date
                | DataType::Time
                | DataType::Char(_)
                | DataType::Varchar(_)
                | DataType::Clob(_)
                | DataType::Text
                | DataType::String => ColumnType::String,
                DataType::Uuid
                | DataType::Binary(_)
                | DataType::Varbinary(_)
                | DataType::Blob(_)
                | DataType::Bytea
                | DataType::Array(_) => ColumnType::Bytes,
                DataType::Decimal(precision, scale) => {
                    let mut precision = precision.unwrap_or(18);
                    let mut scale = scale.unwrap_or(5);
                    if precision > 18 {
                        precision = 18;
                    }
                    if scale > 5 {
                        scale = 10;
                    }
                    if scale > precision {
                        precision = scale;
                    }
                    ColumnType::Decimal {
                        precision: precision as i32,
                        scale: scale as i32,
                    }
                }
                DataType::SmallInt | DataType::Int | DataType::BigInt | DataType::Interval => {
                    ColumnType::Int
                }
                DataType::Boolean => ColumnType::Boolean,
                DataType::Float(_) | DataType::Real | DataType::Double => ColumnType::Float,
                DataType::Timestamp => ColumnType::Timestamp,
                DataType::Custom(custom) => {
                    let custom_type_name = custom.to_string().to_lowercase();
                    match custom_type_name.as_str() {
                        "mediumint" => ColumnType::Int,
                        "bytes" => ColumnType::Bytes,
                        "varbinary" => ColumnType::Bytes,
                        "hyperloglog" => ColumnType::HyperLogLog(HllFlavour::Airlift),
                        "hyperloglogpp" => ColumnType::HyperLogLog(HllFlavour::ZetaSketch),
                        "hll_snowflake" => ColumnType::HyperLogLog(HllFlavour::Snowflake),
                        "hll_postgres" => ColumnType::HyperLogLog(HllFlavour::Postgres),
                        _ => {
                            return Err(CubeError::user(format!(
                                "Custom type '{}' is not supported",
                                custom
                            )))
                        }
                    }
                }
                DataType::Regclass => {
                    return Err(CubeError::user(
                        "Type 'RegClass' is not suppored.".to_string(),
                    ));
                }
            },
            i,
        );
        rolupdb_columns.push(cube_col);
    }
    Ok(rolupdb_columns)
}

fn parse_chunk(chunk: &[Vec<Expr>], column: &Vec<&Column>) -> Result<Vec<ArrayRef>, CubeError> {
    let mut buffer = Vec::new();
    let mut builders = column
        .iter()
        .map(|c| create_array_builder(c.get_column_type()))
        .collect_vec();
    for r in chunk {
        for i in 0..r.len() {
            extract_data(&r[i], &column[i], &mut buffer, builders[i].as_mut())?;
        }
    }
    let mut order = (0..column.len()).collect_vec();
    order.sort_unstable_by_key(|i| column[*i].get_index());

    let mut arrays = Vec::with_capacity(builders.len());
    for i in order {
        let b = &mut builders[i];
        let a = b.finish();
        assert_eq!(
            a.len(),
            chunk.len(),
            "invalid number of rows: {} in array vs {} in input data. array: {:?}",
            a.len(),
            chunk.len(),
            a
        );
        arrays.push(a);
    }
    Ok(arrays)
}

fn parse_hyper_log_log<'a>(
    buffer: &'a mut Vec<u8>,
    v: &'a Value,
    f: HllFlavour,
) -> Result<&'a [u8], CubeError> {
    match f {
        HllFlavour::Snowflake => {
            let str = if let Value::SingleQuotedString(str) = v {
                str
            } else {
                return Err(CubeError::user(format!(
                    "Single quoted string is expected but {:?} found",
                    v
                )));
            };
            let hll = HllSketch::read_snowflake(str)?;
            *buffer = hll.write();
            Ok(buffer)
        }
        HllFlavour::Postgres => {
            let bytes = parse_binary_string(buffer, v)?;
            let hll = HllSketch::read_hll_storage_spec(bytes)?;
            *buffer = hll.write();
            Ok(buffer)
        }
        HllFlavour::Airlift | HllFlavour::ZetaSketch => {
            let bytes = parse_binary_string(buffer, v)?;
            is_valid_plain_binary_hll(bytes, f)?;
            Ok(bytes)
        }
    }
}

fn parse_binary_string<'a>(buffer: &'a mut Vec<u8>, v: &'a Value) -> Result<&'a [u8], CubeError> {
    match v {
        Value::Number(s, _) => Ok(s.as_bytes()),
        // We interpret strings of the form '0f 0a 14 ff' as a list of hex-encoded bytes.
        // MySQL will store bytes of the string itself instead and we should do the same.
        // TODO: Ensure CubeJS does not send strings of this form our way and match MySQL behavior.
        Value::SingleQuotedString(s) => parse_space_separated_binstring(buffer, s.as_ref()),
        // TODO: allocate directly on arena.
        Value::HexStringLiteral(s) => {
            *buffer = Vec::from_hex(s.as_bytes())?;
            Ok(buffer.as_slice())
        }
        _ => Err(CubeError::user(format!(
            "cannot convert value to binary string: {}",
            v
        ))),
    }
}

fn extract_data<'a>(
    cell: &'a Expr,
    column: &Column,
    buffer: &'a mut Vec<u8>,
    builder: &mut dyn ArrayBuilder,
) -> Result<(), CubeError> {
    let is_null = match cell {
        Expr::Value(Value::Null) => true,
        _ => false,
    };
    match column.get_column_type() {
        ColumnType::String => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .unwrap();
            if is_null {
                builder.append_null()?;
                return Ok(());
            }
            let val = if let Expr::Value(Value::SingleQuotedString(v)) = cell {
                v
            } else {
                return Err(CubeError::user(format!(
                    "Single quoted string is expected but {:?} found",
                    cell
                )));
            };
            builder.append_value(val)?;
        }
        ColumnType::Int => {
            let builder = builder.as_any_mut().downcast_mut::<Int64Builder>().unwrap();
            if is_null {
                builder.append_null()?;
                return Ok(());
            }
            let val_int = match cell {
                Expr::Value(Value::Number(v, _)) | Expr::Value(Value::SingleQuotedString(v)) => {
                    v.parse::<i64>()
                }
                Expr::UnaryOp {
                    op: UnaryOperator::Minus,
                    expr,
                } => {
                    if let Expr::Value(Value::Number(v, _)) = expr.as_ref() {
                        v.parse::<i64>().map(|v| v * -1)
                    } else {
                        return Err(CubeError::user(format!("Can't parse int from, {:?}", cell)));
                    }
                }
                _ => return Err(CubeError::user(format!("Can't parse int from, {:?}", cell))),
            };
            if let Err(e) = val_int {
                return Err(CubeError::user(format!(
                    "Can't parse int from, {:?}: {}",
                    cell, e
                )));
            }
            builder.append_value(val_int.unwrap())?;
        }
        t @ ColumnType::Decimal { .. } => {
            let scale = u8::try_from(t.target_scale()).unwrap();
            let d = match is_null {
                false => Some(parse_decimal(cell, scale)?),
                true => None,
            };
            let d = d.map(|d| d.raw_value());
            match scale {
                0 => builder
                    .as_any_mut()
                    .downcast_mut::<Int64Decimal0Builder>()
                    .unwrap()
                    .append_option(d)?,
                1 => builder
                    .as_any_mut()
                    .downcast_mut::<Int64Decimal1Builder>()
                    .unwrap()
                    .append_option(d)?,
                2 => builder
                    .as_any_mut()
                    .downcast_mut::<Int64Decimal2Builder>()
                    .unwrap()
                    .append_option(d)?,
                3 => builder
                    .as_any_mut()
                    .downcast_mut::<Int64Decimal3Builder>()
                    .unwrap()
                    .append_option(d)?,
                4 => builder
                    .as_any_mut()
                    .downcast_mut::<Int64Decimal4Builder>()
                    .unwrap()
                    .append_option(d)?,
                5 => builder
                    .as_any_mut()
                    .downcast_mut::<Int64Decimal5Builder>()
                    .unwrap()
                    .append_option(d)?,
                10 => builder
                    .as_any_mut()
                    .downcast_mut::<Int64Decimal10Builder>()
                    .unwrap()
                    .append_option(d)?,
                n => panic!("unhandled target scale: {}", n),
            }
        }
        ColumnType::Bytes => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<BinaryBuilder>()
                .unwrap();
            if is_null {
                builder.append_null()?;
                return Ok(());
            }
            let val;
            if let Expr::Value(v) = cell {
                val = parse_binary_string(buffer, v)?
            } else {
                return Err(CubeError::user("Corrupted data in query.".to_string()));
            };
            builder.append_value(val)?;
        }
        &ColumnType::HyperLogLog(f) => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<BinaryBuilder>()
                .unwrap();
            if is_null {
                builder.append_null()?;
                return Ok(());
            }
            let val;
            if let Expr::Value(v) = cell {
                val = parse_hyper_log_log(buffer, v, f)?
            } else {
                return Err(CubeError::user("Corrupted data in query.".to_string()));
            };
            builder
                .as_any_mut()
                .downcast_mut::<BinaryBuilder>()
                .unwrap()
                .append_value(val)?;
        }
        ColumnType::Timestamp => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<TimestampMicrosecondBuilder>()
                .unwrap();
            if is_null {
                builder.append_null()?;
                return Ok(());
            }
            match cell {
                Expr::Value(Value::SingleQuotedString(v)) => {
                    builder.append_value(timestamp_from_string(v)?.get_time_stamp() / 1000)?;
                }
                x => {
                    return Err(CubeError::user(format!(
                        "Can't parse timestamp from, {:?}",
                        x
                    )))
                }
            }
        }
        ColumnType::Boolean => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<BooleanBuilder>()
                .unwrap();
            if is_null {
                builder.append_null()?;
                return Ok(());
            }
            let v = match cell {
                Expr::Value(Value::SingleQuotedString(v)) => v.eq_ignore_ascii_case("true"),
                Expr::Value(Value::Boolean(b)) => *b,
                x => {
                    return Err(CubeError::user(format!(
                        "Can't parse boolean from, {:?}",
                        x
                    )))
                }
            };
            builder.append_value(v)?;
        }
        ColumnType::Float => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<Float64Builder>()
                .unwrap();
            if is_null {
                builder.append_null()?;
                return Ok(());
            }
            let v = parse_float(cell)?;
            builder.append_value(v)?;
        }
    }
    Ok(())
}

pub fn timestamp_from_string(v: &str) -> Result<TimestampValue, CubeError> {
    let nanos;
    if v.ends_with("UTC") {
        // TODO this parsed as nanoseconds instead of milliseconds
        #[rustfmt::skip] // built from "%Y-%m-%d %H:%M:%S%.3f UTC".
        const FORMAT: [chrono::format::Item; 14] = [Numeric(Year, Zero), Literal("-"), Numeric(Month, Zero), Literal("-"), Numeric(Day, Zero), Space(" "), Numeric(Hour, Zero), Literal(":"), Numeric(Minute, Zero), Literal(":"), Numeric(Second, Zero), Fixed(Nanosecond3), Space(" "), Literal("UTC")];
        match parse_time(v, &FORMAT).and_then(|p| p.to_datetime_with_timezone(&Utc)) {
            Ok(ts) => nanos = ts.timestamp_nanos(),
            Err(_) => return Err(CubeError::user(format!("Can't parse timestamp: {}", v))),
        }
    } else {
        match string_to_timestamp_nanos(v) {
            Ok(ts) => nanos = ts,
            Err(_) => return Err(CubeError::user(format!("Can't parse timestamp: {}", v))),
        }
    }
    Ok(TimestampValue::new(nanos))
}

fn parse_time(s: &str, format: &[chrono::format::Item]) -> ParseResult<Parsed> {
    let mut p = Parsed::new();
    chrono::format::parse(&mut p, s, format.into_iter())?;
    Ok(p)
}

fn parse_float(cell: &Expr) -> Result<f64, CubeError> {
    match cell {
        Expr::Value(Value::Number(v, _)) | Expr::Value(Value::SingleQuotedString(v)) => {
            Ok(v.parse::<f64>()?)
        }
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: box Expr::Value(Value::Number(v, _)),
        } => Ok(-v.parse::<f64>()?),
        _ => Err(CubeError::user(format!(
            "Can't parse float from, {:?}",
            cell
        ))),
    }
}
fn parse_decimal(cell: &Expr, scale: u8) -> Result<Decimal, CubeError> {
    match cell {
        Expr::Value(Value::Number(v, _)) | Expr::Value(Value::SingleQuotedString(v)) => {
            crate::import::parse_decimal(v, scale)
        }
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: box Expr::Value(Value::Number(v, _)),
        } => Ok(crate::import::parse_decimal(v, scale)?.negate()),
        _ => Err(CubeError::user(format!(
            "Can't parse decimal from, {:?}",
            cell
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Write;
    use std::path::PathBuf;
    use std::time::Duration;
    use std::{env, fs};

    use async_compression::tokio::write::GzipEncoder;
    use futures_timer::Delay;
    use itertools::Itertools;
    use pretty_assertions::assert_eq;
    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};
    use rocksdb::{Options, DB};
    use tokio::io::{AsyncWriteExt, BufWriter};
    use uuid::Uuid;

    use crate::cluster::MockCluster;
    use crate::config::{Config, FileStoreProvider};
    use crate::import::MockImportService;
    use crate::metastore::RocksMetaStore;
    use crate::queryplanner::query_executor::MockQueryExecutor;
    use crate::queryplanner::MockQueryPlanner;
    use crate::remotefs::{LocalDirRemoteFs, RemoteFile, RemoteFs};
    use crate::store::ChunkStore;

    use super::*;
    use crate::queryplanner::pretty_printers::pp_phys_plan;
    use crate::remotefs::queue::QueueRemoteFs;
    use crate::scheduler::SchedulerImpl;
    use crate::table::data::{cmp_min_rows, cmp_row_key_heap};
    use regex::Regex;

    #[tokio::test]
    async fn create_schema_test() {
        let config = Config::test("create_schema_test");
        let path = "/tmp/test_create_schema";
        let _ = DB::destroy(&Options::default(), path);
        let store_path = path.to_string() + &"_store".to_string();
        let remote_store_path = path.to_string() + &"remote_store".to_string();
        let _ = fs::remove_dir_all(store_path.clone());
        let _ = fs::remove_dir_all(remote_store_path.clone());

        {
            let remote_fs = LocalDirRemoteFs::new(
                Some(PathBuf::from(remote_store_path.clone())),
                PathBuf::from(store_path.clone()),
            );
            let meta_store = RocksMetaStore::new(path, remote_fs.clone(), config.config_obj());
            let rows_per_chunk = 10;
            let query_timeout = Duration::from_secs(30);
            let store = ChunkStore::new(
                meta_store.clone(),
                remote_fs.clone(),
                Arc::new(MockCluster::new()),
                config.config_obj(),
                rows_per_chunk,
            );
            let limits = Arc::new(ConcurrencyLimits::new(4));
            let service = SqlServiceImpl::new(
                meta_store,
                store,
                limits,
                Arc::new(MockQueryPlanner::new()),
                Arc::new(MockQueryExecutor::new()),
                Arc::new(MockCluster::new()),
                Arc::new(MockImportService::new()),
                config.config_obj(),
                remote_fs.clone(),
                rows_per_chunk,
                query_timeout,
                query_timeout,
                10_000, // max_cached_queries
            );
            let i = service.exec_query("CREATE SCHEMA foo").await.unwrap();
            assert_eq!(
                i.get_rows()[0],
                Row::new(vec![
                    TableValue::Int(1),
                    TableValue::String("foo".to_string())
                ])
            );
        }
        let _ = DB::destroy(&Options::default(), path);
        let _ = fs::remove_dir_all(store_path.clone());
        let _ = fs::remove_dir_all(remote_store_path.clone());
    }

    #[tokio::test]
    async fn create_table_test() {
        let config = Config::test("create_table_test");
        let path = "/tmp/test_create_table";
        let _ = DB::destroy(&Options::default(), path);
        let store_path = path.to_string() + &"_store".to_string();
        let remote_store_path = path.to_string() + &"remote_store".to_string();
        let _ = fs::remove_dir_all(store_path.clone());
        let _ = fs::remove_dir_all(remote_store_path.clone());
        {
            let remote_fs = LocalDirRemoteFs::new(
                Some(PathBuf::from(remote_store_path.clone())),
                PathBuf::from(store_path.clone()),
            );
            let meta_store = RocksMetaStore::new(path, remote_fs.clone(), config.config_obj());
            let rows_per_chunk = 10;
            let query_timeout = Duration::from_secs(30);
            let chunk_store = ChunkStore::new(
                meta_store.clone(),
                remote_fs.clone(),
                Arc::new(MockCluster::new()),
                config.config_obj(),
                rows_per_chunk,
            );
            let limits = Arc::new(ConcurrencyLimits::new(4));
            let service = SqlServiceImpl::new(
                meta_store.clone(),
                chunk_store,
                limits,
                Arc::new(MockQueryPlanner::new()),
                Arc::new(MockQueryExecutor::new()),
                Arc::new(MockCluster::new()),
                Arc::new(MockImportService::new()),
                config.config_obj(),
                remote_fs.clone(),
                rows_per_chunk,
                query_timeout,
                query_timeout,
                10_000, // max_cached_queries
            );
            let i = service.exec_query("CREATE SCHEMA Foo").await.unwrap();
            assert_eq!(
                i.get_rows()[0],
                Row::new(vec![
                    TableValue::Int(1),
                    TableValue::String("Foo".to_string())
                ])
            );
            let query = "CREATE TABLE Foo.Persons (
                                PersonID int,
                                LastName varchar(255),
                                FirstName varchar(255),
                                Address varchar(255),
                                City varchar(255)
                              );";
            let i = service.exec_query(&query.to_string()).await.unwrap();
            assert_eq!(i.get_rows()[0], Row::new(vec![
                TableValue::Int(1),
                TableValue::String("Persons".to_string()),
                TableValue::String("1".to_string()),
                TableValue::String("[{\"name\":\"PersonID\",\"column_type\":\"Int\",\"column_index\":0},{\"name\":\"LastName\",\"column_type\":\"String\",\"column_index\":1},{\"name\":\"FirstName\",\"column_type\":\"String\",\"column_index\":2},{\"name\":\"Address\",\"column_type\":\"String\",\"column_index\":3},{\"name\":\"City\",\"column_type\":\"String\",\"column_index\":4}]".to_string()),
                TableValue::String("NULL".to_string()),
                TableValue::String("NULL".to_string()),
                TableValue::String("false".to_string()),
                TableValue::String("true".to_string()),
                TableValue::String(meta_store.get_table("Foo".to_string(), "Persons".to_string()).await.unwrap().get_row().created_at().as_ref().unwrap().to_string()),
                TableValue::String("NULL".to_string()),
                TableValue::String("NULL".to_string()),
                TableValue::String("".to_string()),
                TableValue::String("NULL".to_string()),
                TableValue::String("NULL".to_string()),
                TableValue::String("NULL".to_string()),
            ]));
        }
        let _ = DB::destroy(&Options::default(), path);
        let _ = fs::remove_dir_all(store_path.clone());
        let _ = fs::remove_dir_all(remote_store_path.clone());
    }

    #[derive(Debug)]
    pub struct FailingRemoteFs(Arc<dyn RemoteFs>);

    crate::di_service!(FailingRemoteFs, [RemoteFs]);

    #[async_trait::async_trait]
    impl RemoteFs for FailingRemoteFs {
        async fn upload_file(
            &self,
            _temp_upload_path: &str,
            _remote_path: &str,
        ) -> Result<u64, CubeError> {
            Err(CubeError::internal("Not allowed".to_string()))
        }

        async fn download_file(
            &self,
            remote_path: &str,
            expected_file_size: Option<u64>,
        ) -> Result<String, CubeError> {
            self.0.download_file(remote_path, expected_file_size).await
        }

        async fn delete_file(&self, remote_path: &str) -> Result<(), CubeError> {
            self.0.delete_file(remote_path).await
        }

        async fn list(&self, remote_prefix: &str) -> Result<Vec<String>, CubeError> {
            self.0.list(remote_prefix).await
        }

        async fn list_with_metadata(
            &self,
            remote_prefix: &str,
        ) -> Result<Vec<RemoteFile>, CubeError> {
            self.0.list_with_metadata(remote_prefix).await
        }

        async fn local_path(&self) -> String {
            self.0.local_path().await
        }

        async fn local_file(&self, remote_path: &str) -> Result<String, CubeError> {
            self.0.local_file(remote_path).await
        }
    }

    #[tokio::test]
    async fn failed_upload_drop() {
        Config::test("failed_upload_drop").start_with_injector_override(async move |injector| {
            injector.register_typed::<dyn RemoteFs, _, _, _>(async move |injector| {
                Arc::new(FailingRemoteFs(
                    injector.get_service_typed::<QueueRemoteFs>().await,
                ))
            })
                .await
        }, async move |services| {
            let service = services.sql_service;

            let _ = service.exec_query("CREATE SCHEMA foo").await.unwrap();

            let _ = service
                .exec_query("CREATE TABLE foo.values (id int, dec_value decimal, dec_value_1 decimal(18, 2))")
                .await
                .unwrap();

            let res = service
                .exec_query("INSERT INTO foo.values (id, dec_value, dec_value_1) VALUES (1, -153, 1), (2, 20.01, 3.5), (3, 20.30, 12.3), (4, 120.30, 43.12), (5, NULL, NULL), (6, NULL, NULL), (7, NULL, NULL), (NULL, NULL, NULL)")
                .await;

            assert!(res.is_err(), "Expected {:?} to be not allowed error", res);

            let remote_fs = services.injector.get_service_typed::<QueueRemoteFs>().await;

            let temp_upload = remote_fs.temp_upload_path("").await.unwrap();
            let res = fs::read_dir(temp_upload.clone()).unwrap();
            assert!(res.into_iter().next().is_none(), "Expected empty uploads directory but found: {:?}", fs::read_dir(temp_upload).unwrap().into_iter().map(|e| e.unwrap().path().to_string_lossy().to_string()).collect::<Vec<_>>());
        })
            .await;
    }

    #[tokio::test]
    async fn decimal() {
        Config::test("decimal").update_config(|mut c| {
            c.partition_split_threshold = 2;
            c
        }).start_test(async move |services| {
            let service = services.sql_service;

            let _ = service.exec_query("CREATE SCHEMA foo").await.unwrap();

            let _ = service
                .exec_query("CREATE TABLE foo.values (id int, dec_value decimal, dec_value_1 decimal(18, 2))")
                .await
                .unwrap();

            service
                .exec_query("INSERT INTO foo.values (id, dec_value, dec_value_1) VALUES (1, -153, 1), (2, 20.01, 3.5), (3, 20.30, 12.3), (4, 120.30, 43.12), (5, NULL, NULL), (6, NULL, NULL), (7, NULL, NULL), (NULL, NULL, NULL)")
                .await
                .unwrap();

            let result = service
                .exec_query("SELECT sum(dec_value), sum(dec_value_1) from foo.values")
                .await
                .unwrap();

            assert_eq!(result.get_rows()[0], Row::new(vec![TableValue::Decimal(Decimal::new(761000)), TableValue::Decimal(Decimal::new(5992))]));

            let result = service
                .exec_query("SELECT sum(dec_value), sum(dec_value_1) from foo.values where dec_value > 10")
                .await
                .unwrap();

            assert_eq!(result.get_rows()[0], Row::new(vec![TableValue::Decimal(Decimal::new(16061000)), TableValue::Decimal(Decimal::new(5892))]));

            let result = service
                .exec_query("SELECT sum(dec_value), sum(dec_value_1) / 10 from foo.values where dec_value > 10")
                .await
                .unwrap();

            assert_eq!(result.get_rows()[0], Row::new(vec![TableValue::Decimal(Decimal::new(16061000)), TableValue::Float(5.892.into())]));

            let result = service
                .exec_query("SELECT sum(dec_value), sum(dec_value_1) / 10 from foo.values where dec_value_1 < 10")
                .await
                .unwrap();

            assert_eq!(result.get_rows()[0], Row::new(vec![TableValue::Decimal(Decimal::new(-13299000)), TableValue::Float(0.45.into())]));

            let result = service
                .exec_query("SELECT sum(dec_value), sum(dec_value_1) / 10 from foo.values where dec_value_1 < '10'")
                .await
                .unwrap();

            assert_eq!(result.get_rows()[0], Row::new(vec![TableValue::Decimal(Decimal::new(-13299000)), TableValue::Float(0.45.into())]));
        })
            .await;
    }

    #[tokio::test]
    async fn over_2k_booleans() {
        Config::test("over_2k_booleans").update_config(|mut c| {
            c.partition_split_threshold = 1000000;
            c.compaction_chunks_count_threshold = 0;
            c
        }).start_test(async move |services| {
            let service = services.sql_service;

            let _ = service.exec_query("CREATE SCHEMA foo").await.unwrap();

            let _ = service.exec_query("CREATE TABLE foo.bool_group (bool_value boolean)").await.unwrap();

            for batch in 0..25 {
                let mut bools = Vec::new();
                for i in 0..1000 {
                    bools.push(i % (batch + 1) == 0);
                }

                let values = bools.into_iter().map(|b| format!("({})", b)).join(", ");
                service.exec_query(
                    &format!("INSERT INTO foo.bool_group (bool_value) VALUES {}", values)
                ).await.unwrap();
            }

            let result = service.exec_query("SELECT count(*) from foo.bool_group").await.unwrap();
            assert_eq!(result.get_rows()[0], Row::new(vec![TableValue::Int(25000)]));

            let result = service.exec_query("SELECT count(*) from foo.bool_group where bool_value = true").await.unwrap();
            assert_eq!(result.get_rows()[0], Row::new(vec![TableValue::Int(3823)]));

            let result = service.exec_query("SELECT g.bool_value, count(*) from foo.bool_group g GROUP BY 1 ORDER BY 2 DESC").await.unwrap();

            assert_eq!(result.get_rows()[0], Row::new(vec![TableValue::Boolean(false), TableValue::Int(21177)]));
            assert_eq!(result.get_rows()[1], Row::new(vec![TableValue::Boolean(true), TableValue::Int(3823)]));
        }).await;
    }

    #[tokio::test]
    async fn over_10k_join() {
        Config::test("over_10k_join").update_config(|mut c| {
            c.partition_split_threshold = 1000000;
            c.compaction_chunks_count_threshold = 50;
            c
        }).start_test(async move |services| {
            let service = services.sql_service;

            service.exec_query("CREATE SCHEMA foo").await.unwrap();

            service.exec_query("CREATE TABLE foo.orders (amount int, email text)").await.unwrap();

            service.exec_query("CREATE INDEX orders_by_email ON foo.orders (email)").await.unwrap();

            service.exec_query("CREATE TABLE foo.customers (email text, system text, uuid text)").await.unwrap();

            service.exec_query("CREATE INDEX customers_by_email ON foo.customers (email)").await.unwrap();

            let mut join_results = Vec::new();

            for batch in 0..25 {
                let mut orders = Vec::new();
                let mut customers = Vec::new();
                for i in 0..1000 {
                    let email = String::from_utf8(thread_rng()
                        .sample_iter(&Alphanumeric)
                        .take(5)
                        .collect()
                    ).unwrap();
                    let domain = String::from_utf8(thread_rng()
                        .sample_iter(&Alphanumeric)
                        .take(5)
                        .collect()
                    ).unwrap();
                    let email = format!("{}@{}.com", email, domain);
                    orders.push((i, email.clone()));
                    if i % (batch + 1) == 0 {
                        let uuid = Uuid::new_v4().to_string();
                        customers.push((email.clone(), uuid.clone()));
                        if i % (batch + 1 + 10) == 0 {
                            customers.push((email.clone(), uuid.clone()));
                            join_results.push(Row::new(vec![TableValue::String(email.clone()), TableValue::String(uuid), TableValue::Int(i * 2)]))
                        } else {
                            join_results.push(Row::new(vec![TableValue::String(email.clone()), TableValue::String(uuid), TableValue::Int(i)]))
                        }
                    } else {
                        join_results.push(Row::new(vec![TableValue::String(email.clone()), TableValue::Null, TableValue::Int(i)]))
                    }
                }

                let values = orders.into_iter().map(|(amount, email)| format!("({}, '{}')", amount, email)).join(", ");

                service.exec_query(
                    &format!("INSERT INTO foo.orders (amount, email) VALUES {}", values)
                ).await.unwrap();

                let values = customers.into_iter().map(|(email, uuid)| format!("('{}', 'system', '{}')", email, uuid)).join(", ");

                service.exec_query(
                    &format!("INSERT INTO foo.customers (email, system, uuid) VALUES {}", values)
                ).await.unwrap();
            }

            join_results.sort_by(|a, b| cmp_row_key_heap(1, &a.values(), &b.values()));

            let result = service.exec_query("SELECT o.email, c.uuid, sum(o.amount) from foo.orders o LEFT JOIN foo.customers c ON o.email = c.email GROUP BY 1, 2 ORDER BY 1 ASC").await.unwrap();

            assert_eq!(result.get_rows().len(), join_results.len());
            for i in 0..result.get_rows().len() {
                // println!("Actual {}: {:?}", i, &result.get_rows()[i]);
                // println!("Expected {}: {:?}", i, &join_results[i]);
                assert_eq!(&result.get_rows()[i], &join_results[i]);
            }
        }).await;
    }

    #[tokio::test]
    async fn file_size_consistency() {
        Config::test("file_size_consistency")
            .start_test(async move |services| {
                let service = services.sql_service;

                let _ = service.exec_query("CREATE SCHEMA foo").await.unwrap();

                let _ = service
                    .exec_query("CREATE TABLE foo.ints (value int)")
                    .await
                    .unwrap();

                service
                    .exec_query("INSERT INTO foo.ints (value) VALUES (42)")
                    .await
                    .unwrap();

                let chunk = services.meta_store.get_chunk(1).await.unwrap();

                let path = {
                    let dir = env::temp_dir();

                    let path = dir.clone().join("1.chunk.parquet");
                    let mut file = File::create(path.clone()).unwrap();
                    file.write_all("Malformed parquet".as_bytes()).unwrap();
                    path
                };

                let remote_fs = services.injector.get_service_typed::<dyn RemoteFs>().await;
                remote_fs
                    .upload_file(
                        path.to_str().unwrap(),
                        &chunk.get_row().get_full_name(chunk.get_id()),
                    )
                    .await
                    .unwrap();

                let result = service.exec_query("SELECT count(*) from foo.ints").await;
                println!("Result: {:?}", result);
                assert!(result.is_err(), "Expected error but {:?} found", result);

                let result = service.exec_query("SELECT count(*) from foo.ints").await;
                println!("Result: {:?}", result);
                assert!(
                    result
                        .clone()
                        .err()
                        .unwrap()
                        .to_string()
                        .contains("not found"),
                    "Expected table not found error but got {:?}",
                    result
                );
            })
            .await;
    }

    #[tokio::test]
    async fn high_frequency_inserts() {
        Config::test("high_frequency_inserts")
            .update_config(|mut c| {
                c.partition_split_threshold = 100;
                c.compaction_chunks_count_threshold = 100;
                c
            })
            .start_test(async move |services| {
                let service = services.sql_service;

                service.exec_query("CREATE SCHEMA foo").await.unwrap();

                service
                    .exec_query("CREATE TABLE foo.numbers (num int)")
                    .await
                    .unwrap();

                for i in 0..300 {
                    service
                        .exec_query(&format!("INSERT INTO foo.numbers (num) VALUES ({})", i))
                        .await
                        .unwrap();
                }

                let result = service
                    .exec_query("SELECT count(*) from foo.numbers")
                    .await
                    .unwrap();
                assert_eq!(result.get_rows()[0], Row::new(vec![TableValue::Int(300)]));

                let result = service
                    .exec_query("SELECT sum(num) from foo.numbers")
                    .await
                    .unwrap();
                assert_eq!(result.get_rows()[0], Row::new(vec![TableValue::Int(44850)]));
            })
            .await;
    }

    #[tokio::test]
    async fn decimal_partition_pruning() {
        Config::test("decimal_partition_pruning")
            .update_config(|mut c| {
                c.partition_split_threshold = 1;
                c.compaction_chunks_count_threshold = 0;
                c
            })
            .start_test(async move |services| {
                let service = services.sql_service;

                service.exec_query("CREATE SCHEMA foo").await.unwrap();

                service
                    .exec_query("CREATE TABLE foo.numbers (num decimal)")
                    .await
                    .unwrap();

                for i in 0..100 {
                    service
                        .exec_query(&format!("INSERT INTO foo.numbers (num) VALUES ({})", i))
                        .await
                        .unwrap();
                }

                let result = service
                    .exec_query("SELECT count(*) from foo.numbers")
                    .await
                    .unwrap();
                assert_eq!(result.get_rows()[0], Row::new(vec![TableValue::Int(100)]));

                let result = service
                    .exec_query("SELECT sum(num) from foo.numbers where num = 50")
                    .await
                    .unwrap();
                assert_eq!(
                    result.get_rows()[0],
                    Row::new(vec![TableValue::Decimal(Decimal::new(5000000))])
                );

                let partitions = service
                    .exec_query("SELECT id, min_value, max_value FROM system.partitions")
                    .await
                    .unwrap();

                println!("All partitions: {:#?}", partitions);

                let plans = service
                    .plan_query("SELECT sum(num) from foo.numbers where num = 50")
                    .await
                    .unwrap();

                let worker_plan = pp_phys_plan(plans.worker.as_ref());
                println!("Worker Plan: {}", worker_plan);
                let parquet_regex = Regex::new(r"\d+-[a-z0-9]+.parquet").unwrap();
                let matches = parquet_regex.captures_iter(&worker_plan).count();
                assert!(
                    // TODO 2 because partition pruning doesn't respect half open intervals yet
                    matches < 3 && matches > 0,
                    "{}\nshould have 2 and less partition scan nodes",
                    worker_plan
                );
            })
            .await;
    }

    #[tokio::test]
    async fn delete_middle_main() {
        Config::test("delete_middle_main")
            .update_config(|mut c| {
                c.partition_split_threshold = 10;
                c.compaction_chunks_count_threshold = 0;
                c
            })
            .start_test(async move |services| {
                let service = services.sql_service;

                service.exec_query("CREATE SCHEMA foo").await.unwrap();

                service
                    .exec_query("CREATE TABLE foo.numbers (num int)")
                    .await
                    .unwrap();

                for i in 0..100 {
                    service
                        .exec_query(&format!("INSERT INTO foo.numbers (num) VALUES ({})", i))
                        .await
                        .unwrap();

                    let partitions = services
                        .meta_store
                        .get_partitions_with_chunks_created_seconds_ago(0)
                        .await
                        .unwrap();
                    for p in partitions.into_iter() {
                        services
                            .injector
                            .get_service_typed::<SchedulerImpl>()
                            .await
                            .schedule_partition_to_compact(&p)
                            .await
                            .unwrap()
                    }
                }

                let to_repartition = services
                    .meta_store
                    .all_inactive_partitions_to_repartition()
                    .await
                    .unwrap();

                for p in to_repartition.into_iter() {
                    services
                        .injector
                        .get_service_typed::<SchedulerImpl>()
                        .await
                        .schedule_repartition_if_needed(&p)
                        .await
                        .unwrap();
                }

                let chunks = services.meta_store.chunks_table().all_rows().await.unwrap();

                println!("All chunks: {:?}", chunks);

                for c in chunks.into_iter().filter(|c| !c.get_row().active()) {
                    let _ = services.meta_store.delete_chunk(c.get_id()).await;
                }

                let all_inactive_partitions = services
                    .meta_store
                    .all_inactive_middle_man_partitions()
                    .await
                    .unwrap();
                println!("Middle man partitions: {:?}", all_inactive_partitions);
                let mut futures = Vec::new();
                for p in all_inactive_partitions.into_iter() {
                    futures.push(services.meta_store.delete_middle_man_partition(p.get_id()))
                }
                join_all(futures)
                    .await
                    .into_iter()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();

                println!(
                    "All partitions: {:?}",
                    services
                        .meta_store
                        .partition_table()
                        .all_rows()
                        .await
                        .unwrap()
                );

                let result = service
                    .exec_query("SELECT count(*) from foo.numbers")
                    .await
                    .unwrap();
                assert_eq!(result.get_rows()[0], Row::new(vec![TableValue::Int(100)]));
            })
            .await;
    }

    #[tokio::test]
    async fn high_frequency_inserts_s3() {
        if env::var("CUBESTORE_AWS_ACCESS_KEY_ID").is_err() {
            return;
        }
        Config::test("high_frequency_inserts_s3")
            .update_config(|mut c| {
                c.partition_split_threshold = 1000000;
                c.compaction_chunks_count_threshold = 100;
                c.store_provider = FileStoreProvider::S3 {
                    region: "us-west-2".to_string(),
                    bucket_name: "cube-store-ci-test".to_string(),
                    sub_path: Some("high_frequency_inserts_s3".to_string()),
                };
                c.select_workers = vec!["127.0.0.1:4306".to_string()];
                c
            })
            .start_test(async move |services| {
                let service = services.sql_service;

                Config::test("high_frequency_inserts_worker_1")
                    .update_config(|mut c| {
                        c.worker_bind_address = Some("127.0.0.1:4306".to_string());
                        c.server_name = "127.0.0.1:4306".to_string();
                        c.store_provider = FileStoreProvider::S3 {
                            region: "us-west-2".to_string(),
                            bucket_name: "cube-store-ci-test".to_string(),
                            sub_path: Some("high_frequency_inserts_s3".to_string()),
                        };
                        c
                    })
                    .start_test_worker(async move |_| {
                        service.exec_query("CREATE SCHEMA foo").await.unwrap();

                        service
                            .exec_query("CREATE TABLE foo.numbers (num int)")
                            .await
                            .unwrap();

                        for _ in 0..3 {
                            let mut values = Vec::new();
                            for i in 0..100000 {
                                values.push(i);
                            }

                            let values = values.into_iter().map(|v| format!("({})", v)).join(", ");
                            service
                                .exec_query(&format!(
                                    "INSERT INTO foo.numbers (num) VALUES {}",
                                    values
                                ))
                                .await
                                .unwrap();
                        }

                        let (first_query, second_query) = futures::future::join(
                            service.exec_query("SELECT count(*) from foo.numbers"),
                            service.exec_query("SELECT sum(num) from foo.numbers"),
                        )
                        .await;

                        let result = first_query.unwrap();
                        assert_eq!(
                            result.get_rows()[0],
                            Row::new(vec![TableValue::Int(300000)])
                        );

                        let result = second_query.unwrap();
                        assert_eq!(
                            result.get_rows()[0],
                            Row::new(vec![TableValue::Int(300000 / 2 * 99999)])
                        );
                    })
                    .await;
            })
            .await;
    }

    #[tokio::test]
    async fn high_frequency_inserts_gcs() {
        if env::var("SERVICE_ACCOUNT_JSON").is_err()
            && env::var("CUBESTORE_GCP_SERVICE_ACCOUNT_JSON").is_err()
        {
            return;
        }
        Config::test("high_frequency_inserts_gcs")
            .update_config(|mut c| {
                c.partition_split_threshold = 1000000;
                c.compaction_chunks_count_threshold = 0;
                c.store_provider = FileStoreProvider::GCS {
                    bucket_name: "cube-store-ci-test".to_string(),
                    sub_path: Some("high_frequency_inserts_gcs".to_string()),
                };
                c.select_workers = vec!["127.0.0.1:4312".to_string()];
                c.metastore_bind_address = Some("127.0.0.1:15312".to_string());
                c
            })
            .start_test(async move |services| {
                let service = services.sql_service;

                Config::test("high_frequency_inserts_gcs_worker_1")
                    .update_config(|mut c| {
                        c.worker_bind_address = Some("127.0.0.1:4312".to_string());
                        c.server_name = "127.0.0.1:4312".to_string();
                        c.store_provider = FileStoreProvider::GCS {
                            bucket_name: "cube-store-ci-test".to_string(),
                            sub_path: Some("high_frequency_inserts_gcs".to_string()),
                        };
                        c.metastore_remote_address = Some("127.0.0.1:15312".to_string());
                        c
                    })
                    .start_test_worker(async move |_| {
                        service.exec_query("CREATE SCHEMA foo").await.unwrap();

                        service
                            .exec_query("CREATE TABLE foo.numbers (num int)")
                            .await
                            .unwrap();

                        for _ in 0..3 {
                            let mut values = Vec::new();
                            for i in 0..100000 {
                                values.push(i);
                            }

                            let values = values.into_iter().map(|v| format!("({})", v)).join(", ");
                            service
                                .exec_query(&format!(
                                    "INSERT INTO foo.numbers (num) VALUES {}",
                                    values
                                ))
                                .await
                                .unwrap();
                        }

                        let (first_query, second_query) = futures::future::join(
                            service.exec_query("SELECT count(*) from foo.numbers"),
                            service.exec_query("SELECT sum(num) from foo.numbers"),
                        )
                        .await;

                        let result = first_query.unwrap();
                        assert_eq!(
                            result.get_rows()[0],
                            Row::new(vec![TableValue::Int(300000)])
                        );

                        let result = second_query.unwrap();
                        assert_eq!(
                            result.get_rows()[0],
                            Row::new(vec![TableValue::Int(300000 / 2 * 99999)])
                        );
                    })
                    .await;
            })
            .await;
    }

    #[tokio::test]
    async fn inactive_partitions_cleanup() {
        Config::test("inactive_partitions_cleanup")
            .update_config(|mut c| {
                c.partition_split_threshold = 1000000;
                c.compaction_chunks_count_threshold = 0;
                c.not_used_timeout = 0;
                c.meta_store_log_upload_interval = 1;
                c.meta_store_snapshot_interval = 1;
                c.gc_loop_interval = 1;
                c
            })
            .start_test(async move |services| {
                let service = services.sql_service;

                service.exec_query("CREATE SCHEMA foo").await.unwrap();

                service
                    .exec_query("CREATE TABLE foo.numbers (num int)")
                    .await
                    .unwrap();

                for i in 0..10_u64 {
                    service
                        .exec_query(&format!("INSERT INTO foo.numbers (num) VALUES ({})", i))
                        .await
                        .unwrap();
                }

                // let listener = services.cluster.job_result_listener();
                // let active_partitions = services
                //     .meta_store
                //     .get_active_partitions_by_index_id(1)
                //     .await
                //     .unwrap();
                // let mut last_active_partition = active_partitions.iter().next().unwrap();
                // listener
                //     .wait_for_job_results(vec![(
                //         RowKey::Table(TableId::Partitions, last_active_partition.get_id()),
                //         JobType::Repartition,
                //     )])
                //     .await
                //     .unwrap();

                // TODO API to wait for all jobs to be completed and all events processed
                Delay::new(Duration::from_millis(500)).await;

                let result = service
                    .exec_query("SELECT count(*) from foo.numbers")
                    .await
                    .unwrap();
                assert_eq!(result.get_rows()[0], Row::new(vec![TableValue::Int(10)]));

                let active_partitions = services
                    .meta_store
                    .get_active_partitions_by_index_id(1)
                    .await
                    .unwrap();
                let last_active_partition = active_partitions.iter().next().unwrap();

                // Wait for GC tasks to drop files
                Delay::new(Duration::from_millis(3000)).await;

                let remote_fs = services.injector.get_service_typed::<dyn RemoteFs>().await;
                let files = remote_fs
                    .list("")
                    .await
                    .unwrap()
                    .into_iter()
                    .filter(|r| r.ends_with(".parquet"))
                    .collect::<Vec<_>>();
                assert_eq!(
                    files,
                    vec![format!(
                        "{}-{}.parquet",
                        last_active_partition.get_id(),
                        last_active_partition.get_row().suffix().as_ref().unwrap()
                    )]
                )
            })
            .await
    }

    #[tokio::test]
    async fn in_memory_compaction() {
        Config::test("inmemory_compaction")
            .update_config(|mut c| {
                c.partition_split_threshold = 1000000;
                c.compaction_chunks_count_threshold = 10;
                c.not_used_timeout = 0;
                c.compaction_in_memory_chunks_count_threshold = 5;
                c.compaction_in_memory_chunks_max_lifetime_threshold = 1;
                c
            })
            .start_test(async move |services| {
                let service = services.sql_service;

                service.exec_query("CREATE SCHEMA foo").await.unwrap();

                service
                    .exec_query("CREATE TABLE foo.numbers (a int, num int) UNIQUE KEY (a)")
                    .await
                    .unwrap();

                for i in 0..6 {
                    service
                        .exec_query(&format!(
                            "INSERT INTO foo.numbers (a, num, __seq) VALUES ({}, {}, {})",
                            i, i, i
                        ))
                        .await
                        .unwrap();
                }

                Delay::new(Duration::from_millis(500)).await;

                let active_partitions = services
                    .meta_store
                    .get_active_partitions_by_index_id(1)
                    .await
                    .unwrap();
                assert_eq!(active_partitions.len(), 1);
                let partition = active_partitions.first().unwrap();
                assert_eq!(partition.get_row().main_table_row_count(), 0);
                let chunks = services
                    .meta_store
                    .get_chunks_by_partition(partition.get_id(), false)
                    .await
                    .unwrap();
                assert_eq!(chunks.len(), 1);
                assert_eq!(chunks.first().unwrap().get_row().get_row_count(), 6);
                //waiting for more then compaction_chunks_count_threshold
                Delay::new(Duration::from_millis(2000)).await;
                service
                    .exec_query(&format!(
                        "INSERT INTO foo.numbers (a, num, __seq) VALUES ({}, {}, {})",
                        7, 7, 7
                    ))
                    .await
                    .unwrap();
                Delay::new(Duration::from_millis(1000)).await;
                let active_partitions = services
                    .meta_store
                    .get_active_partitions_by_index_id(1)
                    .await
                    .unwrap();
                assert_eq!(active_partitions.len(), 1);
                let partition = active_partitions.first().unwrap();
                assert_eq!(partition.get_row().main_table_row_count(), 6);
                let chunks = services
                    .meta_store
                    .get_chunks_by_partition(partition.get_id(), false)
                    .await
                    .unwrap();
                assert_eq!(chunks.len(), 1);
                assert_eq!(chunks.first().unwrap().get_row().get_row_count(), 1);
            })
            .await
    }

    #[tokio::test]
    async fn cluster() {
        Config::test("cluster_router").update_config(|mut config| {
            config.select_workers = vec!["127.0.0.1:14306".to_string(), "127.0.0.1:14307".to_string()];
            config.metastore_bind_address = Some("127.0.0.1:15306".to_string());
            config.compaction_chunks_count_threshold = 0;
            config
        }).start_test(async move |services| {
            let service = services.sql_service;

            Config::test("cluster_worker_1").update_config(|mut config| {
                config.worker_bind_address = Some("127.0.0.1:14306".to_string());
                config.server_name = "127.0.0.1:14306".to_string();
                config.metastore_remote_address = Some("127.0.0.1:15306".to_string());
                config.store_provider = FileStoreProvider::Filesystem {
                    remote_dir: Some(env::current_dir()
                        .unwrap()
                        .join("cluster_router-upstream".to_string())),
                };
                config.compaction_chunks_count_threshold = 0;
                config
            }).start_test_worker(async move |_| {
                Config::test("cluster_worker_2").update_config(|mut config| {
                    config.worker_bind_address = Some("127.0.0.1:14307".to_string());
                    config.server_name = "127.0.0.1:14307".to_string();
                    config.metastore_remote_address = Some("127.0.0.1:15306".to_string());
                    config.store_provider = FileStoreProvider::Filesystem {
                        remote_dir: Some(env::current_dir()
                            .unwrap()
                            .join("cluster_router-upstream".to_string())),
                    };
                    config.compaction_chunks_count_threshold = 0;
                    config
                }).start_test_worker(async move |_| {
                    service.exec_query("CREATE SCHEMA foo").await.unwrap();

                    service.exec_query("CREATE TABLE foo.orders_1 (orders_customer_id text, orders_product_id int, amount int)").await.unwrap();
                    service.exec_query("CREATE TABLE foo.orders_2 (orders_customer_id text, orders_product_id int, amount int)").await.unwrap();
                    service.exec_query("CREATE INDEX orders_by_product_1 ON foo.orders_1 (orders_product_id)").await.unwrap();
                    service.exec_query("CREATE INDEX orders_by_product_2 ON foo.orders_2 (orders_product_id)").await.unwrap();
                    service.exec_query("CREATE TABLE foo.customers (customer_id text, city text, state text)").await.unwrap();
                    service.exec_query("CREATE TABLE foo.products (product_id int, name text)").await.unwrap();

                    service.exec_query(
                        "INSERT INTO foo.orders_1 (orders_customer_id, orders_product_id, amount) VALUES ('a', 1, 10), ('b', 2, 2), ('b', 2, 3)"
                    ).await.unwrap();

                    service.exec_query(
                        "INSERT INTO foo.orders_1 (orders_customer_id, orders_product_id, amount) VALUES ('b', 1, 10), ('c', 2, 2), ('c', 2, 3)"
                    ).await.unwrap();

                    service.exec_query(
                        "INSERT INTO foo.orders_2 (orders_customer_id, orders_product_id, amount) VALUES ('c', 1, 10), ('d', 2, 2), ('d', 2, 3)"
                    ).await.unwrap();

                    service.exec_query(
                        "INSERT INTO foo.customers (customer_id, city, state) VALUES ('a', 'San Francisco', 'CA'), ('b', 'New York', 'NY')"
                    ).await.unwrap();

                    service.exec_query(
                        "INSERT INTO foo.customers (customer_id, city, state) VALUES ('c', 'San Francisco', 'CA'), ('d', 'New York', 'NY')"
                    ).await.unwrap();

                    service.exec_query(
                        "INSERT INTO foo.products (product_id, name) VALUES (1, 'Potato'), (2, 'Tomato')"
                    ).await.unwrap();

                    let result = service.exec_query(
                        "SELECT city, name, sum(amount) FROM (SELECT * FROM foo.orders_1 UNION ALL SELECT * FROM foo.orders_2) o \
                LEFT JOIN foo.customers c ON orders_customer_id = customer_id \
                LEFT JOIN foo.products p ON orders_product_id = product_id \
                WHERE customer_id = 'a' \
                GROUP BY 1, 2 ORDER BY 3 DESC, 1 ASC, 2 ASC"
                    ).await.unwrap();

                    let expected = vec![
                        Row::new(vec![TableValue::String("San Francisco".to_string()), TableValue::String("Potato".to_string()), TableValue::Int(10)]),
                    ];

                    assert_eq!(
                        result.get_rows(),
                        &expected
                    );
                }).await;
            }).await;
        }).await;
    }

    #[tokio::test]
    async fn table_partition_split_threshold() {
        let test_name = "table_partition_split_threshold";
        let port_base = 24406;
        Config::test(test_name).update_config(|mut config| {
            config.select_workers = vec![format!("127.0.0.1:{}", port_base + 1), format!("127.0.0.1:{}", port_base + 2)];
            config.metastore_bind_address = Some(format!("127.0.0.1:{}", port_base));
            config.compaction_chunks_count_threshold = 0;
            config.max_partition_split_threshold = 200;
            config
        }).start_test(async move |services| {
            let service = services.sql_service;

            Config::test(&format!("{}_cluster_worker_1", test_name)).update_config(|mut config| {
                config.worker_bind_address = Some(format!("127.0.0.1:{}", port_base + 1));
                config.server_name = format!("127.0.0.1:{}", port_base + 1);
                config.metastore_remote_address = Some(format!("127.0.0.1:{}", port_base));
                config.store_provider = FileStoreProvider::Filesystem {
                    remote_dir: Some(env::current_dir()
                        .unwrap()
                        .join(format!("{}-upstream", test_name))),
                };
                config.compaction_chunks_count_threshold = 0;
                config.max_partition_split_threshold = 200;
                config
            }).start_test_worker(async move |_| {
                Config::test(&format!("{}_cluster_worker_2", test_name)).update_config(|mut config| {
                    config.worker_bind_address = Some(format!("127.0.0.1:{}", port_base + 2));
                    config.server_name = format!("127.0.0.1:{}", port_base + 2);
                    config.metastore_remote_address = Some(format!("127.0.0.1:{}", port_base));
                    config.store_provider = FileStoreProvider::Filesystem {
                        remote_dir: Some(env::current_dir()
                            .unwrap()
                            .join(format!("{}-upstream", test_name))),
                    };
                    config.compaction_chunks_count_threshold = 0;
                    config.max_partition_split_threshold = 200;
                    config
                }).start_test_worker(async move |_| {
                    let url = "https://data.wprdc.org/dataset/0b584c84-7e35-4f4d-a5a2-b01697470c0f/resource/e95dd941-8e47-4460-9bd8-1e51c194370b/download/bikepghpublic.csv";

                    service
                        .exec_query("CREATE SCHEMA IF NOT EXISTS foo")
                        .await
                        .unwrap();

                    let create_table_sql = format!("CREATE TABLE foo.bikes (`Response ID` int, `Start Date` text, `End Date` text) LOCATION '{}'", url);

                    service.exec_query(&create_table_sql).await.unwrap();

                    let result = service
                        .exec_query("SELECT count(*) from foo.bikes")
                        .await
                        .unwrap();

                    assert_eq!(
                        result.get_rows(),
                        &vec![Row::new(vec![TableValue::Int(813)])]
                    );

                    let result = service
                        .exec_query("SELECT partition_split_threshold from system.tables")
                        .await
                        .unwrap();

                    assert_eq!(
                        result.get_rows(),
                        &vec![Row::new(vec![TableValue::Int(200)])]
                    );
                }).await;
            }).await;
        }).await;
    }

    #[tokio::test]
    async fn create_table_with_location_cluster() {
        if env::var("CUBESTORE_AWS_ACCESS_KEY_ID").is_err() {
            return;
        }
        Config::test("create_table_with_location_cluster")
            .update_config(|mut c| {
                c.partition_split_threshold = 1000000;
                c.compaction_chunks_count_threshold = 100;
                c.store_provider = FileStoreProvider::S3 {
                    region: "us-west-2".to_string(),
                    bucket_name: "cube-store-ci-test".to_string(),
                    sub_path: Some("create_table_with_location_cluster".to_string()),
                };
                c.select_workers = vec!["127.0.0.1:24306".to_string()];
                c.metastore_bind_address = Some("127.0.0.1:25312".to_string());
                c
            })
            .start_test(async move |services| {
                let service = services.sql_service;

                Config::test("create_table_with_location_cluster_worker_1")
                    .update_config(|mut c| {
                        c.worker_bind_address = Some("127.0.0.1:24306".to_string());
                        c.server_name = "127.0.0.1:24306".to_string();
                        c.store_provider = FileStoreProvider::S3 {
                            region: "us-west-2".to_string(),
                            bucket_name: "cube-store-ci-test".to_string(),
                            sub_path: Some("create_table_with_location_cluster".to_string()),
                        };
                        c.metastore_remote_address = Some("127.0.0.1:25312".to_string());
                        c
                    })
                    .start_test_worker(async move |_| {
                        let paths = {
                            let dir = env::temp_dir();

                            let path_1 = dir.clone().join("foo-cluster-1.csv");
                            let path_2 = dir.clone().join("foo-cluster-2.csv.gz");
                            let mut file = File::create(path_1.clone()).unwrap();

                            file.write_all("id,city,arr,t\n".as_bytes()).unwrap();
                            file.write_all("1,San Francisco,\"[\"\"Foo\n\n\"\",\"\"Bar\"\",\"\"FooBar\"\"]\",\"2021-01-24 12:12:23 UTC\"\n".as_bytes()).unwrap();
                            file.write_all("2,\"New York\",\"[\"\"\"\"]\",2021-01-24 19:12:23.123 UTC\n".as_bytes()).unwrap();
                            file.write_all("3,New York,\"de Comunicación\",2021-01-25 19:12:23 UTC\n".as_bytes()).unwrap();

                            let mut file = GzipEncoder::new(BufWriter::new(tokio::fs::File::create(path_2.clone()).await.unwrap()));

                            file.write_all("id,city,arr,t\n".as_bytes()).await.unwrap();
                            file.write_all("1,San Francisco,\"[\"\"Foo\"\",\"\"Bar\"\",\"\"FooBar\"\"]\",\"2021-01-24 12:12:23 UTC\"\n".as_bytes()).await.unwrap();
                            file.write_all("2,\"New York\",\"[\"\"\"\"]\",2021-01-24 19:12:23 UTC\n".as_bytes()).await.unwrap();
                            file.write_all("3,New York,,2021-01-25 19:12:23 UTC\n".as_bytes()).await.unwrap();
                            file.write_all("4,New York,\"\",2021-01-25 19:12:23 UTC\n".as_bytes()).await.unwrap();
                            file.write_all("5,New York,\"\",2021-01-25 19:12:23 UTC\n".as_bytes()).await.unwrap();

                            file.shutdown().await.unwrap();

                            vec![path_1, path_2]
                        };

                        let _ = service.exec_query("CREATE SCHEMA IF NOT EXISTS Foo").await.unwrap();
                        let _ = service.exec_query(
                            &format!(
                                "CREATE TABLE Foo.Persons (id int, city text, t timestamp, arr text) INDEX persons_city (`city`, `id`) LOCATION {}",
                                paths.into_iter().map(|p| format!("'{}'", p.to_string_lossy())).join(",")
                            )
                        ).await.unwrap();

                        let result = service.exec_query("SELECT count(*) as cnt from Foo.Persons").await.unwrap();
                        assert_eq!(result.get_rows(), &vec![Row::new(vec![TableValue::Int(8)])]);
                    })
                    .await;
            })
            .await;
    }

    #[tokio::test]
    async fn compaction() {
        Config::test("compaction").update_config(|mut config| {
            config.partition_split_threshold = 5;
            config.compaction_chunks_count_threshold = 0;
            config
        }).start_test(async move |services| {
            let service = services.sql_service;

            service.exec_query("CREATE SCHEMA foo").await.unwrap();

            service.exec_query("CREATE TABLE foo.table (t int)").await.unwrap();

            let listener = services.cluster.job_result_listener();

            service.exec_query(
                "INSERT INTO foo.table (t) VALUES (NULL), (1), (3), (5), (10), (20), (25), (25), (25), (25), (25), (NULL), (NULL), (NULL), (2), (4), (5), (27), (28), (29)"
            ).await.unwrap();

            let wait = listener.wait_for_job_results(vec![
                (RowKey::Table(TableId::Partitions, 1), JobType::PartitionCompaction),
            ]);
            timeout(Duration::from_secs(10), wait).await.unwrap().unwrap();

            let partitions = services.meta_store.get_active_partitions_by_index_id(1).await.unwrap();

            assert_eq!(partitions.len(), 4);
            let p_1 = partitions.iter().find(|r| r.get_id() == 2).unwrap();
            let p_2 = partitions.iter().find(|r| r.get_id() == 3).unwrap();
            let p_3 = partitions.iter().find(|r| r.get_id() == 4).unwrap();
            let p_4 = partitions.iter().find(|r| r.get_id() == 5).unwrap();
            let new_partitions = vec![p_1, p_2, p_3, p_4];
            println!("{:?}", new_partitions);
            let mut intervals_set = new_partitions.into_iter()
                .map(|p| (p.get_row().get_min_val().clone(), p.get_row().get_max_val().clone()))
                .collect::<Vec<_>>();
            intervals_set.sort_by(|(min_a, _), (min_b, _)| cmp_min_rows(1, min_a.as_ref(), min_b.as_ref()));
            let mut expected = vec![
                (None, Some(Row::new(vec![TableValue::Int(2)]))),
                (Some(Row::new(vec![TableValue::Int(2)])), Some(Row::new(vec![TableValue::Int(10)]))),
                (Some(Row::new(vec![TableValue::Int(10)])), Some(Row::new(vec![TableValue::Int(27)]))),
                (Some(Row::new(vec![TableValue::Int(27)])), None),
            ].into_iter().collect::<Vec<_>>();
            expected.sort_by(|(min_a, _), (min_b, _)| cmp_min_rows(1, min_a.as_ref(), min_b.as_ref()));
            assert_eq!(intervals_set, expected);

            let result = service.exec_query("SELECT count(*) from foo.table").await.unwrap();

            assert_eq!(result.get_rows()[0], Row::new(vec![TableValue::Int(20)]));
        }).await;
    }

    #[tokio::test]
    async fn create_table_with_temp_file() {
        Config::run_test("create_table_with_temp_file", async move |services| {
            let service = services.sql_service;

            let paths = {
                let dir = env::temp_dir();

                let path_2 = dir.clone().join("foo-3.csv.gz");

                let mut file = GzipEncoder::new(BufWriter::new(tokio::fs::File::create(path_2.clone()).await.unwrap()));

                file.write_all("id,city,arr,t\n".as_bytes()).await.unwrap();
                file.write_all("1,San Francisco,\"[\"\"Foo\"\",\"\"Bar\"\",\"\"FooBar\"\"]\",\"2021-01-24 12:12:23 UTC\"\n".as_bytes()).await.unwrap();
                file.write_all("2,\"New York\",\"[\"\"\"\"]\",2021-01-24 19:12:23 UTC\n".as_bytes()).await.unwrap();
                file.write_all("3,New York,,2021-01-25 19:12:23 UTC\n".as_bytes()).await.unwrap();
                file.write_all("4,New York,\"\",2021-01-25 19:12:23 UTC\n".as_bytes()).await.unwrap();
                file.write_all("5,New York,\"\",2021-01-25 19:12:23 UTC\n".as_bytes()).await.unwrap();

                file.shutdown().await.unwrap();

                let remote_fs = services.injector.get_service_typed::<dyn RemoteFs>().await;
                remote_fs.upload_file(path_2.to_str().unwrap(), "temp-uploads/foo-3.csv.gz").await.unwrap();

                vec!["temp://foo-3.csv.gz".to_string()]
            };

            let _ = service.exec_query("CREATE SCHEMA IF NOT EXISTS Foo").await.unwrap();
            let _ = service.exec_query(
                &format!(
                    "CREATE TABLE Foo.Persons (id int, city text, t timestamp, arr text) INDEX persons_city (`city`, `id`) LOCATION {}",
                    paths.into_iter().map(|p| format!("'{}'", p)).join(",")
                )
            ).await.unwrap();

            let result = service.exec_query("SELECT count(*) as cnt from Foo.Persons").await.unwrap();
            assert_eq!(result.get_rows(), &vec![Row::new(vec![TableValue::Int(5)])]);

            let result = service.exec_query("SELECT count(*) as cnt from Foo.Persons WHERE arr = '[\"Foo\",\"Bar\",\"FooBar\"]' or arr = '[\"\"]' or arr is null").await.unwrap();
            assert_eq!(result.get_rows(), &vec![Row::new(vec![TableValue::Int(5)])]);
        }).await;
    }

    #[tokio::test]
    async fn explain_logical_plan() {
        Config::run_test("explain_logical_plan", async move |services| {
            let service = services.sql_service;
            service.exec_query("CREATE SCHEMA foo").await.unwrap();

            service.exec_query("CREATE TABLE foo.orders (id int, platform text, age int, amount int)").await.unwrap();

            service.exec_query(
                "INSERT INTO foo.orders (id, platform, age, amount) VALUES (1, 'android', 18, 4), (2, 'andorid', 17, 4), (3, 'ios', 20, 5)"
                ).await.unwrap();

            let result = service.exec_query(
                "EXPLAIN SELECT platform, sum(amount) from foo.orders where age > 15 group by platform" 
            ).await.unwrap();
            assert_eq!(result.len(), 1);
            assert_eq!(result.get_columns().len(), 1);

            let pp_plan = match &result
                .get_rows()[0]
                .values()[0] {
                    TableValue::String(pp_plan) => pp_plan,
                    _ => {assert!(false); ""}
                };
            assert_eq!(
                pp_plan,
                "Projection, [foo.orders.platform, SUM(foo.orders.amount)]\
                \n  Aggregate\
                \n    ClusterSend, indices: [[1]]\
                \n      Filter\
                \n        Scan foo.orders, source: CubeTable(index: default:1:[1]), fields: [platform, age, amount]"
            );
        }).await;
    }
    #[tokio::test]
    async fn explain_physical_plan() {
        Config::test("explain_analyze_router").update_config(|mut config| {
            config.select_workers = vec!["127.0.0.1:14006".to_string()];
            config.metastore_bind_address = Some("127.0.0.1:15006".to_string());
            config.compaction_chunks_count_threshold = 0;
            config
        }).start_test(async move |services| {
            let service = services.sql_service;

            Config::test("expalain_analyze_worker_1").update_config(|mut config| {
                config.worker_bind_address = Some("127.0.0.1:14006".to_string());
                config.server_name = "127.0.0.1:14006".to_string();
                config.metastore_remote_address = Some("127.0.0.1:15006".to_string());
                config.store_provider = FileStoreProvider::Filesystem {
                    remote_dir: Some(env::current_dir()
                        .unwrap()
                        .join("explain_analyze_router-upstream".to_string())),
                };
                config.compaction_chunks_count_threshold = 0;
                config
            }).start_test_worker(async move |_| {
                service.exec_query("CREATE SCHEMA foo").await.unwrap();

                service.exec_query("CREATE TABLE foo.orders (id int, platform text, age int, amount int)").await.unwrap();

                service.exec_query(
                    "INSERT INTO foo.orders (id, platform, age, amount) VALUES (1, 'android', 18, 4), (2, 'andorid', 17, 4), (3, 'ios', 20, 5)"
                    ).await.unwrap();

                let result = service.exec_query(
                    "EXPLAIN ANALYZE SELECT platform, sum(amount) from foo.orders where age > 15 group by platform" 
                    ).await.unwrap();

                assert_eq!(result.len(), 2);

                assert_eq!(result.get_columns().len(), 3);

                let router_row = &result.get_rows()[0];
                match &router_row
                    .values()[0] {
                        TableValue::String(node_type) => {assert_eq!(node_type, "router");},
                        _ => {assert!(false);}
                    };
                match &router_row
                    .values()[1] {
                        TableValue::String(node_name) => {assert!(node_name.is_empty());},
                        _ => {assert!(false);}
                    };
                match &router_row
                    .values()[2] {
                        TableValue::String(pp_plan) => {
                            assert_eq!(
                                pp_plan,
                                "Projection, [platform, SUM(foo.orders.amount)@1:SUM(amount)]\
                                \n  FinalHashAggregate\
                                \n    ClusterSend, partitions: [[1]]"
                            );
                        },
                        _ => {assert!(false);}
                    };

                let worker_row = &result.get_rows()[1];
                match &worker_row
                    .values()[0] {
                        TableValue::String(node_type) => {assert_eq!(node_type, "worker");},
                        _ => {assert!(false);}
                    };
                match &worker_row
                    .values()[1] {
                        TableValue::String(node_name) => {assert_eq!(node_name, "127.0.0.1:14006");},
                        _ => {assert!(false);}
                    };
                match &worker_row
                    .values()[2] {
                        TableValue::String(pp_plan) => {
                            let regex = Regex::new(
                                r"PartialHas+hAggregate\s+Filter\s+Merge\s+Scan, index: default:1:\[1\], fields+: \[platform, age, amount\]\s+ParquetScan, files+: .*\.chunk\.parquet"
                            ).unwrap();
                            let matches = regex.captures_iter(&pp_plan).count();
                            assert_eq!(matches, 1);
                        },
                        _ => {assert!(false);}
                    };

            }).await;
        }).await;
    }
    #[tokio::test]
    async fn create_aggr_index() {
        assert!(true);
        Config::test("aggregate_index")
            .update_config(|mut c| {
                c.partition_split_threshold = 10;
                c.compaction_chunks_count_threshold = 0;
                c
            })
            .start_test(async move |services| {
                let service = services.sql_service;
                service.exec_query("CREATE SCHEMA foo").await.unwrap();

                let paths = {
                    let dir = env::temp_dir();

                    let path_2 = dir.clone().join("orders.csv.gz");

                    let mut file = GzipEncoder::new(BufWriter::new(
                        tokio::fs::File::create(path_2.clone()).await.unwrap(),
                    ));

                    file.write_all("platform,age,gender,cnt,max_id\n".as_bytes())
                        .await
                        .unwrap();
                    file.write_all("\"ios\",20,\"M\",10,100\n".as_bytes())
                        .await
                        .unwrap();
                    file.write_all("\"android\",20,\"M\",2,10\n".as_bytes())
                        .await
                        .unwrap();
                    file.write_all("\"web\",20,\"M\",20,111\n".as_bytes())
                        .await
                        .unwrap();

                    file.write_all("\"ios\",20,\"F\",10,100\n".as_bytes())
                        .await
                        .unwrap();
                    file.write_all("\"android\",20,\"F\",2,10\n".as_bytes())
                        .await
                        .unwrap();
                    file.write_all("\"web\",22,\"F\",20,115\n".as_bytes())
                        .await
                        .unwrap();
                    file.write_all("\"web\",22,\"F\",20,222\n".as_bytes())
                        .await
                        .unwrap();

                    file.shutdown().await.unwrap();

                    services
                        .injector
                        .get_service_typed::<dyn RemoteFs>()
                        .await
                        .upload_file(path_2.to_str().unwrap(), "temp-uploads/orders.csv.gz")
                        .await
                        .unwrap();

                    vec!["temp://orders.csv.gz".to_string()]
                };
                let query = format!(
                    "CREATE TABLE foo.Orders (
                                    platform varchar(255),
                                    age int,
                                    gender varchar(2),
                                    cnt int,
                                    max_id int
                                  )
                    AGGREGATIONS (sum(cnt), max(max_id))
                    INDEX index1 (platform, age)
                    AGGREGATE INDEX aggr_index (platform, age)
                    LOCATION {}",
                    paths.into_iter().map(|p| format!("'{}'", p)).join(",")
                );
                service.exec_query(&query).await.unwrap();

                let indices = services.meta_store.get_table_indexes(1).await.unwrap();

                let aggr_index = indices
                    .iter()
                    .find(|i| i.get_row().get_name() == "aggr_index")
                    .unwrap();

                let partitions = services
                    .meta_store
                    .get_active_partitions_by_index_id(aggr_index.get_id())
                    .await
                    .unwrap();
                let chunks = services
                    .meta_store
                    .get_chunks_by_partition(partitions[0].get_id(), false)
                    .await
                    .unwrap();

                assert_eq!(chunks.len(), 1);
                assert_eq!(chunks[0].get_row().get_row_count(), 4);

                let p = service
                    .plan_query(
                        "SELECT platform, age, sum(cnt) FROM foo.Orders GROUP BY platform, age",
                    )
                    .await
                    .unwrap();

                let worker_plan = pp_phys_plan(p.worker.as_ref());
                assert!(worker_plan.find("aggr_index").is_some());
            })
            .await;
    }
}

impl SqlServiceImpl {
    fn handle_workbench_queries(q: &str) -> Option<DataFrame> {
        if q == "SHOW SESSION VARIABLES LIKE 'lower_case_table_names'" {
            return Some(DataFrame::new(
                vec![
                    Column::new("Variable_name".to_string(), ColumnType::String, 0),
                    Column::new("Value".to_string(), ColumnType::String, 1),
                ],
                vec![Row::new(vec![
                    TableValue::String("lower_case_table_names".to_string()),
                    TableValue::String("2".to_string()),
                ])],
            ));
        }
        if q == "SHOW SESSION VARIABLES LIKE 'sql_mode'" {
            return Some(DataFrame::new(
                vec![
                    Column::new("Variable_name".to_string(), ColumnType::String, 0),
                    Column::new("Value".to_string(), ColumnType::String, 1),
                ],
                vec![Row::new(vec![
                    TableValue::String("sql_mode".to_string()),
                    TableValue::String("TRADITIONAL".to_string()),
                ])],
            ));
        }
        if q.to_lowercase() == "select current_user()" {
            return Some(DataFrame::new(
                vec![Column::new("user".to_string(), ColumnType::String, 0)],
                vec![Row::new(vec![TableValue::String("root".to_string())])],
            ));
        }
        if q.to_lowercase() == "select connection_id()" {
            // TODO
            return Some(DataFrame::new(
                vec![Column::new(
                    "connection_id".to_string(),
                    ColumnType::String,
                    0,
                )],
                vec![Row::new(vec![TableValue::String("1".to_string())])],
            ));
        }
        if q.to_lowercase() == "select connection_id() as connectionid" {
            // TODO
            return Some(DataFrame::new(
                vec![Column::new(
                    "connectionId".to_string(),
                    ColumnType::String,
                    0,
                )],
                vec![Row::new(vec![TableValue::String("1".to_string())])],
            ));
        }
        if q.to_lowercase() == "set character set utf8" {
            return Some(DataFrame::new(vec![], vec![]));
        }
        if q.to_lowercase() == "set names utf8" {
            return Some(DataFrame::new(vec![], vec![]));
        }
        if q.to_lowercase() == "show character set where charset = 'utf8mb4'" {
            return Some(DataFrame::new(vec![], vec![]));
        }
        None
    }
}
