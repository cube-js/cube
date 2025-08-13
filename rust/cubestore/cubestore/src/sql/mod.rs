use std::collections::HashMap;
use std::convert::TryFrom;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::format::Fixed::Nanosecond3;
use chrono::format::Item::{Fixed, Literal, Numeric, Space};
use chrono::format::Numeric::{Day, Hour, Minute, Month, Second, Year};
use chrono::format::Pad::Zero;
use chrono::format::Parsed;
use chrono::{ParseResult, TimeZone, Utc};
use datafusion::arrow::array::*;
use datafusion::arrow::compute::kernels::cast_utils::string_to_timestamp_nanos;
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

use crate::cachestore::CacheStore;
use crate::cluster::Cluster;
use crate::config::injection::DIService;
use crate::config::ConfigObj;
use crate::import::limits::ConcurrencyLimits;
use crate::import::{parse_space_separated_binstring, ImportService, Ingestion};
use crate::metastore::multi_index::MultiIndex;
use crate::metastore::source::SourceCredentials;
use crate::metastore::{
    is_valid_plain_binary_hll, HllFlavour, IdRow, ImportFormat, Index, IndexDef, IndexType,
    MetaStoreTable, Schema,
};
use crate::queryplanner::panic::PanicWorkerNode;
use crate::queryplanner::pretty_printers::{pp_phys_plan, pp_plan};
use crate::queryplanner::query_executor::{batches_to_dataframe, ClusterSendExec, QueryExecutor};
use crate::queryplanner::serialized_plan::{RowFilter, SerializedPlan};
use crate::queryplanner::{PlanningMeta, QueryPlan, QueryPlanner};
use crate::remotefs::RemoteFs;
use crate::sql::cache::SqlResultCache;
use crate::sql::parser::{CubeStoreParser, DropCommand, MetaStoreCommand, SystemCommand};
use crate::store::ChunkDataStore;
use crate::table::{data, Row, TableValue, TimestampValue};
use crate::util::decimal::{Decimal, Decimal96};
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
use deepsize::DeepSizeOf;

pub mod cache;
pub mod cachestore;
pub mod parser;
mod table_creator;

use crate::cluster::rate_limiter::ProcessRateLimiter;
use crate::sql::cachestore::CacheStoreSqlService;
use crate::util::metrics;
use mockall::automock;
use table_creator::{convert_columns_type, TableCreator};
pub use table_creator::{TableExtensionService, TableExtensionServiceImpl};

#[automock]
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

#[derive(Serialize, Deserialize, Clone, Hash, Eq, PartialEq, Debug, DeepSizeOf)]
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
    cachestore: CacheStoreSqlService,
    chunk_store: Arc<dyn ChunkDataStore>,
    remote_fs: Arc<dyn RemoteFs>,
    limits: Arc<ConcurrencyLimits>,
    query_planner: Arc<dyn QueryPlanner>,
    query_executor: Arc<dyn QueryExecutor>,
    cluster: Arc<dyn Cluster>,
    config_obj: Arc<dyn ConfigObj>,
    rows_per_chunk: usize,
    query_timeout: Duration,
    cache: Arc<SqlResultCache>,
    table_creator: Arc<TableCreator>,
}

crate::di_service!(SqlServiceImpl, [SqlService]);
crate::di_service!(MockSqlService, [SqlService]);

impl SqlServiceImpl {
    pub fn new(
        db: Arc<dyn MetaStore>,
        cachestore: Arc<dyn CacheStore>,
        chunk_store: Arc<dyn ChunkDataStore>,
        limits: Arc<ConcurrencyLimits>,
        query_planner: Arc<dyn QueryPlanner>,
        query_executor: Arc<dyn QueryExecutor>,
        cluster: Arc<dyn Cluster>,
        import_service: Arc<dyn ImportService>,
        table_extension_service: Arc<dyn TableExtensionService>,
        config_obj: Arc<dyn ConfigObj>,
        remote_fs: Arc<dyn RemoteFs>,
        rows_per_chunk: usize,
        query_timeout: Duration,
        create_table_timeout: Duration,
        cache: Arc<SqlResultCache>,
        process_rate_limiter: Arc<dyn ProcessRateLimiter>,
    ) -> Arc<SqlServiceImpl> {
        Arc::new(SqlServiceImpl {
            cachestore: CacheStoreSqlService::new(
                cachestore,
                query_planner.clone(),
                process_rate_limiter,
            ),
            table_creator: TableCreator::new(
                db.clone(),
                cluster.clone(),
                import_service,
                table_extension_service,
                config_obj.clone(),
                create_table_timeout,
                cache.clone(),
            ),
            db,
            chunk_store,
            limits,
            query_planner,
            query_executor,
            cluster,
            config_obj,
            rows_per_chunk,
            query_timeout,
            remote_fs,
            cache,
        })
    }

    async fn create_schema(
        &self,
        name: String,
        if_not_exists: bool,
    ) -> Result<IdRow<Schema>, CubeError> {
        self.db.create_schema(name, if_not_exists).await
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
                None,
            )
            .await?;

        let mut dump_dir = PathBuf::from(&self.remote_fs.local_path().await?);
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
                for (_, f, size, _) in p.all_required_files() {
                    let f = self.remote_fs.download_file(f, size).await?;
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
            .logical_plan(
                DFStatement::Statement(statement),
                &InlineTables::new(),
                None,
            )
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
            QueryPlan::Meta(logical_plan) => {
                if !analyze {
                    Ok(DataFrame::new(
                        vec![Column::new(
                            "logical plan".to_string(),
                            ColumnType::String,
                            0,
                        )],
                        vec![Row::new(vec![TableValue::String(pp_plan(&logical_plan))])],
                    ))
                } else {
                    Err(CubeError::user(
                        "EXPLAIN ANALYZE is not supported for selects from system tables"
                            .to_string(),
                    ))
                }
            }
        }?;
        Ok(Arc::new(res))
    }
}

pub fn string_prop(credentials: &Vec<SqlOption>, prop_name: &str) -> Option<String> {
    credentials
        .iter()
        .find(|o| o.name.value == prop_name)
        .and_then(|x| {
            if let Value::SingleQuotedString(v) = &x.value {
                Some(v.to_string())
            } else {
                None
            }
        })
}

pub fn boolean_prop(credentials: &Vec<SqlOption>, prop_name: &str) -> Option<bool> {
    credentials
        .iter()
        .find(|o| o.name.value == prop_name)
        .and_then(|x| {
            if let Value::Boolean(v) = &x.value {
                Some(*v)
            } else {
                None
            }
        })
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
                        None,
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
                SystemCommand::Drop(command) => match command {
                    DropCommand::DropQueryCache => {
                        self.cache.clear().await;

                        Ok(Arc::new(DataFrame::new(vec![], vec![])))
                    }
                    DropCommand::DropAllCache => {
                        self.cache.clear().await;

                        Ok(Arc::new(DataFrame::new(vec![], vec![])))
                    }
                },
                SystemCommand::MetaStore(command) => match command {
                    MetaStoreCommand::SetCurrent { id } => {
                        self.db.set_current_snapshot(id).await?;
                        Ok(Arc::new(DataFrame::new(vec![], vec![])))
                    }
                    MetaStoreCommand::Compaction => {
                        self.db.compaction().await?;
                        Ok(Arc::new(DataFrame::new(vec![], vec![])))
                    }
                    MetaStoreCommand::Healthcheck => {
                        self.db.healthcheck().await?;
                        Ok(Arc::new(DataFrame::new(vec![], vec![])))
                    }
                },
                SystemCommand::CacheStore(command) => {
                    self.cachestore
                        .exec_system_command_with_context(context, command)
                        .await
                }
            },
            CubeStoreStatement::Statement(Statement::SetVariable { .. }) => {
                Ok(Arc::new(DataFrame::new(vec![], vec![])))
            }
            CubeStoreStatement::CreateSchema {
                schema_name,
                if_not_exists,
            } => {
                app_metrics::DATA_QUERIES.add_with_tags(
                    1,
                    Some(&vec![metrics::format_tag("command", "create_schema")]),
                );

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
                        if_not_exists,
                        ..
                    },
                indexes,
                aggregates,
                locations,
                unique_key,
                partitioned_index,
            } => {
                app_metrics::DATA_QUERIES.add_with_tags(
                    1,
                    Some(&vec![metrics::format_tag("command", "create_table")]),
                );

                let nv = &name.0;
                if nv.len() != 2 {
                    return Err(CubeError::user(format!(
                        "Schema's name should be present in table name but found: {}",
                        name
                    )));
                }
                let schema_name = &nv[0].value;
                let table_name = &nv[1].value;
                let mut import_format = with_options
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

                let delimiter = with_options
                    .iter()
                    .find(|&opt| opt.name.value == "delimiter")
                    .map_or(Ok(None), |option| match &option.value {
                        Value::SingleQuotedString(delimiter) => match delimiter.as_str() {
                            "tab" => Ok(Some('\t')),
                            "^A" => Ok(Some('\u{0001}')),
                            s if s.len() != 1 => {
                                Err(CubeError::user(format!("Bad delimiter {}", option.value)))
                            }
                            s => Ok(Some(s.chars().next().unwrap())),
                        },
                        _ => Err(CubeError::user(format!("Bad delimiter {}", option.value))),
                    })?;

                if let Some(delimiter) = delimiter {
                    import_format = match import_format {
                        ImportFormat::CSV => ImportFormat::CSVOptions {
                            delimiter: Some(delimiter),
                            has_header: true,
                            escape: None,
                            quote: None,
                        },
                        ImportFormat::CSVNoHeader => ImportFormat::CSVOptions {
                            delimiter: Some(delimiter),
                            has_header: false,
                            escape: None,
                            quote: None,
                        },
                        ImportFormat::CSVOptions {
                            has_header,
                            escape,
                            quote,
                            ..
                        } => ImportFormat::CSVOptions {
                            delimiter: Some(delimiter),
                            has_header,
                            escape,
                            quote,
                        },
                    }
                }
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

                let seal_at = with_options
                    .iter()
                    .find(|&opt| opt.name.value == "seal_at")
                    .map_or(Result::Ok(None), |option| match &option.value {
                        Value::SingleQuotedString(seal_at) => {
                            let ts = timestamp_from_string(seal_at)?;
                            let utc = Utc.timestamp_nanos(ts.get_time_stamp());
                            Result::Ok(Some(utc))
                        }
                        _ => Result::Err(CubeError::user(format!("Bad seal_at {}", option.value))),
                    })?;
                let select_statement = with_options
                    .iter()
                    .find(|&opt| opt.name.value == "select_statement")
                    .map_or(Result::Ok(None), |option| match &option.value {
                        Value::SingleQuotedString(select_statement) => {
                            Result::Ok(Some(select_statement.clone()))
                        }
                        _ => Result::Err(CubeError::user(format!(
                            "Bad select_statement {}",
                            option.value
                        ))),
                    })?;
                let source_table = with_options
                    .iter()
                    .find(|&opt| opt.name.value == "source_table")
                    .map_or(Result::Ok(None), |option| match &option.value {
                        Value::SingleQuotedString(source_table) => {
                            Result::Ok(Some(source_table.clone()))
                        }
                        _ => Result::Err(CubeError::user(format!(
                            "Bad source_table {}",
                            option.value
                        ))),
                    })?;
                let stream_offset = with_options
                    .iter()
                    .find(|&opt| opt.name.value == "stream_offset")
                    .map_or(Result::Ok(None), |option| match &option.value {
                        Value::SingleQuotedString(select_statement) => {
                            Result::Ok(Some(select_statement.clone()))
                        }
                        _ => Result::Err(CubeError::user(format!(
                            "Bad stream_offset {}. Expected string.",
                            option.value
                        ))),
                    })?;

                let res = self
                    .table_creator
                    .clone()
                    .create_table(
                        schema_name.clone(),
                        table_name.clone(),
                        &columns,
                        external,
                        if_not_exists,
                        locations,
                        Some(import_format),
                        build_range_end,
                        seal_at,
                        select_statement,
                        source_table,
                        stream_offset,
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
                app_metrics::DATA_QUERIES.add_with_tags(
                    1,
                    Some(&vec![metrics::format_tag("command", "create_index")]),
                );

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
                app_metrics::DATA_QUERIES.add_with_tags(
                    1,
                    Some(&vec![metrics::format_tag("command", "create_source")]),
                );

                if or_update {
                    let creds = match source_type.as_str() {
                        "ksql" => {
                            let user = string_prop(&credentials, "user");
                            let password = string_prop(&credentials, "password");
                            let url = string_prop(&credentials, "url");
                            Ok(SourceCredentials::KSql {
                                user,
                                password,
                                url: url.ok_or(CubeError::user(
                                    "url is required as credential for ksql source".to_string(),
                                ))?,
                            })
                        }
                        "kafka" => {
                            let user = string_prop(&credentials, "user");
                            let password = string_prop(&credentials, "password");
                            let host = string_prop(&credentials, "host");
                            let use_ssl = boolean_prop(&credentials, "use_ssl");
                            Ok(SourceCredentials::Kafka {
                                user,
                                password,
                                host: host.ok_or(CubeError::user(
                                    "host is required as credential for kafka source".to_string(),
                                ))?,
                                use_ssl: use_ssl.unwrap_or(false),
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
                app_metrics::DATA_QUERIES.add_with_tags(
                    1,
                    Some(&vec![metrics::format_tag(
                        "command",
                        "create_partitioned_index",
                    )]),
                );

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
                let command = match object_type {
                    ObjectType::Schema => {
                        self.db.delete_schema(names[0].to_string()).await?;
                        &"drop_schema"
                    }
                    ObjectType::Table => {
                        let table = self
                            .db
                            .get_table(names[0].0[0].to_string(), names[0].0[1].to_string())
                            .await?;
                        self.db.drop_table(table.get_id()).await?;
                        &"drop_table"
                    }
                    ObjectType::PartitionedIndex => {
                        let schema = names[0].0[0].value.clone();
                        let name = names[0].0[1].value.clone();
                        self.db.drop_partitioned_index(schema, name).await?;
                        &"drop_partitioned_index"
                    }
                    _ => return Err(CubeError::user("Unsupported drop operation".to_string())),
                };

                app_metrics::DATA_QUERIES
                    .add_with_tags(1, Some(&vec![metrics::format_tag("command", command)]));

                Ok(Arc::new(DataFrame::new(vec![], vec![])))
            }
            CubeStoreStatement::Statement(Statement::Insert {
                table_name,
                columns,
                source,
                ..
            }) => {
                app_metrics::DATA_QUERIES
                    .add_with_tags(1, Some(&vec![metrics::format_tag("command", "insert")]));

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
            CubeStoreStatement::Queue(command) => {
                self.cachestore
                    .exec_queue_command_with_context(context, command)
                    .await
            }
            CubeStoreStatement::Cache(command) => {
                self.cachestore
                    .exec_cache_command_with_context(context, command)
                    .await
            }
            CubeStoreStatement::Statement(Statement::Query(q)) => {
                let logical_plan = self
                    .query_planner
                    .logical_plan(
                        DFStatement::Statement(Statement::Query(q)),
                        &context.inline_tables,
                        context.trace_obj.clone(),
                    )
                    .await?;

                // TODO distribute and combine
                let res = match logical_plan {
                    QueryPlan::Meta(logical_plan) => {
                        app_metrics::META_QUERIES.increment();
                        Arc::new(self.query_planner.execute_meta_plan(logical_plan).await?)
                    }
                    QueryPlan::Select(serialized, workers) => {
                        app_metrics::DATA_QUERIES.add_with_tags(
                            1,
                            Some(&vec![metrics::format_tag("command", "select")]),
                        );

                        let cluster = self.cluster.clone();
                        let executor = self.query_executor.clone();
                        timeout(
                            self.query_timeout,
                            self.cache
                                .get(query, context, serialized, async move |plan| {
                                    let records;
                                    if workers.len() == 0 {
                                        records =
                                            executor.execute_router_plan(plan, cluster).await?.1;
                                    } else {
                                        // Pick one of the workers to run as main for the request.
                                        let i = thread_rng().sample(Uniform::new(0, workers.len()));
                                        let rs = cluster.route_select(&workers[i], plan).await?.1;
                                        records = rs
                                            .into_iter()
                                            .map(|r| r.read())
                                            .collect::<Result<Vec<_>, _>>()?;
                                    }
                                    Ok(cube_ext::spawn_blocking(
                                        move || -> Result<DataFrame, CubeError> {
                                            let df = batches_to_dataframe(records)?;
                                            Ok(df)
                                        },
                                    )
                                    .await??)
                                })
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
                        None,
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
                        for (_, f, _, _) in worker_plan.files_to_download() {
                            let name = self.remote_fs.local_file(f.clone()).await?;
                            mocked_names.insert(f, name);
                        }
                        let chunk_ids_to_batches = worker_plan
                            .in_memory_chunks_to_load()
                            .into_iter()
                            .map(|(c, _, _)| (c.get_id(), Vec::new()))
                            .collect();
                        return Ok(QueryPlans {
                            router: self
                                .query_executor
                                .router_plan(router_plan, self.cluster.clone())
                                .await?
                                .0,
                            worker: self
                                .query_executor
                                .worker_plan(worker_plan, mocked_names, chunk_ids_to_batches, None)
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
                file_path.to_string_lossy().to_string(),
                format!("temp-uploads/{}", name),
            )
            .await?;
        Ok(())
    }

    async fn temp_uploads_dir(&self, _context: SqlQueryContext) -> Result<String, CubeError> {
        self.remote_fs.uploads_dir().await
    }
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
        HllFlavour::DataSketches => {
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
        ColumnType::Int96 => {
            let builder = builder.as_any_mut().downcast_mut::<Int96Builder>().unwrap();
            if is_null {
                builder.append_null()?;
                return Ok(());
            }
            let val_int = match cell {
                Expr::Value(Value::Number(v, _)) | Expr::Value(Value::SingleQuotedString(v)) => {
                    v.parse::<i128>()
                }
                Expr::UnaryOp {
                    op: UnaryOperator::Minus,
                    expr,
                } => {
                    if let Expr::Value(Value::Number(v, _)) = expr.as_ref() {
                        v.parse::<i128>().map(|v| v * -1)
                    } else {
                        return Err(CubeError::user(format!(
                            "Can't parse int96 from, {:?}",
                            cell
                        )));
                    }
                }
                _ => {
                    return Err(CubeError::user(format!(
                        "Can't parse int96 from, {:?}",
                        cell
                    )))
                }
            };
            if let Err(e) = val_int {
                return Err(CubeError::user(format!(
                    "Can't parse int96 from, {:?}: {}",
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
        t @ ColumnType::Decimal96 { .. } => {
            let scale = u8::try_from(t.target_scale()).unwrap();
            let d = match is_null {
                false => Some(parse_decimal_96(cell, scale)?),
                true => None,
            };
            let d = d.map(|d| d.raw_value());
            match scale {
                0 => builder
                    .as_any_mut()
                    .downcast_mut::<Int96Decimal0Builder>()
                    .unwrap()
                    .append_option(d)?,
                1 => builder
                    .as_any_mut()
                    .downcast_mut::<Int96Decimal1Builder>()
                    .unwrap()
                    .append_option(d)?,
                2 => builder
                    .as_any_mut()
                    .downcast_mut::<Int96Decimal2Builder>()
                    .unwrap()
                    .append_option(d)?,
                3 => builder
                    .as_any_mut()
                    .downcast_mut::<Int96Decimal3Builder>()
                    .unwrap()
                    .append_option(d)?,
                4 => builder
                    .as_any_mut()
                    .downcast_mut::<Int96Decimal4Builder>()
                    .unwrap()
                    .append_option(d)?,
                5 => builder
                    .as_any_mut()
                    .downcast_mut::<Int96Decimal5Builder>()
                    .unwrap()
                    .append_option(d)?,
                10 => builder
                    .as_any_mut()
                    .downcast_mut::<Int96Decimal10Builder>()
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
fn parse_decimal_96(cell: &Expr, scale: u8) -> Result<Decimal96, CubeError> {
    match cell {
        Expr::Value(Value::Number(v, _)) | Expr::Value(Value::SingleQuotedString(v)) => {
            crate::import::parse_decimal_96(v, scale)
        }
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: box Expr::Value(Value::Number(v, _)),
        } => Ok(crate::import::parse_decimal_96(v, scale)?.negate()),
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

    use crate::metastore::job::JobType;
    use crate::store::compaction::CompactionService;
    use crate::table::parquet::CubestoreMetadataCacheFactoryImpl;
    use async_compression::tokio::write::GzipEncoder;
    use cuberockstore::rocksdb::{Options, DB};
    use datafusion::physical_plan::parquet::BasicMetadataCacheFactory;
    use futures_timer::Delay;
    use itertools::Itertools;
    use pretty_assertions::assert_eq;
    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};
    use table_creator::TableExtensionServiceImpl;
    use tokio::io::{AsyncWriteExt, BufWriter};
    use uuid::Uuid;

    use crate::cluster::MockCluster;
    use crate::config::{Config, FileStoreProvider};
    use crate::import::MockImportService;
    use crate::metastore::{BaseRocksStoreFs, RocksMetaStore, RowKey, TableId};
    use crate::queryplanner::query_executor::MockQueryExecutor;
    use crate::queryplanner::MockQueryPlanner;
    use crate::remotefs::{ExtendedRemoteFs, LocalDirRemoteFs, RemoteFile, RemoteFs};
    use crate::store::ChunkStore;

    use super::*;
    use crate::cachestore::RocksCacheStore;
    use crate::cluster::rate_limiter::BasicProcessRateLimiter;
    use crate::queryplanner::pretty_printers::{pp_phys_plan, pp_phys_plan_ext, PPOptions};
    use crate::remotefs::queue::QueueRemoteFs;
    use crate::scheduler::SchedulerImpl;
    use crate::table::data::{cmp_min_rows, cmp_row_key_heap};
    use crate::table::TableValue;
    use crate::util::int96::Int96;
    use regex::Regex;

    #[tokio::test]
    async fn create_schema_test() {
        let config = Config::test("create_schema_test");
        let path = "/tmp/test_create_schema";

        let store_path = path.to_string() + &"_store".to_string();
        let remote_store_path = path.to_string() + &"remote_store".to_string();

        let _ = fs::remove_dir_all(path);
        let _ = fs::remove_dir_all(store_path.clone());
        let _ = fs::remove_dir_all(remote_store_path.clone());

        {
            let remote_fs = LocalDirRemoteFs::new(
                Some(PathBuf::from(remote_store_path.clone())),
                PathBuf::from(store_path.clone()),
            );
            let meta_store = RocksMetaStore::new(
                &Path::new(path).join("metastore"),
                BaseRocksStoreFs::new_for_metastore(remote_fs.clone(), config.config_obj()),
                config.config_obj(),
            )
            .unwrap();
            let cache_store = RocksCacheStore::new(
                &Path::new(path).join("cachestore"),
                BaseRocksStoreFs::new_for_cachestore(remote_fs.clone(), config.config_obj()),
                config.config_obj(),
            )
            .unwrap();
            let rows_per_chunk = 10;
            let query_timeout = Duration::from_secs(30);
            let store = ChunkStore::new(
                meta_store.clone(),
                remote_fs.clone(),
                Arc::new(MockCluster::new()),
                config.config_obj(),
                CubestoreMetadataCacheFactoryImpl::new(Arc::new(BasicMetadataCacheFactory::new())),
                rows_per_chunk,
            );
            let limits = Arc::new(ConcurrencyLimits::new(4));
            let service = SqlServiceImpl::new(
                meta_store,
                cache_store,
                store,
                limits,
                Arc::new(MockQueryPlanner::new()),
                Arc::new(MockQueryExecutor::new()),
                Arc::new(MockCluster::new()),
                Arc::new(MockImportService::new()),
                TableExtensionServiceImpl::new(),
                config.config_obj(),
                remote_fs.clone(),
                rows_per_chunk,
                query_timeout,
                query_timeout,
                Arc::new(SqlResultCache::new(
                    config.config_obj().query_cache_max_capacity_bytes(),
                    config.config_obj().query_cache_time_to_idle_secs(),
                    1000,
                )),
                BasicProcessRateLimiter::new(),
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

        let _ = DB::destroy(&Options::default(), Path::new(path).join("metastore"));
        let _ = DB::destroy(&Options::default(), Path::new(path).join("cachestore"));

        let _ = fs::remove_dir_all(store_path.clone());
        let _ = fs::remove_dir_all(remote_store_path.clone());
    }

    #[tokio::test]
    async fn create_table_test() {
        let config = Config::test("create_table_test");
        let path = "/tmp/test_create_table";

        let store_path = path.to_string() + &"_store".to_string();
        let remote_store_path = path.to_string() + &"remote_store".to_string();

        let _ = fs::remove_dir_all(path);
        let _ = fs::remove_dir_all(store_path.clone());
        let _ = fs::remove_dir_all(remote_store_path.clone());

        {
            let remote_fs = LocalDirRemoteFs::new(
                Some(PathBuf::from(remote_store_path.clone())),
                PathBuf::from(store_path.clone()),
            );
            let meta_store = RocksMetaStore::new(
                &Path::new(path).join("metastore"),
                BaseRocksStoreFs::new_for_metastore(remote_fs.clone(), config.config_obj()),
                config.config_obj(),
            )
            .unwrap();
            let cache_store = RocksCacheStore::new(
                &Path::new(path).join("cachestore"),
                BaseRocksStoreFs::new_for_cachestore(remote_fs.clone(), config.config_obj()),
                config.config_obj(),
            )
            .unwrap();
            let rows_per_chunk = 10;
            let query_timeout = Duration::from_secs(30);
            let chunk_store = ChunkStore::new(
                meta_store.clone(),
                remote_fs.clone(),
                Arc::new(MockCluster::new()),
                config.config_obj(),
                CubestoreMetadataCacheFactoryImpl::new(Arc::new(BasicMetadataCacheFactory::new())),
                rows_per_chunk,
            );
            let limits = Arc::new(ConcurrencyLimits::new(4));
            let service = SqlServiceImpl::new(
                meta_store.clone(),
                cache_store,
                chunk_store,
                limits,
                Arc::new(MockQueryPlanner::new()),
                Arc::new(MockQueryExecutor::new()),
                Arc::new(MockCluster::new()),
                Arc::new(MockImportService::new()),
                TableExtensionServiceImpl::new(),
                config.config_obj(),
                remote_fs.clone(),
                rows_per_chunk,
                query_timeout,
                query_timeout,
                Arc::new(SqlResultCache::new(
                    config.config_obj().query_cache_max_capacity_bytes(),
                    config.config_obj().query_cache_time_to_idle_secs(),
                    1000,
                )),
                BasicProcessRateLimiter::new(),
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
                TableValue::String("false".to_string()),
                TableValue::String("NULL".to_string()),
                TableValue::String("NULL".to_string()),
                TableValue::String("NULL".to_string()),
                TableValue::String("NULL".to_string()),
                TableValue::String("".to_string()),
                TableValue::String("NULL".to_string()),
                TableValue::String("NULL".to_string()),
                TableValue::String("NULL".to_string()),
                TableValue::String("NULL".to_string()),
            ]));
        }

        let _ = DB::destroy(&Options::default(), Path::new(path).join("metastore"));
        let _ = DB::destroy(&Options::default(), Path::new(path).join("cachestore"));

        let _ = fs::remove_dir_all(store_path.clone());
        let _ = fs::remove_dir_all(remote_store_path.clone());
    }

    #[tokio::test]
    async fn create_table_test_seal_at() {
        let config = Config::test("create_table_test_seal_at");
        let path = "/tmp/test_create_table_seal_at";

        let store_path = path.to_string() + &"_store".to_string();
        let remote_store_path = path.to_string() + &"remote_store".to_string();

        let _ = fs::remove_dir_all(path);
        let _ = fs::remove_dir_all(store_path.clone());
        let _ = fs::remove_dir_all(remote_store_path.clone());

        {
            let remote_fs = LocalDirRemoteFs::new(
                Some(PathBuf::from(remote_store_path.clone())),
                PathBuf::from(store_path.clone()),
            );
            let meta_store = RocksMetaStore::new(
                &Path::new(path).join("metastore"),
                BaseRocksStoreFs::new_for_metastore(remote_fs.clone(), config.config_obj()),
                config.config_obj(),
            )
            .unwrap();
            let cache_store = RocksCacheStore::new(
                &Path::new(path).join("cachestore"),
                BaseRocksStoreFs::new_for_cachestore(remote_fs.clone(), config.config_obj()),
                config.config_obj(),
            )
            .unwrap();
            let rows_per_chunk = 10;
            let query_timeout = Duration::from_secs(30);
            let chunk_store = ChunkStore::new(
                meta_store.clone(),
                remote_fs.clone(),
                Arc::new(MockCluster::new()),
                config.config_obj(),
                CubestoreMetadataCacheFactoryImpl::new(Arc::new(BasicMetadataCacheFactory::new())),
                rows_per_chunk,
            );
            let limits = Arc::new(ConcurrencyLimits::new(4));
            let service = SqlServiceImpl::new(
                meta_store.clone(),
                cache_store,
                chunk_store,
                limits,
                Arc::new(MockQueryPlanner::new()),
                Arc::new(MockQueryExecutor::new()),
                Arc::new(MockCluster::new()),
                Arc::new(MockImportService::new()),
                TableExtensionServiceImpl::new(),
                config.config_obj(),
                remote_fs.clone(),
                rows_per_chunk,
                query_timeout,
                query_timeout,
                Arc::new(SqlResultCache::new(
                    config.config_obj().query_cache_max_capacity_bytes(),
                    config.config_obj().query_cache_time_to_idle_secs(),
                    1000,
                )),
                BasicProcessRateLimiter::new(),
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
                              ) WITH (seal_at='2022-10-05T01:00:00.000Z', select_statement='SELECT * FROM test WHERE created_at > \\'2022-05-01 00:00:00\\'');";
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
                TableValue::String("2022-10-05 01:00:00 UTC".to_string()),
                TableValue::String("false".to_string()),
                TableValue::String("SELECT * FROM test WHERE created_at > '2022-05-01 00:00:00'".to_string()),
                TableValue::String("NULL".to_string()),
                TableValue::String("NULL".to_string()),
                TableValue::String("NULL".to_string()),
                TableValue::String("".to_string()),
                TableValue::String("NULL".to_string()),
                TableValue::String("NULL".to_string()),
                TableValue::String("NULL".to_string()),
                TableValue::String("NULL".to_string()),
            ]));
        }

        let _ = DB::destroy(&Options::default(), Path::new(path).join("metastore"));
        let _ = DB::destroy(&Options::default(), Path::new(path).join("cachestore"));

        let _ = fs::remove_dir_all(store_path.clone());
        let _ = fs::remove_dir_all(remote_store_path.clone());
    }

    //#[derive(Debug)]
    pub struct FailingRemoteFs(Arc<dyn RemoteFs>);

    crate::di_service!(FailingRemoteFs, [RemoteFs]);
    use crate::remotefs::CommonRemoteFsUtils;

    #[async_trait::async_trait]
    impl RemoteFs for FailingRemoteFs {
        async fn temp_upload_path(&self, remote_path: String) -> Result<String, CubeError> {
            CommonRemoteFsUtils::temp_upload_path(self, remote_path).await
        }

        async fn uploads_dir(&self) -> Result<String, CubeError> {
            CommonRemoteFsUtils::uploads_dir(self).await
        }

        async fn check_upload_file(
            &self,
            remote_path: String,
            expected_size: u64,
        ) -> Result<(), CubeError> {
            CommonRemoteFsUtils::check_upload_file(self, remote_path, expected_size).await
        }
        async fn upload_file(
            &self,
            _temp_upload_path: String,
            _remote_path: String,
        ) -> Result<u64, CubeError> {
            Err(CubeError::internal("Not allowed".to_string()))
        }

        async fn download_file(
            &self,
            remote_path: String,
            expected_file_size: Option<u64>,
        ) -> Result<String, CubeError> {
            self.0.download_file(remote_path, expected_file_size).await
        }

        async fn delete_file(&self, remote_path: String) -> Result<(), CubeError> {
            self.0.delete_file(remote_path).await
        }

        async fn list(&self, remote_prefix: String) -> Result<Vec<String>, CubeError> {
            self.0.list(remote_prefix).await
        }

        async fn list_with_metadata(
            &self,
            remote_prefix: String,
        ) -> Result<Vec<RemoteFile>, CubeError> {
            self.0.list_with_metadata(remote_prefix).await
        }

        async fn local_path(&self) -> Result<String, CubeError> {
            self.0.local_path().await
        }

        async fn local_file(&self, remote_path: String) -> Result<String, CubeError> {
            self.0.local_file(remote_path).await
        }
    }

    #[async_trait::async_trait]
    impl ExtendedRemoteFs for FailingRemoteFs {}

    #[tokio::test]
    async fn create_table_if_not_exists() {
        Config::test("create_table_if_not_exists").start_with_injector_override(async move |injector| {
            injector.register_typed::<dyn RemoteFs, _, _, _>(async move |injector| {
                Arc::new(FailingRemoteFs(
                    injector.get_service_typed::<QueueRemoteFs>().await,
                ))
            })
                .await
        }, async move |services| {
            let service = services.sql_service;

            let _ = service.exec_query("CREATE SCHEMA foo").await.unwrap();

            let created_table = service
                .exec_query("CREATE TABLE foo.values (id int, dec_value decimal, dec_value_1 decimal(18, 2))")
                .await.unwrap();
            let res = service
                .exec_query("CREATE TABLE foo.values (id int, dec_value decimal, dec_value_1 decimal(18, 2))")
                .await;
            assert!(res.is_err());
            let res = service
                .exec_query("CREATE TABLE IF NOT EXISTS foo.values (id int, dec_value decimal, dec_value_1 decimal(18, 2))")
                .await;
            assert_eq!(res.unwrap(), created_table);


        })
            .await;
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

            let temp_upload = remote_fs.temp_upload_path("".to_string()).await.unwrap();
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
    async fn int96() {
        Config::test("int96").update_config(|mut c| {
            c.partition_split_threshold = 2;
            c
        }).start_test(async move |services| {
            let service = services.sql_service;

            let _ = service.exec_query("CREATE SCHEMA foo").await.unwrap();

            let _ = service
                .exec_query("CREATE TABLE foo.values (id int, value int96)")
                .await
                .unwrap();

            service
                .exec_query("INSERT INTO foo.values (id, value) VALUES (1, 10000000000000000000000), (2, 20000000000000000000000), (3, 10000000000000220000000), (4, 12000000000000000000024), (5, 123)")
                .await
                .unwrap();

            let result = service
                .exec_query("SELECT * from foo.values")
                .await
                .unwrap();

            assert_eq!(result.get_rows()[0], Row::new(vec![TableValue::Int(1), TableValue::Int96(Int96::new(10000000000000000000000))]));
            assert_eq!(result.get_rows()[1], Row::new(vec![TableValue::Int(2), TableValue::Int96(Int96::new(20000000000000000000000))]));
            assert_eq!(result.get_rows()[2], Row::new(vec![TableValue::Int(3), TableValue::Int96(Int96::new(10000000000000220000000))]));
            assert_eq!(result.get_rows()[3], Row::new(vec![TableValue::Int(4), TableValue::Int96(Int96::new(12000000000000000000024))]));
            assert_eq!(result.get_rows()[4], Row::new(vec![TableValue::Int(5), TableValue::Int96(Int96::new(123))]));

            let result = service
                .exec_query("SELECT sum(value) from foo.values")
                .await
                .unwrap();

            assert_eq!(result.get_rows()[0], Row::new(vec![TableValue::Int96(Int96::new(52000000000000220000147))]));

            let result = service
                .exec_query("SELECT max(value), min(value) from foo.values")
                .await
                .unwrap();

            assert_eq!(result.get_rows()[0], Row::new(vec![TableValue::Int96(Int96::new(20000000000000000000000)), TableValue::Int96(Int96::new(123))]));

            let result = service
                .exec_query("SELECT value + 103, value + value, value = 12000000000000000000024 from foo.values where value = 12000000000000000000024")
                .await
                .unwrap();

            assert_eq!(result.get_rows()[0], Row::new(vec![TableValue::Int96(Int96::new(12000000000000000000127)),
            TableValue::Int96(Int96::new(2 * 12000000000000000000024)), TableValue::Boolean(true)]));

            let result = service
                .exec_query("SELECT value / 2, value * 2 from foo.values where value > 12000000000000000000024")
                .await
                .unwrap();

            assert_eq!(result.get_rows()[0], Row::new(vec![TableValue::Int96(Int96::new(10000000000000000000000)),
            TableValue::Int96(Int96::new(40000000000000000000000))]));

            let result = service
                .exec_query("SELECT * from foo.values order by value")
                .await
                .unwrap();

            assert_eq!(result.get_rows()[0], Row::new(vec![TableValue::Int(5), TableValue::Int96(Int96::new(123))]));
            assert_eq!(result.get_rows()[1], Row::new(vec![TableValue::Int(1), TableValue::Int96(Int96::new(10000000000000000000000))]));
            assert_eq!(result.get_rows()[2], Row::new(vec![TableValue::Int(3), TableValue::Int96(Int96::new(10000000000000220000000))]));
            assert_eq!(result.get_rows()[3], Row::new(vec![TableValue::Int(4), TableValue::Int96(Int96::new(12000000000000000000024))]));
            assert_eq!(result.get_rows()[4], Row::new(vec![TableValue::Int(2), TableValue::Int96(Int96::new(20000000000000000000000))]));

            let _ = service
                .exec_query("CREATE TABLE foo.values2 (id int, value int96)")
                .await
                .unwrap();

            service
                .exec_query("INSERT INTO foo.values2 (id, value) VALUES (1, 10000000000000000000000), (2, 20000000000000000000000), (3, 10000000000000000000000), (4, 20000000000000000000000), (5, 123)")
                .await
                .unwrap();

            let result = service
                .exec_query("SELECT value, count(*) from foo.values2 group by value order by value")
                .await
                .unwrap();

            assert_eq!(result.get_rows()[0], Row::new(vec![TableValue::Int96(Int96::new(123)), TableValue::Int(1)]));
            assert_eq!(result.get_rows()[1], Row::new(vec![TableValue::Int96(Int96::new(10000000000000000000000)), TableValue::Int(2)]));
            assert_eq!(result.get_rows()[2], Row::new(vec![TableValue::Int96(Int96::new(20000000000000000000000)), TableValue::Int(2)]));

            let _ = service
                .exec_query("CREATE TABLE foo.values3 (id int, value int96)")
                .await
                .unwrap();

            service
                .exec_query("INSERT INTO foo.values3 (id, value) VALUES (1, -10000000000000000000000), (2, -20000000000000000000000), (3, -10000000000000220000000), (4, -12000000000000000000024), (5, -123)")
                .await
                .unwrap();

            let result = service
                .exec_query("SELECT * from foo.values3")
                .await
                .unwrap();

            assert_eq!(result.get_rows()[0], Row::new(vec![TableValue::Int(1), TableValue::Int96(Int96::new(-10000000000000000000000))]));
            assert_eq!(result.get_rows()[1], Row::new(vec![TableValue::Int(2), TableValue::Int96(Int96::new(-20000000000000000000000))]));
            assert_eq!(result.get_rows()[2], Row::new(vec![TableValue::Int(3), TableValue::Int96(Int96::new(-10000000000000220000000))]));
            assert_eq!(result.get_rows()[3], Row::new(vec![TableValue::Int(4), TableValue::Int96(Int96::new(-12000000000000000000024))]));
            assert_eq!(result.get_rows()[4], Row::new(vec![TableValue::Int(5), TableValue::Int96(Int96::new(-123))]));

        })
            .await;
    }

    #[tokio::test]
    async fn decimal96() {
        Config::test("decimal96").update_config(|mut c| {
            c.partition_split_threshold = 2;
            c
        }).start_test(async move |services| {
            let service = services.sql_service;

            let _ = service.exec_query("CREATE SCHEMA foo").await.unwrap();

            let _ = service
                .exec_query("CREATE TABLE foo.values (id int, value decimal96)")
                .await
                .unwrap();

            service
                .exec_query("INSERT INTO foo.values (id, value) VALUES (1, 100000000000000000000.10), (2, 200000000000000000000), (3, 100000000000002200000.01), (4, 120000000000000000.10024), (5, 1.23)")
                .await
                .unwrap();

            let result = service
                .exec_query("SELECT * from foo.values")
                .await
                .unwrap();

            assert_eq!(result.get_rows()[0], Row::new(vec![TableValue::Int(1), TableValue::Decimal96(Decimal96::new(10000000000000000000010000))]));
            assert_eq!(result.get_rows()[1], Row::new(vec![TableValue::Int(2), TableValue::Decimal96(Decimal96::new(20000000000000000000000000))]));
            assert_eq!(result.get_rows()[2], Row::new(vec![TableValue::Int(3), TableValue::Decimal96(Decimal96::new(10000000000000220000001000))]));
            assert_eq!(result.get_rows()[3], Row::new(vec![TableValue::Int(4), TableValue::Decimal96(Decimal96::new(12000000000000000010024))]));
            assert_eq!(result.get_rows()[4], Row::new(vec![TableValue::Int(5), TableValue::Decimal96(Decimal96::new(123000))]));

            let result = service
                .exec_query("SELECT sum(value) from foo.values")
                .await
                .unwrap();


            assert_eq!(result.get_rows()[0], Row::new(vec![TableValue::Decimal96(Decimal96::new(40012000000000220000144024))]));

            let result = service
                .exec_query("SELECT max(value), min(value) from foo.values")
                .await
                .unwrap();

            assert_eq!(result.get_rows()[0], Row::new(vec![TableValue::Decimal96(Decimal96::new(20000000000000000000000000)), TableValue::Decimal96(Decimal96::new(123000))]));

            let result = service
                .exec_query("SELECT value + 10.103, value + value from foo.values where id = 4")
                .await
                .unwrap();


            assert_eq!(result.get_rows()[0], Row::new(vec![TableValue::Decimal96(Decimal96::new(12000000000000001020324)),
            TableValue::Decimal96(Decimal96::new(2 * 12000000000000000010024))]));

           let result = service
                .exec_query("SELECT value / 2, value * 2 from foo.values where value > 100000000000002200000")
                .await
                .unwrap();

            assert_eq!(result.get_rows()[0], Row::new(vec![TableValue::Float(1.0000000000000002e20.into()),
            TableValue::Float(4.0000000000000007e20.into())]));

           let result = service
                .exec_query("SELECT * from foo.values order by value")
                .await
                .unwrap();

            assert_eq!(result.get_rows()[0], Row::new(vec![TableValue::Int(5), TableValue::Decimal96(Decimal96::new(123000))]));
            assert_eq!(result.get_rows()[1], Row::new(vec![TableValue::Int(4), TableValue::Decimal96(Decimal96::new(12000000000000000010024))]));
            assert_eq!(result.get_rows()[2], Row::new(vec![TableValue::Int(1), TableValue::Decimal96(Decimal96::new(10000000000000000000010000))]));
            assert_eq!(result.get_rows()[3], Row::new(vec![TableValue::Int(3), TableValue::Decimal96(Decimal96::new(10000000000000220000001000))]));
            assert_eq!(result.get_rows()[4], Row::new(vec![TableValue::Int(2), TableValue::Decimal96(Decimal96::new(20000000000000000000000000))]));

              let _ = service
                .exec_query("CREATE TABLE foo.values2 (id int, value decimal(27, 2))")
                .await
                .unwrap();

            service
                .exec_query("INSERT INTO foo.values2 (id, value) VALUES (1, 100000000000000000000.10), (2, 20000000000000000000000.1), (3, 100000000000000000000.10), (4, 20000000000000000000000.1), (5, 123)")
                .await
                .unwrap();

            let result = service
                .exec_query("SELECT value, count(*) from foo.values2 group by value order by value")
                .await
                .unwrap();

            assert_eq!(result.get_rows()[0], Row::new(vec![TableValue::Decimal96(Decimal96::new(12300)), TableValue::Int(1)]));
            assert_eq!(result.get_rows()[1], Row::new(vec![TableValue::Decimal96(Decimal96::new(10000000000000000000010)), TableValue::Int(2)]));
            assert_eq!(result.get_rows()[2], Row::new(vec![TableValue::Decimal96(Decimal96::new(2000000000000000000000010)), TableValue::Int(2)]));


            let _ = service
                .exec_query("CREATE TABLE foo.values3 (id int, value decimal96)")
                .await
                .unwrap();

            service
                .exec_query("INSERT INTO foo.values3 (id, value) VALUES (1, -100000000000000000000.10), (2, -200000000000000000000), (3, -100000000000002200000.01), (4, -120000000000000000.10024), (5, -1.23)")
                .await
                .unwrap();

            let result = service
                .exec_query("SELECT * from foo.values3")
                .await
                .unwrap();

            assert_eq!(result.get_rows()[0], Row::new(vec![TableValue::Int(1), TableValue::Decimal96(Decimal96::new(-10000000000000000000010000))]));
            assert_eq!(result.get_rows()[1], Row::new(vec![TableValue::Int(2), TableValue::Decimal96(Decimal96::new(-20000000000000000000000000))]));
            assert_eq!(result.get_rows()[2], Row::new(vec![TableValue::Int(3), TableValue::Decimal96(Decimal96::new(-10000000000000220000001000))]));
            assert_eq!(result.get_rows()[3], Row::new(vec![TableValue::Int(4), TableValue::Decimal96(Decimal96::new(-12000000000000000010024))]));
            assert_eq!(result.get_rows()[4], Row::new(vec![TableValue::Int(5), TableValue::Decimal96(Decimal96::new(-123000))]));

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
    async fn flatten_union() {
        Config::test("flatten_union").start_test(async move |services| {
            let service = services.sql_service;

            let _ = service.exec_query("CREATE SCHEMA foo").await.unwrap();

            let _ = service.exec_query("CREATE TABLE foo.a (a int, b int, c int)").await.unwrap();
            let _ = service.exec_query("CREATE TABLE foo.b (a int, b int, c int)").await.unwrap();

            let _ = service.exec_query("CREATE TABLE foo.a1 (a int, b int, c int)").await.unwrap();
            let _ = service.exec_query("CREATE TABLE foo.b1 (a int, b int, c int)").await.unwrap();

            service.exec_query(
                "INSERT INTO foo.a (a, b, c) VALUES (1, 1, 1)"
            ).await.unwrap();
            service.exec_query(
                "INSERT INTO foo.b (a, b, c) VALUES (2, 2, 1)"
            ).await.unwrap();
            service.exec_query(
                "INSERT INTO foo.a1 (a, b, c) VALUES (1, 1, 2)"
            ).await.unwrap();
            service.exec_query(
                "INSERT INTO foo.b1 (a, b, c) VALUES (2, 2, 2)"
            ).await.unwrap();

            let result = service.exec_query("EXPLAIN SELECT a `sel__a`, b `sel__b`, sum(c) `sel__c` from ( \
                         select * from ( \
                                        select * from foo.a \
                                        union all \
                                        select * from foo.b \
                                        ) \
                             union all
                             select * from
                                ( \
                                        select * from foo.a1 \
                                        union all \
                                        select * from foo.b1 \
                                        union all \
                                        select * from foo.b \
                                ) \
                         ) AS `lambda` where a = 1 group by 1, 2 order by 3 desc").await.unwrap();
            match &result.get_rows()[0].values()[0] {
                TableValue::String(s) => {
                    assert_eq!(s,
                                "Sort\
                                \n  Projection, [sel__a, sel__b, sel__c]\
                                \n    Aggregate\
                                \n      ClusterSend, indices: [[1, 2, 3, 4, 2]]\
                                \n        Union\
                                \n          Filter\
                                \n            Scan foo.a, source: CubeTable(index: default:1:[1]:sort_on[a, b]), fields: *\
                                \n          Filter\
                                \n            Scan foo.b, source: CubeTable(index: default:2:[2]:sort_on[a, b]), fields: *\
                                \n          Filter\
                                \n            Scan foo.a1, source: CubeTable(index: default:3:[3]:sort_on[a, b]), fields: *\
                                \n          Filter\
                                \n            Scan foo.b1, source: CubeTable(index: default:4:[4]:sort_on[a, b]), fields: *\
                                \n          Filter\
                                \n            Scan foo.b, source: CubeTable(index: default:2:[2]:sort_on[a, b]), fields: *"

                               );
                }
                _ => assert!(false),
            };

            let result = service.exec_query("EXPLAIN SELECT a `sel__a`, b `sel__b`, sum(c) `sel__c` from ( \
                         select * from ( \
                                        select * from foo.a\
                                        ) \
                             union all
                             select * from
                                ( \
                                        select * from foo.a1 \
                                        union all \
                                        select * from foo.b1 \
                                ) \
                            union all
                            select * from foo.b \
                         ) AS `lambda` where a = 1 group by 1, 2 order by 3 desc").await.unwrap();
            match &result.get_rows()[0].values()[0] {
                TableValue::String(s) => {
                    assert_eq!(s,
                                "Sort\
                                \n  Projection, [sel__a, sel__b, sel__c]\
                                \n    Aggregate\
                                \n      ClusterSend, indices: [[1, 3, 4, 2]]\
                                \n        Union\
                                \n          Filter\
                                \n            Scan foo.a, source: CubeTable(index: default:1:[1]:sort_on[a, b]), fields: *\
                                \n          Filter\
                                \n            Scan foo.a1, source: CubeTable(index: default:3:[3]:sort_on[a, b]), fields: *\
                                \n          Filter\
                                \n            Scan foo.b1, source: CubeTable(index: default:4:[4]:sort_on[a, b]), fields: *\
                                \n          Filter\
                                \n            Scan foo.b, source: CubeTable(index: default:2:[2]:sort_on[a, b]), fields: *"

                               );
                }
                _ => assert!(false),
            };
            let result = service.exec_query("EXPLAIN SELECT a `sel__a`, b `sel__b`, sum(c) `sel__c` from ( \
                         select * from ( \
                                        select * from foo.a where 1 = 0\
                                        ) \
                             union all
                             select * from
                                ( \
                                        select * from foo.a1 \
                                        union all \
                                        select * from foo.b1 \
                                ) \
                            union all
                            select * from foo.b \
                         ) AS `lambda` where a = 1 group by 1, 2 order by 3 desc").await.unwrap();
            match &result.get_rows()[0].values()[0] {
                TableValue::String(s) => {
                    assert_eq!(s,
                                "Sort\
                                \n  Projection, [sel__a, sel__b, sel__c]\
                                \n    Aggregate\
                                \n      ClusterSend, indices: [[1, 3, 4, 2]]\
                                \n        Union\
                                \n          Filter\
                                \n            Filter\
                                \n              Scan foo.a, source: CubeTable(index: default:1:[1]:sort_on[a, b]), fields: *\
                                \n          Filter\
                                \n            Scan foo.a1, source: CubeTable(index: default:3:[3]:sort_on[a, b]), fields: *\
                                \n          Filter\
                                \n            Scan foo.b1, source: CubeTable(index: default:4:[4]:sort_on[a, b]), fields: *\
                                \n          Filter\
                                \n            Scan foo.b, source: CubeTable(index: default:2:[2]:sort_on[a, b]), fields: *"

                               );
                }
                _ => assert!(false),
            };
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
                        path.to_str().unwrap().to_string(),
                        chunk.get_row().get_full_name(chunk.get_id()),
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

                // TODO API to wait for all jobs to be completed and all events processed
                Delay::new(Duration::from_millis(500)).await;

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
    async fn check_memory_test() {
        Config::test("check_memory_test")
            .update_config(|mut c| {
                c.partition_split_threshold = 25;
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

                for _ in 0..2 {
                    let t = (0..100).map(|i| format!("({i})")).join(", ");
                    service
                        .exec_query(&format!("INSERT INTO foo.numbers (num) VALUES {}", t))
                        .await
                        .unwrap();
                }

                let mut opts = PPOptions::default();
                opts.show_check_memory_nodes = true;

                let plans = service
                    .plan_query("SELECT sum(num) from foo.numbers where num = 50")
                    .await
                    .unwrap();
                let plan_regexp = Regex::new(r"ParquetScan.*\.parquet").unwrap();

                let expected = "Projection, [SUM(foo.numbers.num)@0:SUM(num)]\
                \n  FinalHashAggregate\
                \n    Worker\
                \n      PartialHashAggregate\
                \n        Filter\
                \n          MergeSort\
                \n            Scan, index: default:1:[1]:sort_on[num], fields: *\
                \n              FilterByKeyRange\
                \n                CheckMemoryExec\
                \n                  ParquetScan\
                \n              FilterByKeyRange\
                \n                CheckMemoryExec\
                \n                  ParquetScan";
                let plan = pp_phys_plan_ext(plans.worker.as_ref(), &opts);
                let p = plan_regexp.replace_all(&plan, "ParquetScan");
                println!("pp {}", p);
                assert_eq!(p, expected);
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
                Delay::new(Duration::from_millis(4000)).await;

                let remote_fs = services.injector.get_service_typed::<dyn RemoteFs>().await;
                let files = remote_fs
                    .list("".to_string())
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
                c.compaction_chunks_count_threshold = 2;
                c.not_used_timeout = 0;
                c.compaction_in_memory_chunks_count_threshold = 5;
                c.compaction_in_memory_chunks_max_lifetime_threshold = 1;
                c
            })
            .start_test(async move |services| {
                let service = services.sql_service;
                let compaction_service = services
                    .injector
                    .get_service_typed::<dyn CompactionService>()
                    .await;

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

                compaction_service
                    .compact_in_memory_chunks(1)
                    .await
                    .unwrap();

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
                assert_eq!(chunks.first().unwrap().get_row().in_memory(), true);
                Delay::new(Duration::from_millis(2000)).await;
                for i in 0..6 {
                    service
                        .exec_query(&format!(
                            "INSERT INTO foo.numbers (a, num, __seq) VALUES ({}, {}, {})",
                            i + 1,
                            i + 1,
                            i + 1
                        ))
                        .await
                        .unwrap();
                }
                compaction_service
                    .compact_in_memory_chunks(1)
                    .await
                    .unwrap();
                Delay::new(Duration::from_millis(2000)).await;
                let active_partitions = services
                    .meta_store
                    .get_active_partitions_by_index_id(1)
                    .await
                    .unwrap();
                assert_eq!(active_partitions.len(), 1);
                let partition = active_partitions.first().unwrap();
                assert_eq!(partition.get_row().main_table_row_count(), 6);
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
    async fn disk_space_limit() {
        Config::test("disk_space_limit")
            .update_config(|mut c| {
                c.partition_split_threshold = 1000000;
                c.compaction_chunks_count_threshold = 100;
                c.max_disk_space = 3000;
                c.select_workers = vec!["127.0.0.1:24308".to_string()];
                c.metastore_bind_address = Some("127.0.0.1:25314".to_string());
                c
            })
            .start_test(async move |services| {
                let service = services.sql_service;

                Config::test("disk_space_limit_worker_1")
                    .update_config(|mut c| {
                        c.worker_bind_address = Some("127.0.0.1:24308".to_string());
                        c.server_name = "127.0.0.1:24308".to_string();
                        c.max_disk_space = 3000;
                        c.metastore_remote_address = Some("127.0.0.1:25314".to_string());
                        c.store_provider = FileStoreProvider::Filesystem {
                            remote_dir: Some(env::current_dir()
                                .unwrap()
                                .join("disk_space_limit-upstream")),
                        };
                        c
                    })
                    .start_test_worker(async move |_| {
                        let paths = {
                            let dir = env::temp_dir();

                            let path_1 = dir.clone().join("foo-cluster-1.csv");
                            let path_2 = dir.clone().join("foo-cluster-2.csv.gz");
                            let mut file = File::create(path_1.clone()).unwrap();

                            file.write_all("id,city,arr,t\n".as_bytes()).unwrap();
                            for i in 0..50
                            {
                                file.write_all(format!("{},\"New York\",\"[\"\"\"\"]\",2021-01-24 19:12:23.123 UTC\n", i).as_bytes()).unwrap();
                            }


                            let mut file = GzipEncoder::new(BufWriter::new(tokio::fs::File::create(path_2.clone()).await.unwrap()));

                            file.write_all("id,city,arr,t\n".as_bytes()).await.unwrap();
                            for i in 0..50
                            {
                                file.write_all(format!("{},San Francisco,\"[\"\"Foo\"\",\"\"Bar\"\",\"\"FooBar\"\"]\",\"2021-01-24 12:12:23 UTC\"\n", i).as_bytes()).await.unwrap();
                            }

                            file.shutdown().await.unwrap();

                            vec![path_1, path_2]
                        };

                        let _ = service.exec_query("CREATE SCHEMA IF NOT EXISTS Foo").await.unwrap();
                        let _ = service.exec_query(
                            &format!(
                                "CREATE TABLE Foo.Persons (id int, city text, t timestamp, arr text) INDEX persons_city (`city`, `id`) LOCATION {}",
                                paths.iter().map(|p| format!("'{}'", p.to_string_lossy())).join(",")
                            )
                        ).await.unwrap();

                        let res = service.exec_query(
                            &format!(
                                "CREATE TABLE Foo.Persons2 (id int, city text, t timestamp, arr text) INDEX persons_city (`city`, `id`) LOCATION {}",
                                paths.iter().map(|p| format!("'{}'", p.to_string_lossy())).join(",")
                            )
                        ).await;
                        if let Err(err) = res {
                            assert!(err.message.starts_with("Exceeded available storage space:"));
                        } else {
                            assert!(false);
                        }

                    })
                    .await;
            })
            .await;
    }

    #[tokio::test]
    async fn disk_space_limit_per_worker() {
        Config::test("disk_space_limit_per_worker")
            .update_config(|mut c| {
                c.partition_split_threshold = 1000000;
                c.compaction_chunks_count_threshold = 100;
                c.max_disk_space_per_worker = 3000;
                c.select_workers = vec!["127.0.0.1:24309".to_string()];
                c.metastore_bind_address = Some("127.0.0.1:25315".to_string());
                c
            })
            .start_test(async move |services| {
                let service = services.sql_service;

                Config::test("disk_space_limit_per_worker_worker_1")
                    .update_config(|mut c| {
                        c.worker_bind_address = Some("127.0.0.1:24309".to_string());
                        c.server_name = "127.0.0.1:24309".to_string();
                        c.max_disk_space_per_worker = 3000;
                        c.metastore_remote_address = Some("127.0.0.1:25315".to_string());
                        c.store_provider = FileStoreProvider::Filesystem {
                            remote_dir: Some(env::current_dir()
                                .unwrap()
                                .join("disk_space_limit_per_worker-upstream")),
                        };
                        c
                    })
                    .start_test_worker(async move |_| {
                        let paths = {
                            let dir = env::temp_dir();

                            let path_1 = dir.clone().join("foo-cluster-1.csv");
                            let path_2 = dir.clone().join("foo-cluster-2.csv.gz");
                            let mut file = File::create(path_1.clone()).unwrap();

                            file.write_all("id,city,arr,t\n".as_bytes()).unwrap();
                            for i in 0..50
                            {
                                file.write_all(format!("{},\"New York\",\"[\"\"\"\"]\",2021-01-24 19:12:23.123 UTC\n", i).as_bytes()).unwrap();
                            }


                            let mut file = GzipEncoder::new(BufWriter::new(tokio::fs::File::create(path_2.clone()).await.unwrap()));

                            file.write_all("id,city,arr,t\n".as_bytes()).await.unwrap();
                            for i in 0..50
                            {
                                file.write_all(format!("{},San Francisco,\"[\"\"Foo\"\",\"\"Bar\"\",\"\"FooBar\"\"]\",\"2021-01-24 12:12:23 UTC\"\n", i).as_bytes()).await.unwrap();
                            }

                            file.shutdown().await.unwrap();

                            vec![path_1, path_2]
                        };

                        let _ = service.exec_query("CREATE SCHEMA IF NOT EXISTS Foo").await.unwrap();
                        let _ = service.exec_query(
                            &format!(
                                "CREATE TABLE Foo.Persons (id int, city text, t timestamp, arr text) INDEX persons_city (`city`, `id`) LOCATION {}",
                                paths.iter().map(|p| format!("'{}'", p.to_string_lossy())).join(",")
                            )
                        ).await.unwrap();

                        let res = service.exec_query(
                            &format!(
                                "CREATE TABLE Foo.Persons2 (id int, city text, t timestamp, arr text) INDEX persons_city (`city`, `id`) LOCATION {}",
                                paths.iter().map(|p| format!("'{}'", p.to_string_lossy())).join(",")
                            )
                        ).await;
                        if let Err(err) = res {
                            assert!(err.message.contains("Exceeded available storage space on worker"));
                        } else {
                            assert!(false);
                        }

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
            config.select_worker_pool_size = 1;
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
            let mut intervals_set = new_partitions.into_iter()
                .map(|p| (p.get_row().get_min_val().clone(), p.get_row().get_max_val().clone(), p.get_row().get_min().clone(), p.get_row().get_max().clone()))
                .collect::<Vec<_>>();
            intervals_set.sort_by(|(min_a, _, _, _), (min_b, _, _, _)| cmp_min_rows(1, min_a.as_ref(), min_b.as_ref()));
            let mut expected = vec![
                (
                    None, Some(Row::new(vec![TableValue::Int(2)])),
                    Some(Row::new(vec![TableValue::Null])), Some(Row::new(vec![TableValue::Int(1)]))
                    ),
                (
                    Some(Row::new(vec![TableValue::Int(2)])), Some(Row::new(vec![TableValue::Int(10)])),
                    Some(Row::new(vec![TableValue::Int(2)])), Some(Row::new(vec![TableValue::Int(5)]))
                ),
                (
                    Some(Row::new(vec![TableValue::Int(10)])), Some(Row::new(vec![TableValue::Int(27)])),
                    Some(Row::new(vec![TableValue::Int(10)])), Some(Row::new(vec![TableValue::Int(25)]))
                ),
                (
                    Some(Row::new(vec![TableValue::Int(27)])), None,
                    Some(Row::new(vec![TableValue::Int(27)])), Some(Row::new(vec![TableValue::Int(29)])),
                ),
            ].into_iter().collect::<Vec<_>>();
            expected.sort_by(|(min_a, _, _, _), (min_b, _, _, _)| cmp_min_rows(1, min_a.as_ref(), min_b.as_ref()));
            assert_eq!(intervals_set, expected);

            let result = service.exec_query("SELECT count(*) from foo.table").await.unwrap();

            assert_eq!(result.get_rows()[0], Row::new(vec![TableValue::Int(20)]));
        }).await;
    }

    #[test]
    fn create_table_with_temp_file() {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_stack_size(4 * 1024 * 1024)
            .build()
            .unwrap()
            .block_on( async {
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
                remote_fs.upload_file(path_2.to_str().unwrap().to_string(), "temp-uploads/foo-3.csv.gz".to_string()).await.unwrap();

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
            )
    }

    #[tokio::test]
    async fn explain_meta_logical_plan() {
        Config::run_test("explain_meta_logical_plan", async move |services| {
            let service = services.sql_service;
            service.exec_query("CREATE SCHEMA foo").await.unwrap();

            let result = service.exec_query(
                "EXPLAIN SELECT table_name FROM information_schema.tables WHERE table_schema = 'foo'"
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
                "Projection, [information_schema.tables.table_name]\
                \n  Filter\
                \n    Scan information_schema.tables, source: InfoSchemaTableProvider, fields: [table_schema, table_name]"
            );
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

            Config::test("explain_analyze_worker_1").update_config(|mut config| {
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
                        .upload_file(
                            path_2.to_str().unwrap().to_string(),
                            "temp-uploads/orders.csv.gz".to_string(),
                        )
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

    #[tokio::test]
    async fn validate_ksql_location() {
        Config::test("validate_ksql_location").update_config(|mut c| {
            c.partition_split_threshold = 2;
            c
        }).start_test(async move |services| {
            let service = services.sql_service;

            let _ = service.exec_query("CREATE SCHEMA test").await.unwrap();

            service
                .exec_query("CREATE SOURCE OR UPDATE ksql AS 'ksql' VALUES (user = 'foo', password = 'bar', url = 'http://foo.com')")
                .await
                .unwrap();

            let _ = service
                .exec_query("CREATE TABLE test.events_by_type_1 (`EVENT` text, `KSQL_COL_0` int) WITH (select_statement = 'SELECT * FROM EVENTS_BY_TYPE WHERE time >= \\'2022-01-01\\' AND time < \\'2022-02-01\\'') unique key (`EVENT`) location 'stream://ksql/EVENTS_BY_TYPE'")
                .await
                .unwrap();

            let _ = service
                .exec_query("CREATE TABLE test.events_by_type_2 (`EVENT` text, `KSQL_COL_0` int) WITH (select_statement = 'SELECT * FROM EVENTS_BY_TYPE') unique key (`EVENT`) location 'stream://ksql/EVENTS_BY_TYPE'")
                .await
                .unwrap();

            let _ = service
                .exec_query("CREATE TABLE test.events_by_type_3 (`EVENT` text, `KSQL_COL_0` int) unique key (`EVENT`) location 'stream://ksql/EVENTS_BY_TYPE'")
                .await
                .unwrap();

            let _ = service
                .exec_query("CREATE TABLE test.events_by_type_fail_1 (`EVENT` text, `KSQL_COL_0` int) WITH (select_statement = 'SELECT * EVENTS_BY_TYPE WHERE time >= \\'2022-01-01\\' AND time < \\'2022-02-01\\'') unique key (`EVENT`) location 'stream://ksql/EVENTS_BY_TYPE'")
                .await
                .expect_err("Validation should fail");

            let _ = service
                .exec_query("CREATE TABLE test.events_by_type_fail_2 (`EVENT` text, `KSQL_COL_0` int) WITH (select_statement = 'SELECT * FROM (SELECT * FROM EVENTS_BY_TYPE WHERE time >= \\'2022-01-01\\' AND time < \\'2022-02-01\\')') unique key (`EVENT`) location 'stream://ksql/EVENTS_BY_TYPE'")
                .await
                .expect_err("Validation should fail");
        }).await;
    }

    #[tokio::test]
    async fn create_stream_table_with_projection() {
        Config::test("create_stream_table_with_projection").update_config(|mut c| {
            c.partition_split_threshold = 2;
            c
        }).start_test(async move |services| {
            let service = services.sql_service;
            let metastore = services.meta_store;

            let _ = service.exec_query("CREATE SCHEMA test").await.unwrap();

            service
                .exec_query("CREATE SOURCE OR UPDATE kafka AS 'kafka' VALUES (user = 'foo', password = 'bar', host = 'localhost:9092')")
                .await
                .unwrap();

            let _ = service
                .exec_query("CREATE TABLE test.events_1 (a int, b int) WITH (\
                select_statement = 'SELECT a as a, b + c as b FROM EVENTS_BY_TYPE WHERE c > 10',\
                source_table = 'CREATE TABLE events1 (a int, b int, c int)'
                            ) unique key (`a`) location 'stream://kafka/EVENTS_BY_TYPE/0'")
                .await
                .unwrap();
            let table = metastore.get_table("test".to_string(), "events_1".to_string()).await.unwrap();
            assert_eq!(
                table.get_row().source_columns(),
                &Some(vec![
                     Column::new("a".to_string(), ColumnType::Int, 0),
                     Column::new("b".to_string(), ColumnType::Int, 1),
                     Column::new("c".to_string(), ColumnType::Int, 2),
                ])
            );
            let _ = service
                .exec_query("CREATE TABLE test.events_1 (a int, b int) WITH (\
                select_statement = 'SELECT a as a, b + c  as b FROM EVENTS_BY_TYPE WHERE c > 10',\
                source_table = 'TABLE events1 (a int, b int, c int)'
                            ) unique key (`a`) location 'stream://kafka/EVENTS_BY_TYPE/0'")
                    .await
                    .expect_err("Validation should fail");

            let _ = service
                .exec_query("CREATE TABLE test.events_1 (a int, b int) WITH (\
                select_statement = 'SELECT a as a, b + c as b FROM EVENTS_BY_TYPE WHERE c > 10',\
                source_table = 'CREATE TABLE events1 (a int, b int, c int'
                            ) unique key (`a`) location 'stream://kafka/EVENTS_BY_TYPE/0'")
                    .await
                    .expect_err("Validation should fail");


        }).await;
    }

    #[tokio::test]
    async fn trace_obj_for_streaming_table() {
        Config::test("trace_obj_for_streaming_table").start_test(async move |services| {
            let service = services.sql_service;
            let meta_store = services.meta_store;

            let _ = service.exec_query("CREATE SCHEMA test").await.unwrap();

            service
                .exec_query("CREATE SOURCE OR UPDATE ksql AS 'ksql' VALUES (user = 'foo', password = 'bar', url = 'http://foo.com')").await.unwrap();
            let context = SqlQueryContext::default().with_trace_obj(Some("{\"test\":\"context\"}".to_string()));

            let _ = service
                .exec_query_with_context(context, "CREATE TABLE test.table_1 (`EVENT` text, `KSQL_COL_0` int) unique key (`EVENT`) location 'stream://ksql/EVENTS_BY_TYPE'")
                .await
                .unwrap();

            let table = meta_store.get_table("test".to_string(), "table_1".to_string()).await.unwrap();
            let trace_obj = meta_store.get_trace_obj_by_table_id(table.get_id()).await.unwrap();
            assert!(trace_obj.is_some());
            assert_eq!(trace_obj.unwrap(), "{\"test\":\"context\"}".to_string());

            let _ = service
                .exec_query("CREATE TABLE test.table_2 (`EVENT` text, `KSQL_COL_0` int) unique key (`EVENT`)")
                .await
                .unwrap();

            let table = meta_store.get_table("test".to_string(), "table_2".to_string()).await.unwrap();
            let trace_obj = meta_store.get_trace_obj_by_table_id(table.get_id()).await.unwrap();
            println!("tobj {:?}", trace_obj);
            assert!(trace_obj.is_none());

        }).await;
    }

    #[tokio::test]
    async fn total_count_over_groupping() {
        Config::test("total_count_over_groupping")
            .start_test(async move |services| {
                let service = services.sql_service;

                let _ = service.exec_query("CREATE SCHEMA test").await.unwrap();

                service
                    .exec_query("CREATE TABLE test.test (id int, created timestamp, value int)")
                    .await
                    .unwrap();
                service
                    .exec_query("CREATE TABLE test.test1 (id int, created timestamp, value int)")
                    .await
                    .unwrap();

                service
                    .exec_query(
                        "INSERT INTO test.test (id, created, value) values \
                            (1, '2022-01-01T00:00:00Z', 1),\
                            (2, '2022-01-02T00:00:00Z', 1),\
                            (1, '2022-02-03T00:00:00Z', 1),\
                            (2, '2022-02-03T00:00:00Z', 2),\
                            (2, '2022-01-02T00:00:00Z', 1)\
                            ",
                    )
                    .await
                    .unwrap();
                service
                    .exec_query(
                        "INSERT INTO test.test1 (id, created, value) values \
                            (1, '2022-01-01T00:00:00Z', 1),\
                            (2, '2022-01-02T00:00:00Z', 1),\
                            (1, '2022-02-03T00:00:00Z', 1),\
                            (2, '2022-02-03T00:00:00Z', 2),\
                            (2, '2022-01-02T00:00:00Z', 1)\
                            ",
                    )
                    .await
                    .unwrap();
                let res = service
                    .exec_query(
                        "SELECT count(*) cnt FROM \
                                (\
                                 SELECT \
                                 date_trunc('month', created) as month,
                                 sum(value) as v
                                 from test.test
                                 group by 1
                                 order by 2
                                 ) tmp",
                    )
                    .await
                    .unwrap();
                assert_eq!(res.get_rows(), &vec![Row::new(vec![TableValue::Int(2)])]);

                let res = service
                    .exec_query(
                        "SELECT count(*) cnt FROM \
                                (\
                                 SELECT \
                                 created as month,
                                 sum(value) as v
                                 from test.test
                                 group by 1
                                 order by 2
                                 ) tmp",
                    )
                    .await
                    .unwrap();
                assert_eq!(res.get_rows(), &vec![Row::new(vec![TableValue::Int(3)])]);

                let res = service
                    .exec_query(
                        "SELECT count(*) cnt FROM \
                        (\
                        SELECT \
                        id id,
                        created created,
                        sum(value) value
                        from (
                            select * from test.test
                            union all
                            select * from test.test1
                            )
                        group by 1, 2
                        ) tmp",
                    )
                    .await
                    .unwrap();
                assert_eq!(res.get_rows(), &vec![Row::new(vec![TableValue::Int(4)])]);

                let res = service
                    .exec_query(
                        "SELECT count(*) cnt FROM \
                                (\
                                 SELECT \
                                 id id,
                                 date_trunc('month', created) as month,
                                 sum(value) as v,
                                 sum(id)
                                 from test.test
                                 group by 1, 2
                                 order by 1, 2
                                 ) tmp",
                    )
                    .await
                    .unwrap();
                assert_eq!(res.get_rows(), &vec![Row::new(vec![TableValue::Int(4)])]);
            })
            .await;

        //assert_eq!(res.get_rows(), &vec![Row::new(vec![TableValue::Int(2)])]);
    }

    #[tokio::test]
    async fn total_count_over_single_row() {
        Config::test("total_count_over_single_row")
            .start_test(async move |services| {
                let service = services.sql_service;

                let _ = service.exec_query("CREATE SCHEMA test").await.unwrap();

                service
                    .exec_query("CREATE TABLE test.test (idd int, value int)")
                    .await
                    .unwrap();

                service
                    .exec_query(
                        "INSERT INTO test.test (idd, value) values \
                            (1, 10)\
                            ",
                    )
                    .await
                    .unwrap();
                let res = service
                    .exec_query(
                        "SELECT count(*) cnt FROM \
                                (\
                                 SELECT \
                                 sum(value) as s
                                 from test.test
                                 ) tmp",
                    )
                    .await
                    .unwrap();
                assert_eq!(res.get_rows(), &vec![Row::new(vec![TableValue::Int(1)])]);
            })
            .await;

        //assert_eq!(res.get_rows(), &vec![Row::new(vec![TableValue::Int(2)])]);
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
