use std::collections::HashMap;
use std::io;

use std::sync::{Arc, RwLock as RwLockSync};
use std::time::SystemTime;

use async_trait::async_trait;

use datafusion::execution::dataframe_impl::DataFrameImpl;
use datafusion::prelude::DataFrame as DFDataFrame;

use log::debug;
use log::error;
use log::trace;

use msql_srv::*;

use serde_json::json;
use tokio::net::TcpListener;
use tokio::sync::{watch, RwLock};

use crate::compile::convert_sql_to_cube_query;
use crate::compile::convert_statement_to_cube_query;
use crate::compile::parser::parse_sql_to_statement;
use crate::config::processing_loop::ProcessingLoop;
use crate::mysql::dataframe::batch_to_dataframe;
use crate::transport::TransportService;
use crate::transport::V1CubeMetaExt;
use crate::CubeError;
use sqlparser::ast;

use super::dataframe;
use super::server_manager::ServerManager;
use super::AuthContext;
use super::SqlAuthService;

#[derive(Debug)]
struct PreparedStatements {
    id: u32,
    statements: HashMap<u32, String>,
}

impl PreparedStatements {
    pub fn new() -> Self {
        Self {
            id: 1,
            statements: HashMap::new(),
        }
    }
}

#[derive(Debug)]
pub struct ConnectionProperties {
    user: Option<String>,
    database: Option<String>,
}

impl ConnectionProperties {
    pub fn new(user: Option<String>, database: Option<String>) -> Self {
        Self { user, database }
    }
}

#[derive(Debug)]
pub struct ConnectionState {
    // connection id, it's immutable
    pub connection_id: u32,
    // Connection properties
    properties: RwLockSync<ConnectionProperties>,
    // @todo Remove RWLock after split of Connection & SQLWorker
    // Context for Transport
    auth_context: RwLockSync<Option<AuthContext>>,
}

impl ConnectionState {
    pub fn new(
        connection_id: u32,
        properties: ConnectionProperties,
        auth_context: Option<AuthContext>,
    ) -> Self {
        Self {
            connection_id,
            properties: RwLockSync::new(properties),
            auth_context: RwLockSync::new(auth_context),
        }
    }

    pub fn user(&self) -> Option<String> {
        let guard = self.properties.read().expect("test");
        guard.user.clone()
    }

    pub fn set_user(&self, user: Option<String>) {
        let mut guard = self.properties.write().expect("test");
        guard.user = user;
    }

    pub fn database(&self) -> Option<String> {
        let guard = self.properties.read().expect("test");
        guard.database.clone()
    }

    pub fn set_database(&self, database: Option<String>) {
        let mut guard = self.properties.write().expect("test");
        guard.database = database;
    }

    pub fn auth_context(&self) -> Option<AuthContext> {
        let guard = self.auth_context.read().expect("test");
        guard.clone()
    }

    pub fn set_auth_context(&self, auth_context: Option<AuthContext>) {
        let mut guard = self.auth_context.write().expect("test");
        *guard = auth_context;
    }
}

#[derive(Debug)]
struct Connection {
    server: Arc<ServerManager>,
    // Props for execution queries
    state: Arc<ConnectionState>,
    // Prepared statements
    statements: Arc<RwLock<PreparedStatements>>,
}

enum QueryResponse {
    Ok(StatusFlags),
    ResultSet(StatusFlags, Arc<dataframe::DataFrame>),
}

impl Connection {
    // This method write response back to client after execution
    async fn handle_query<'a, W: io::Write + Send>(
        &'a mut self,
        query: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> Result<(), io::Error> {
        match self.execute_query(query).await {
            Err(e) => {
                error!("Error during processing {}: {}", query, e.to_string());
                results.error(ErrorKind::ER_INTERNAL_ERROR, e.message.as_bytes())?;

                Ok(())
            }
            Ok(QueryResponse::Ok(status)) => {
                results.completed(0, 0, status)?;
                Ok(())
            }
            Ok(QueryResponse::ResultSet(_, data_frame)) => {
                let columns = data_frame
                    .get_columns()
                    .iter()
                    .map(|c| Column {
                        table: "result".to_string(), // TODO
                        column: c.get_name(),
                        coltype: c.get_type(),
                        colflags: c.get_flags(),
                    })
                    .collect::<Vec<_>>();

                let mut rw = results.start(&columns)?;

                for row in data_frame.get_rows().iter() {
                    for (_i, value) in row.values().iter().enumerate() {
                        match value {
                            dataframe::TableValue::String(s) => rw.write_col(s)?,
                            dataframe::TableValue::Timestamp(s) => rw.write_col(s.to_string())?,
                            dataframe::TableValue::Boolean(s) => rw.write_col(s.to_string())?,
                            dataframe::TableValue::Float64(s) => rw.write_col(s)?,
                            dataframe::TableValue::Int64(s) => rw.write_col(s)?,
                            dataframe::TableValue::Null => rw.write_col(Option::<String>::None)?,
                        }
                    }

                    rw.end_row()?;
                }

                rw.finish()?;

                Ok(())
            }
        }
    }

    // This method executes query and return it as DataFrame
    async fn execute_query<'a>(&'a mut self, query: &'a str) -> Result<QueryResponse, CubeError> {
        let _start = SystemTime::now();

        let query = query.replace("SELECT FROM", "SELECT * FROM");

        let query_lower = query.to_lowercase();
        let query_lower = query_lower.replace("db.`", "");
        let query_lower = query_lower.replace("`", "");

        let ignore = match query_lower.as_str() {
            "rollback" => true,
            "commit" => true,
            // DataGrip workaround
            "set character_set_results = utf8" => true,
            "set character_set_results = latin1" => true,
            "set autocommit=1" => true,
            "set sql_mode='strict_trans_tables'" => true,
            "set sql_select_limit=501" => true,
            _ => false,
        };

        if query_lower.eq("set autocommit=1, sql_mode = concat(@@sql_mode,',strict_trans_tables')")
        {
            return Ok(QueryResponse::Ok(
                StatusFlags::SERVER_STATUS_AUTOCOMMIT | StatusFlags::SERVER_SESSION_STATE_CHANGED,
            ));
        } else if query_lower.eq("select cast('test plain returns' as char(60)) as anon_1") {
            return Ok(
                QueryResponse::ResultSet(StatusFlags::empty(), Arc::new(
                    dataframe::DataFrame::new(
                        vec![dataframe::Column::new(
                            "anon_1".to_string(),
                            ColumnType::MYSQL_TYPE_STRING,
                            ColumnFlags::empty(),
                        )],
                        vec![dataframe::Row::new(vec![
                            dataframe::TableValue::String("test plain returns".to_string())
                        ])]
                    )
                ),)
            )
        } else if query_lower.eq("select cast('test unicode returns' as char(60)) as anon_1") {
            return Ok(
                QueryResponse::ResultSet(StatusFlags::empty(), Arc::new(
                    dataframe::DataFrame::new(
                        vec![dataframe::Column::new(
                            "anon_1".to_string(),
                            ColumnType::MYSQL_TYPE_STRING,
                            ColumnFlags::empty(),
                        )],
                        vec![dataframe::Row::new(vec![
                            dataframe::TableValue::String("test plain returns".to_string())
                        ])]
                    )
                ),)
            )
        } else if query_lower.eq("select cast('test collated returns' as char character set utf8mb4) collate utf8mb4_bin as anon_1") {
            return Ok(
                QueryResponse::ResultSet(StatusFlags::empty(), Arc::new(
                    dataframe::DataFrame::new(
                        vec![dataframe::Column::new(
                            "anon_1".to_string(),
                            ColumnType::MYSQL_TYPE_STRING,
                            ColumnFlags::empty(),
                        )],
                        vec![dataframe::Row::new(vec![
                            dataframe::TableValue::String("test collated returns".to_string())
                        ])]
                    )
                ),)
            )
        } else if query_lower.starts_with("describe") || query_lower.starts_with("explain") {
            let stmt = parse_sql_to_statement(&query)?;
            match stmt {
                ast::Statement::ExplainTable { table_name, .. } => {
                    let table_name_filter = if table_name.0.len() == 2 {
                        &table_name.0[1].value
                    } else {
                        &table_name.0[0].value
                    };

                    let ctx = self.server.transport
                        .meta(&self.auth_context()?)
                        .await?;

                    if let Some(cube) = ctx.cubes.iter().find(|c| c.name.eq(table_name_filter)) {
                        let rows = cube.get_columns().iter().map(|column| dataframe::Row::new(
                            vec![
                                dataframe::TableValue::String(column.get_name().clone()),
                                dataframe::TableValue::String(column.get_column_type().clone()),
                                dataframe::TableValue::String(if column.mysql_can_be_null() { "Yes".to_string() } else { "No".to_string() }),
                                dataframe::TableValue::String("".to_string()),
                                dataframe::TableValue::Null,
                                dataframe::TableValue::String("".to_string()),
                            ]
                        )).collect();


                        return Ok(QueryResponse::ResultSet(StatusFlags::empty(), Arc::new(dataframe::DataFrame::new(
                            vec![
                                dataframe::Column::new(
                                    "Field".to_string(),
                                    ColumnType::MYSQL_TYPE_STRING,
                                    ColumnFlags::empty(),
                                ),
                                dataframe::Column::new(
                                    "Type".to_string(),
                                    ColumnType::MYSQL_TYPE_STRING,
                                    ColumnFlags::empty(),
                                ),
                                dataframe::Column::new(
                                    "Null".to_string(),
                                    ColumnType::MYSQL_TYPE_STRING,
                                    ColumnFlags::empty(),
                                ),
                                dataframe::Column::new(
                                    "Key".to_string(),
                                    ColumnType::MYSQL_TYPE_STRING,
                                    ColumnFlags::empty(),
                                ),
                                dataframe::Column::new(
                                    "Default".to_string(),
                                    ColumnType::MYSQL_TYPE_STRING,
                                    ColumnFlags::empty(),
                                ),
                                dataframe::Column::new(
                                    "Extra".to_string(),
                                    ColumnType::MYSQL_TYPE_STRING,
                                    ColumnFlags::empty(),
                                )
                            ],
                            rows
                        ))))
                    } else {
                        return Err(CubeError::internal("Unknown table".to_string()))
                    }
                },
                ast::Statement::Explain { statement, .. } => {
                    let ctx = self.server.transport
                        .meta(&self.auth_context()?)
                    .await?;

                    let plan = convert_statement_to_cube_query(&statement, Arc::new(ctx), self.state.clone())?;

                    return Ok(QueryResponse::ResultSet(StatusFlags::empty(), Arc::new(dataframe::DataFrame::new(
                        vec![
                            dataframe::Column::new(
                                "Execution Plan".to_string(),
                                ColumnType::MYSQL_TYPE_STRING,
                                ColumnFlags::empty(),
                            ),
                        ],
                        vec![dataframe::Row::new(vec![
                            dataframe::TableValue::String(
                                plan.print(true)?
                            )
                        ])]
                    ))))
                },
                _ => {
                    return Err(CubeError::internal("Unexpected type in ExplainTable".to_string()))
                }
            }
        } else if !ignore {
            trace!("query was not detected");

            let ctx = self.server.transport
                .meta(&self.auth_context()?)
                .await?;

            let plan = convert_sql_to_cube_query(&query, Arc::new(ctx),self.state.clone())?;
            match plan {
                crate::compile::QueryPlan::MetaOk(status) => {
                    return Ok(QueryResponse::Ok(status));
                },
                crate::compile::QueryPlan::MetaTabular(status, data_frame) => {
                    return Ok(QueryResponse::ResultSet(status, data_frame));
                },
                crate::compile::QueryPlan::DataFushionSelect(status, plan, ctx) => {
                    let df = DataFrameImpl::new(
                        ctx.state,
                        &plan,
                    );
                    let batches = df.collect().await?;
                    let response =  batch_to_dataframe(&batches)?;

                    return Ok(QueryResponse::ResultSet(status, Arc::new(response)))
                },
                crate::compile::QueryPlan::CubeSelect(status, plan) => {
                    debug!("Request {}", json!(plan.request).to_string());
                    debug!("Meta {:?}", plan.meta);

                    let response = self.server.transport
                        .load(plan.request, &self.auth_context()?)
                        .await?;

                    let mut columns: Vec<dataframe::Column> = vec![];

                    for column_meta in &plan.meta {
                        columns.push(dataframe::Column::new(
                            column_meta.column_to.clone(),
                            column_meta.column_type,
                            ColumnFlags::empty(),
                        ));
                    }

                    let mut rows: Vec<dataframe::Row> = vec![];

                    if let Some(result) = response.results.first() {
                        debug!("Columns {:?}", columns);
                        debug!("Hydration mapping {:?}", plan.meta);
                        trace!("Response from Cube.js {:?}", result.data);

                        for row in result.data.iter() {
                            if let Some(record) = row.as_object() {
                                rows.push(
                                    dataframe::Row::hydrate_from_response(&plan.meta, record)
                                );
                            } else {
                                error!(
                                    "Unable to map row to DataFrame::Row: {:?}, skipping row",
                                    row
                                );
                            }
                        }

                        return Ok(QueryResponse::ResultSet(status, Arc::new(dataframe::DataFrame::new(
                            columns,
                            rows
                        ))));
                    } else {
                        return Ok(QueryResponse::ResultSet(status, Arc::new(dataframe::DataFrame::new(vec![], vec![]))));
                    }
                }
            }
        }

        if ignore {
            Ok(QueryResponse::ResultSet(
                StatusFlags::empty(),
                Arc::new(dataframe::DataFrame::new(vec![], vec![])),
            ))
        } else {
            Err(CubeError::internal("Unsupported query".to_string()))
        }
    }

    pub(crate) fn auth_context(&self) -> Result<AuthContext, CubeError> {
        if let Some(ctx) = self.state.auth_context() {
            Ok(ctx)
        } else {
            Err(CubeError::internal("must be auth".to_string()))
        }
    }
}

#[async_trait]
impl<W: io::Write + Send> AsyncMysqlShim<W> for Connection {
    type Error = io::Error;

    fn server_version(&self) -> &str {
        "8.0.25"
    }

    fn connection_id(&self) -> u32 {
        self.state.connection_id
    }

    async fn on_prepare<'a>(
        &'a mut self,
        query: &'a str,
        info: StatementMetaWriter<'a, W>,
    ) -> Result<(), Self::Error> {
        debug!("on_execute: {}", query);

        let mut state = self.statements.write().await;
        if state.statements.len() > self.server.configuration.connection_max_prepared_statements {
            let message = format!(
                "Unable to allocate new prepared statement, max allocation reached, actual: {}, max: {}",
                state.statements.len(),
                self.server.configuration.connection_max_prepared_statements
            );
            info.error(ErrorKind::ER_INTERNAL_ERROR, message.as_bytes())
        } else {
            state.id = state.id + 1;

            let next_id = state.id;
            state.statements.insert(next_id, query.to_string());

            info.reply(state.id, &[], &[])
        }
    }

    async fn on_execute<'a>(
        &'a mut self,
        id: u32,
        params_parser: ParamParser<'a>,
        results: QueryResultWriter<'a, W>,
    ) -> Result<(), Self::Error> {
        debug!("on_execute: {}", id);

        let mut state = self.statements.write().await;
        let possible_statement = state.statements.remove(&id);

        std::mem::drop(state);

        let statement = if possible_statement.is_none() {
            return results.error(ErrorKind::ER_INTERNAL_ERROR, b"Unknown statement");
        } else {
            possible_statement.unwrap()
        };

        let params_iter: Params = params_parser.into_iter();

        for _ in params_iter {
            // @todo Support params injection to query with escaping.
            return results.error(
                ErrorKind::ER_UNSUPPORTED_PS,
                b"Execution of prepared statement with parameters is not supported",
            );
        }

        self.handle_query(statement.as_str(), results).await
    }

    async fn on_close<'a>(&'a mut self, _stmt: u32)
    where
        W: 'async_trait,
    {
        trace!("on_close");
    }

    async fn on_query<'a>(
        &'a mut self,
        query: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> Result<(), Self::Error> {
        debug!("on_query: {}", query);

        self.handle_query(query, results).await
    }

    async fn on_auth<'a>(&'a mut self, user: Vec<u8>) -> Result<Option<Vec<u8>>, Self::Error>
    where
        W: 'async_trait,
    {
        let user = if !user.is_empty() {
            Some(String::from_utf8_lossy(user.as_slice()).to_string())
        } else {
            None
        };

        let auth_response = self
            .server
            .auth
            .authenticate(user.clone())
            .await
            .map_err(|e| {
                if e.message != *"Incorrect user name or password" {
                    error!("Error during authentication MySQL connection: {}", e);
                };

                io::Error::new(io::ErrorKind::Other, e.to_string())
            })?;

        let passwd = auth_response.password.map(|p| p.as_bytes().to_vec());

        self.state.set_user(user.clone());
        self.state.set_auth_context(Some(auth_response.context));

        Ok(passwd)
    }

    /// Generate salt for native auth plugin
    async fn generate_nonce<'a>(&'a mut self) -> Result<Vec<u8>, Self::Error>
    where
        W: 'async_trait,
    {
        Ok(self
            .server
            .nonce
            .clone()
            .unwrap_or_else(|| (0..20).map(|_| rand::random::<u8>()).collect()))
    }

    /// Called when client switches database: USE `db`;
    async fn on_init<'a>(
        &'a mut self,
        database: &'a str,
        writter: InitWriter<'a, W>,
    ) -> Result<(), Self::Error> {
        debug!("on_init: {}", database);

        self.state.set_database(Some(database.to_string()));

        writter.ok()?;

        Ok(())
    }
}

pub struct MySqlServer {
    address: String,
    server: Arc<ServerManager>,
    close_socket_rx: RwLock<watch::Receiver<bool>>,
    close_socket_tx: watch::Sender<bool>,
}

crate::di_service!(MySqlServer, []);

#[async_trait]
impl ProcessingLoop for MySqlServer {
    async fn processing_loop(&self) -> Result<(), CubeError> {
        let listener = TcpListener::bind(self.address.clone()).await?;

        println!("🔗 Cube SQL is listening on {}", self.address);

        let mut connection_id_incr = 0;

        loop {
            let mut stop_receiver = self.close_socket_rx.write().await;
            let (socket, _) = tokio::select! {
                res = stop_receiver.changed() => {
                    if res.is_err() || *stop_receiver.borrow() {
                        return Ok(());
                    } else {
                        continue;
                    }
                }
                accept_res = listener.accept() => {
                    match accept_res {
                        Ok(res) => res,
                        Err(err) => {
                            error!("Network error: {}", err);
                            continue;
                        }
                    }
                }
            };

            let server = self.server.clone();

            let connection_id = if connection_id_incr > 100_000_u32 {
                connection_id_incr = 1;

                connection_id_incr
            } else {
                connection_id_incr += 1;

                connection_id_incr
            };

            tokio::spawn(async move {
                if let Err(e) = AsyncMysqlIntermediary::run_on(
                    Connection {
                        server,
                        state: Arc::new(ConnectionState::new(
                            connection_id,
                            ConnectionProperties::new(None, None),
                            None,
                        )),
                        statements: Arc::new(RwLock::new(PreparedStatements::new())),
                    },
                    socket,
                )
                .await
                {
                    error!("Error during processing MySQL connection: {}", e);
                }
            });
        }
    }

    async fn stop_processing(&self) -> Result<(), CubeError> {
        self.close_socket_tx.send(true)?;
        Ok(())
    }
}

impl MySqlServer {
    pub fn new(
        address: String,
        auth: Arc<dyn SqlAuthService>,
        transport: Arc<dyn TransportService>,
        nonce: Option<Vec<u8>>,
    ) -> Arc<Self> {
        let (close_socket_tx, close_socket_rx) = watch::channel(false);
        Arc::new(Self {
            address,
            server: Arc::new(ServerManager::new(auth, transport, nonce)),
            close_socket_rx: RwLock::new(close_socket_rx),
            close_socket_tx,
        })
    }
}
