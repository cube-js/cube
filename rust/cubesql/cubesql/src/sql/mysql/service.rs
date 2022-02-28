use std::collections::HashMap;
use std::io;

use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;

use datafusion::execution::dataframe_impl::DataFrameImpl;
use datafusion::prelude::DataFrame as DFDataFrame;

use log::debug;
use log::error;
use log::trace;

//use msql_srv::*;
use msql_srv::{
    AsyncMysqlIntermediary, AsyncMysqlShim, Column, ErrorKind, InitWriter, ParamParser, Params,
    QueryResultWriter, StatementMetaWriter,
};

use tokio::net::TcpListener;
use tokio::sync::{watch, RwLock};

use crate::compile::convert_sql_to_cube_query;
use crate::config::processing_loop::ProcessingLoop;

use crate::sql::DatabaseProtocol;
use crate::sql::Session;
use crate::sql::SessionManager;
use crate::sql::{
    dataframe::{self, batch_to_dataframe},
    AuthContext, ColumnFlags, ColumnType, QueryResponse, StatusFlags,
};
use crate::CubeError;

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
struct MySqlConnection {
    // Prepared statements
    statements: Arc<RwLock<PreparedStatements>>,
    // Shared
    session: Arc<Session>,
}

impl Drop for MySqlConnection {
    fn drop(&mut self) {
        trace!(
            "[MySqlConnection] Drop {}",
            self.session.state.connection_id
        );

        self.session
            .session_manager
            .drop_session(self.session.state.connection_id)
    }
}

impl MySqlConnection {
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
                results.completed(0, 0, status.to_mysql_flags())?;
                Ok(())
            }
            Ok(QueryResponse::ResultSet(_, data_frame)) => {
                let columns = data_frame
                    .get_columns()
                    .iter()
                    .map(|c| Column {
                        table: "result".to_string(), // TODO
                        column: c.get_name(),
                        coltype: c.get_type().to_mysql(),
                        colflags: c.get_flags().to_mysql(),
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
            _ => false,
        };

        if query_lower.eq("select cast('test plain returns' as char(60)) as anon_1") {
            return Ok(
                QueryResponse::ResultSet(StatusFlags::empty(), Arc::new(
                    dataframe::DataFrame::new(
                        vec![dataframe::Column::new(
                            "anon_1".to_string(),
                            ColumnType::String,
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
                            ColumnType::String,
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
                            ColumnType::String,
                            ColumnFlags::empty(),
                        )],
                        vec![dataframe::Row::new(vec![
                            dataframe::TableValue::String("test collated returns".to_string())
                        ])]
                    )
                ),)
            )
        } else if !ignore {
            trace!("query was not detected");

            let meta = self.session.server.transport
                .meta(self.auth_context()?)
                .await?;

            let plan = convert_sql_to_cube_query(&query, Arc::new(meta), self.session.clone())?;
            match plan {
                crate::compile::QueryPlan::MetaOk(status) => {
                    return Ok(QueryResponse::Ok(status));
                },
                crate::compile::QueryPlan::MetaTabular(status, data_frame) => {
                    return Ok(QueryResponse::ResultSet(status, data_frame));
                },
                crate::compile::QueryPlan::DataFusionSelect(status, plan, ctx) => {
                    let df = DataFrameImpl::new(
                        ctx.state,
                        &plan,
                    );
                    let batches = df.collect().await?;
                    let response =  batch_to_dataframe(&batches)?;

                    return Ok(QueryResponse::ResultSet(status, Arc::new(response)))
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

    pub(crate) fn auth_context(&self) -> Result<Arc<AuthContext>, CubeError> {
        if let Some(ctx) = self.session.state.auth_context() {
            Ok(Arc::new(ctx))
        } else {
            Err(CubeError::internal("must be auth".to_string()))
        }
    }
}

#[async_trait]
impl<W: io::Write + Send> AsyncMysqlShim<W> for MySqlConnection {
    type Error = io::Error;

    fn server_version(&self) -> &str {
        "8.0.25"
    }

    fn connection_id(&self) -> u32 {
        self.session.state.connection_id
    }

    async fn on_prepare<'a>(
        &'a mut self,
        query: &'a str,
        info: StatementMetaWriter<'a, W>,
    ) -> Result<(), Self::Error> {
        debug!("on_execute: {}", query);

        let mut state = self.statements.write().await;
        if state.statements.len()
            > self
                .session
                .server
                .configuration
                .connection_max_prepared_statements
        {
            let message = format!(
                "Unable to allocate new prepared statement, max allocation reached, actual: {}, max: {}",
                state.statements.len(),
                self.session.server.configuration.connection_max_prepared_statements
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
            .session
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

        self.session.state.set_user(user.clone());
        self.session
            .state
            .set_auth_context(Some(auth_response.context));

        Ok(passwd)
    }

    /// Generate salt for native auth plugin
    async fn generate_nonce<'a>(&'a mut self) -> Result<Vec<u8>, Self::Error>
    where
        W: 'async_trait,
    {
        Ok(self
            .session
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
        debug!("on_init: USE {}", database);

        if self
            .execute_query(&format!("USE {}", database))
            .await
            .is_err()
        {
            writter.error(ErrorKind::ER_BAD_DB_ERROR, b"Unknown database")?;
            return Ok(());
        };

        writter.ok()?;

        Ok(())
    }
}

pub struct MySqlServer {
    address: String,
    session_manager: Arc<SessionManager>,
    close_socket_rx: RwLock<watch::Receiver<bool>>,
    close_socket_tx: watch::Sender<bool>,
}

crate::di_service!(MySqlServer, []);

#[async_trait]
impl ProcessingLoop for MySqlServer {
    async fn processing_loop(&self) -> Result<(), CubeError> {
        let listener = TcpListener::bind(self.address.clone()).await?;

        println!("🔗 Cube SQL is listening on {}", self.address);

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

            let session = self.session_manager.create_session(
                DatabaseProtocol::MySQL,
                socket.peer_addr().unwrap().to_string(),
            );

            tokio::spawn(async move {
                if let Err(e) = AsyncMysqlIntermediary::run_on(
                    MySqlConnection {
                        session,
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
    pub fn new(address: String, session_manager: Arc<SessionManager>) -> Arc<Self> {
        let (close_socket_tx, close_socket_rx) = watch::channel(false);
        Arc::new(Self {
            address,
            session_manager,
            close_socket_rx: RwLock::new(close_socket_rx),
            close_socket_tx,
        })
    }
}
