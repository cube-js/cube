use crate::compile::{convert_sql_to_cube_query, DatabaseProtocol, QueryPlan};
use crate::config::processing_loop::{ProcessingLoop, ShutdownMode};
use crate::sql::session::Session;
use crate::sql::session_manager::SessionManager;
use crate::sql::SqlAuthService;
use crate::CubeError;
use async_trait::async_trait;
use datafusion::dataframe::DataFrame as DataFusionDataFrame;
use log::{debug, error, info, trace, warn};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{watch, RwLock};

use super::cache::QueryResultCache;
use super::protocol::{read_message, write_message, Message, PROTOCOL_VERSION};
use super::stream_writer::StreamWriter;

pub struct ArrowNativeServer {
    address: String,
    session_manager: Arc<SessionManager>,
    auth_service: Arc<dyn SqlAuthService>,
    query_cache: Arc<QueryResultCache>,
    close_socket_rx: RwLock<watch::Receiver<Option<ShutdownMode>>>,
    close_socket_tx: watch::Sender<Option<ShutdownMode>>,
}

crate::di_service!(ArrowNativeServer, []);

#[async_trait]
impl ProcessingLoop for ArrowNativeServer {
    async fn processing_loop(&self) -> Result<(), CubeError> {
        let listener = TcpListener::bind(&self.address).await.map_err(|e| {
            CubeError::internal(format!("Failed to bind to {}: {}", self.address, e))
        })?;

        println!("🔗 Cube SQL (arrow) is listening on {}", self.address);

        let mut joinset = tokio::task::JoinSet::new();
        let mut active_shutdown_mode: Option<ShutdownMode> = None;

        loop {
            let mut stop_receiver = self.close_socket_rx.write().await;
            let (socket, addr) = tokio::select! {
                _ = stop_receiver.changed() => {
                    let mode = *stop_receiver.borrow();
                    if mode > active_shutdown_mode {
                        active_shutdown_mode = mode;
                        match active_shutdown_mode {
                            Some(ShutdownMode::Fast) | Some(ShutdownMode::SemiFast) | Some(ShutdownMode::Smart) => {
                                trace!("[arrow] Stopping processing_loop via channel, mode: {:?}", mode);
                                break;
                            }
                            None => {
                                unreachable!("mode compared greater than something; it can't be None");
                            }
                        }
                    } else {
                        continue;
                    }
                }
                Some(_) = joinset.join_next() => {
                    continue;
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

            let connection_id = {
                let peer_addr = socket.peer_addr().ok();
                let (client_addr, client_port) = peer_addr
                    .map(|addr| (addr.ip().to_string(), addr.port()))
                    .unwrap_or_else(|| ("127.0.0.1".to_string(), 0u16));

                trace!("[arrow] New connection from {}", addr);

                let session_manager = self.session_manager.clone();
                let auth_service = self.auth_service.clone();
                let query_cache = self.query_cache.clone();

                let session = match session_manager
                    .create_session(
                        DatabaseProtocol::ArrowNative,
                        client_addr,
                        client_port,
                        None,
                    )
                    .await
                {
                    Ok(session) => session,
                    Err(err) => {
                        error!("Session creation error: {}", err);
                        continue;
                    }
                };

                let connection_id = session.state.connection_id;

                joinset.spawn(async move {
                    if let Err(e) = Self::handle_connection(
                        socket,
                        session_manager.clone(),
                        auth_service,
                        query_cache,
                        session,
                    )
                    .await
                    {
                        error!("Connection error from {}: {}", addr, e);
                    }

                    trace!("[arrow] Removing connection {}", connection_id);
                    session_manager.drop_session(connection_id).await;
                });

                connection_id
            };

            trace!("[arrow] Spawned handler for connection {}", connection_id);
        }

        // Close the listening socket
        drop(listener);

        // Wait for outstanding connections to finish
        loop {
            let mut stop_receiver = self.close_socket_rx.write().await;
            tokio::select! {
                _ = stop_receiver.changed() => {
                    let mode = *stop_receiver.borrow();
                    if mode > active_shutdown_mode {
                        active_shutdown_mode = mode;
                    }
                    continue;
                }
                res = joinset.join_next() => {
                    if res.is_none() {
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    async fn stop_processing(&self, mode: ShutdownMode) -> Result<(), CubeError> {
        self.close_socket_tx.send(Some(mode))?;
        Ok(())
    }
}

impl ArrowNativeServer {
    pub fn new(
        address: String,
        session_manager: Arc<SessionManager>,
        auth_service: Arc<dyn SqlAuthService>,
    ) -> Arc<Self> {
        let (close_socket_tx, close_socket_rx) = watch::channel(None::<ShutdownMode>);
        let query_cache = Arc::new(QueryResultCache::from_env());

        Arc::new(Self {
            address,
            session_manager,
            auth_service,
            query_cache,
            close_socket_rx: RwLock::new(close_socket_rx),
            close_socket_tx,
        })
    }

    async fn handle_connection(
        mut socket: TcpStream,
        _session_manager: Arc<SessionManager>,
        auth_service: Arc<dyn SqlAuthService>,
        query_cache: Arc<QueryResultCache>,
        session: Arc<Session>,
    ) -> Result<(), CubeError> {
        // Handshake phase
        let msg = read_message(&mut socket).await?;
        match msg {
            Message::HandshakeRequest { version } => {
                if version != PROTOCOL_VERSION {
                    warn!(
                        "Client requested version {}, server supports version {}",
                        version, PROTOCOL_VERSION
                    );
                }

                let response = Message::HandshakeResponse {
                    version: PROTOCOL_VERSION,
                    server_version: env!("CARGO_PKG_VERSION").to_string(),
                };
                write_message(&mut socket, &response).await?;
            }
            _ => {
                return Err(CubeError::internal(
                    "Expected handshake request".to_string(),
                ))
            }
        }

        // Authentication phase
        let msg = read_message(&mut socket).await?;
        let database = match msg {
            Message::AuthRequest { token, database } => {
                // Authenticate using token as password
                let auth_request = crate::sql::auth_service::SqlAuthServiceAuthenticateRequest {
                    protocol: "arrow_native".to_string(),
                    method: "token".to_string(),
                };

                let auth_result = auth_service
                    .authenticate(auth_request, None, Some(token.clone()))
                    .await
                    .map_err(|e| CubeError::internal(format!("Authentication failed: {}", e)))?;

                // Check authentication - for token auth, we skip password check
                if !auth_result.skip_password_check && auth_result.password != Some(token.clone()) {
                    let response = Message::AuthResponse {
                        success: false,
                        session_id: String::new(),
                    };
                    write_message(&mut socket, &response).await?;
                    return Err(CubeError::internal("Authentication failed".to_string()));
                }

                // Set auth context after session creation
                session.state.set_auth_context(Some(auth_result.context));

                let session_id = format!("{}", session.state.connection_id);

                let response = Message::AuthResponse {
                    success: true,
                    session_id: session_id.clone(),
                };
                write_message(&mut socket, &response).await?;

                database
            }
            _ => {
                return Err(CubeError::internal("Expected auth request".to_string()));
            }
        };

        info!("Session created: {}", session.state.connection_id);

        // Query execution loop
        loop {
            match read_message(&mut socket).await {
                Ok(msg) => match msg {
                    Message::QueryRequest { sql } => {
                        debug!("Executing query: {}", sql);

                        if let Err(e) = Self::execute_query(
                            &mut socket,
                            query_cache.clone(),
                            session.clone(),
                            &sql,
                            database.as_deref(),
                        )
                        .await
                        {
                            error!("Query execution error: {}", e);

                            // Attempt to send error message to client
                            if let Err(write_err) = StreamWriter::write_error(
                                &mut socket,
                                "QUERY_ERROR".to_string(),
                                e.to_string(),
                            )
                            .await
                            {
                                error!(
                                    "Failed to send error message to client: {}. Original error: {}",
                                    write_err, e
                                );
                                // Connection is broken, exit handler loop
                                break;
                            }

                            // Error successfully sent, continue serving this connection
                            debug!("Error message sent to client successfully");
                        }
                    }
                    _ => {
                        warn!("Unexpected message type during query phase");
                        break;
                    }
                },
                Err(e) => {
                    // Connection closed or error
                    debug!("Connection closed: {}", e);
                    break;
                }
            }
        }

        Ok(())
    }

    async fn execute_query(
        socket: &mut TcpStream,
        query_cache: Arc<QueryResultCache>,
        session: Arc<Session>,
        sql: &str,
        database: Option<&str>,
    ) -> Result<(), CubeError> {
        // Try to get cached result first
        if let Some(cached_batches) = query_cache.get(sql, database).await {
            debug!(
                "Cache HIT - streaming {} cached batches",
                cached_batches.len()
            );
            StreamWriter::stream_cached_batches(socket, &cached_batches, true).await?;
            return Ok(());
        }

        debug!("Cache MISS - executing query");

        // Get auth context - for now we'll use what's in the session
        let auth_context = session
            .state
            .auth_context()
            .ok_or_else(|| CubeError::internal("No auth context available".to_string()))?;

        // Get compiler cache entry
        let cache_entry = session
            .session_manager
            .server
            .compiler_cache
            .get_cache_entry(auth_context, session.state.protocol.clone())
            .await?;

        let meta = session
            .session_manager
            .server
            .compiler_cache
            .meta(cache_entry)
            .await?;

        // Convert SQL to query plan
        let query_plan = convert_sql_to_cube_query(sql, meta, session.clone()).await?;

        // Execute based on query plan type
        match query_plan {
            QueryPlan::DataFusionSelect(plan, ctx) => {
                // Create DataFusion DataFrame from logical plan
                let df = DataFusionDataFrame::new(ctx.state.clone(), &plan);

                // Collect results for caching
                let batches = df.collect().await.map_err(|e| {
                    CubeError::internal(format!("Failed to collect batches: {}", e))
                })?;

                // Cache the results
                query_cache.insert(sql, database, batches.clone()).await;

                // Stream results (from fresh execution)
                StreamWriter::stream_cached_batches(socket, &batches, false).await?;
            }
            QueryPlan::MetaOk(_, _) => {
                // Meta commands (e.g., SET, BEGIN, COMMIT)
                // Send completion with 0 rows
                StreamWriter::write_complete(socket, 0).await?;
            }
            QueryPlan::MetaTabular(_, _data) => {
                // Meta tabular results (e.g., SHOW statements)
                // For now, just send completion
                // TODO: Convert internal DataFrame to Arrow RecordBatch and stream
                StreamWriter::write_complete(socket, 0).await?;
            }
            QueryPlan::CreateTempTable(plan, ctx, _name, _temp_tables) => {
                // Create temp table
                let df = DataFusionDataFrame::new(ctx.state.clone(), &plan);

                // Collect results (temp tables need to be materialized)
                let batches = df.collect().await.map_err(|e| {
                    CubeError::internal(format!("Failed to collect batches: {}", e))
                })?;

                let row_count: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();

                // Note: temp_tables.save() would be called here for full implementation
                // For now, just acknowledge the creation
                StreamWriter::write_complete(socket, row_count).await?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // use super::*;

    #[test]
    fn test_server_creation() {
        // This is a placeholder test - actual server tests would require
        // mock session manager and auth service
    }
}
