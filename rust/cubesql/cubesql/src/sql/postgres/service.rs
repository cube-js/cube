use async_trait::async_trait;
use log::{error, trace};
use std::sync::Arc;
use tokio::{
    net::TcpListener,
    sync::{watch, RwLock},
};
use tokio_util::sync::CancellationToken;

use crate::{
    compile::DatabaseProtocol,
    config::processing_loop::{ProcessingLoop, ShutdownMode},
    sql::SessionManager,
    telemetry::{ContextLogger, SessionLogger},
    CubeError,
};
use crate::sql::Session;
use super::shim::AsyncPostgresShim;

pub struct PostgresServer {
    // options
    address: String,
    close_socket_rx: RwLock<watch::Receiver<Option<ShutdownMode>>>,
    close_socket_tx: watch::Sender<Option<ShutdownMode>>,
    // reference
    session_manager: Arc<SessionManager>,
}

crate::di_service!(PostgresServer, []);

#[async_trait]
impl ProcessingLoop for PostgresServer {
    async fn processing_loop(&self) -> Result<(), CubeError> {
        let listener = TcpListener::bind(self.address.clone()).await?;

        println!("🔗 Cube SQL (pg) is listening on {}", self.address);

        let fast_shutdown_interruptor = CancellationToken::new();
        let semifast_shutdown_interruptor = CancellationToken::new();

        let mut joinset = tokio::task::JoinSet::new();
        let mut active_shutdown_mode: Option<ShutdownMode> = None;

        loop {
            let mut stop_receiver = self.close_socket_rx.write().await;
            let (socket, _) = tokio::select! {
                _ = stop_receiver.changed() => {
                    let mode = *stop_receiver.borrow();
                    if mode > active_shutdown_mode {
                        active_shutdown_mode = mode;
                        match active_shutdown_mode {
                            Some(ShutdownMode::Fast) => {
                                trace!("[pg] Stopping processing_loop via channel, fast mode");

                                fast_shutdown_interruptor.cancel();
                                break;
                            }
                            Some(ShutdownMode::SemiFast) => {
                                trace!("[pg] Stopping processing_loop via channel, semifast mode");

                                semifast_shutdown_interruptor.cancel();
                                break;
                            }
                            Some(ShutdownMode::Smart) => {
                                trace!("[pg] Stopping processing_loop via interruptor, smart mode");
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
                    // We do nothing here; whatever is here needs to be in the join_next() cleanup
                    // after the loop.
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

            let (client_addr, client_port) = match socket.peer_addr() {
                Ok(peer_addr) => (peer_addr.ip().to_string(), peer_addr.port()),
                Err(e) => {
                    error!("[pg] Error while calling peer_addr() on TcpStream: {}", e);

                    ("127.0.0.1".to_string(), 0000_u16)
                }
            };

            let session = match self.session_manager.create_session(DatabaseProtocol::PostgreSQL, client_addr, client_port, None).await {
                Ok(r) => r,
                Err(err) => {
                    error!("Session creation error: {}", err);
                    continue;
                }
            };

            let logger = Arc::new(SessionLogger::new(session.state.clone()));

            trace!("[pg] New connection {}", session.state.connection_id);

            let connection_id = session.state.connection_id;
            let session_manager = self.session_manager.clone();

            let fast_shutdown_interruptor = fast_shutdown_interruptor.clone();
            let semifast_shutdown_interruptor = semifast_shutdown_interruptor.clone();
            let join_handle: tokio::task::JoinHandle<()> = tokio::spawn(async move {
                let handler = AsyncPostgresShim::run_on(
                    fast_shutdown_interruptor,
                    semifast_shutdown_interruptor,
                    socket,
                    session.clone(),
                    logger.clone(),
                );
                if let Err(e) = handler.await {
                    logger.error(
                        format!("Error during processing PostgreSQL connection: {}", e).as_str(),
                        None,
                    );

                    if let Some(bt) = e.backtrace() {
                        trace!("{}", bt);
                    } else {
                        trace!("Backtrace: not found");
                    }
                };
            });

            // We use a separate task because `handler` above, the result of
            // `AsyncPostgresShim::run_on,` can panic, which we want to catch.  (And which the
            // JoinHandle catches.)
            joinset.spawn(async move {
                let _ = join_handle.await;

                trace!("[pg] Removing connection {}", connection_id);

                session_manager.drop_session(connection_id).await;
            });
        }

        // Close the listening socket (so we _visibly_ stop accepting incoming connections) before
        // we wait for the outstanding connection tasks finish.
        drop(listener);

        // Now that we've had the stop signal, wait for outstanding connection tasks to finish
        // cleanly.

        loop {
            let mut stop_receiver = self.close_socket_rx.write().await;
            tokio::select! {
                _ = stop_receiver.changed() => {
                    let mode = *stop_receiver.borrow();
                    if mode > active_shutdown_mode {
                        active_shutdown_mode = mode;
                        match active_shutdown_mode {
                            Some(ShutdownMode::Fast) => {
                                trace!("[pg] Stopping processing_loop via channel: upgrading to fast mode");

                                fast_shutdown_interruptor.cancel();
                            }
                            Some(ShutdownMode::SemiFast) => {
                                trace!("[pg] Stopping processing_loop via channel: upgrading to semifast mode");

                                semifast_shutdown_interruptor.cancel();
                            }
                            _ => {
                                // Because of comparisons made, the smallest and 2nd smallest
                                // Option<ShutdownMode> values are impossible.
                                unreachable!("impossible mode value, where mode={:?}", active_shutdown_mode);
                            }
                        }
                    } else {
                        continue;
                    }
                }
                res = joinset.join_next() => {
                    if let None = res {
                        break;
                    } else {
                        // We do nothing here, same as the other join_next() cleanup in the prior loop.
                        continue;
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

impl PostgresServer {
    pub fn new(address: String, session_manager: Arc<SessionManager>) -> Arc<Self> {
        let (close_socket_tx, close_socket_rx) = watch::channel(None::<ShutdownMode>);
        Arc::new(Self {
            address,
            session_manager,
            close_socket_rx: RwLock::new(close_socket_rx),
            close_socket_tx,
        })
    }
}
