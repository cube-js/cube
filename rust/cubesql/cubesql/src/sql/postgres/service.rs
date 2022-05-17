use std::sync::Arc;

use async_trait::async_trait;
use log::{error, trace};
use tokio::{
    net::TcpListener,
    sync::{watch, RwLock},
};

use crate::{
    config::processing_loop::ProcessingLoop,
    sql::{session::DatabaseProtocol, SessionManager},
    telemetry::{ContextLogger, SessionLogger},
    CubeError,
};

use super::shim::AsyncPostgresShim;

pub struct PostgresServer {
    // options
    address: String,
    close_socket_rx: RwLock<watch::Receiver<bool>>,
    close_socket_tx: watch::Sender<bool>,
    // reference
    session_manager: Arc<SessionManager>,
}

crate::di_service!(PostgresServer, []);

#[async_trait]
impl ProcessingLoop for PostgresServer {
    async fn processing_loop(&self) -> Result<(), CubeError> {
        let listener = TcpListener::bind(self.address.clone()).await?;

        println!("🔗 Cube SQL (pg) is listening on {}", self.address);

        loop {
            let mut stop_receiver = self.close_socket_rx.write().await;
            let (socket, _) = tokio::select! {
                res = stop_receiver.changed() => {
                    if res.is_err() || *stop_receiver.borrow() {
                        trace!("[pg] Stopping processing_loop via channel");

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
                DatabaseProtocol::PostgreSQL,
                socket.peer_addr().unwrap().to_string(),
            );

            trace!("[pg] New connection {}", session.state.connection_id);

            let logger = Arc::new(SessionLogger::new(session.state.clone()));

            tokio::spawn(async move {
                if let Err(e) = AsyncPostgresShim::run_on(socket, session, logger.clone()).await {
                    logger.error(
                        format!("Error during processing PostgreSQL connection: {}", e).as_str(),
                        None,
                    );
                }
            });
        }
    }

    async fn stop_processing(&self) -> Result<(), CubeError> {
        self.close_socket_tx.send(true)?;
        Ok(())
    }
}

impl PostgresServer {
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
