use std::sync::Arc;

use async_trait::async_trait;
use log::error;
use tokio::{
    net::TcpListener,
    sync::{watch, RwLock},
};

use crate::{
    config::processing_loop::ProcessingLoop,
    sql::{DatabaseProtocol, SessionManager},
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

            // let user = Some("root".to_string());
            // let auth_context = self.server.auth.authenticate(user.clone()).await?.context;

            // let state = Arc::new(SessionState::new(
            //     connection_id,
            //     // FIXME: user, database!
            //     "127.0.0.1".to_string(),
            //     DatabaseProtocol::PostgreSQL,
            //     // FIXME: user, database!
            //     SessionProperties::new(user, Some("db".to_string())),
            //     // FIXME: auth_context!
            //     Some(auth_context),
            // ));

            let session = self.session_manager.create_session(
                DatabaseProtocol::PostgreSQL,
                socket.peer_addr().unwrap().to_string(),
            );

            tokio::spawn(async move {
                if let Err(e) = AsyncPostgresShim::run_on(socket, session).await {
                    error!("Error during processing PostgreSQL connection: {}", e);
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
