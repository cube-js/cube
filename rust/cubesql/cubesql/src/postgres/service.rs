use std::sync::Arc;

use async_trait::async_trait;
use log::error;
use tokio::{
    net::TcpListener,
    sync::{watch, RwLock},
};

use crate::{
    config::processing_loop::ProcessingLoop,
    sql_shared::{ConnectionProperties, ConnectionState, SqlAuthService},
    transport::TransportService,
    CubeError,
};

use super::{
    server_manager::ServerManager,
    shim::AsyncPostgresShim,
};

pub struct PostgresServer {
    address: String,
    close_socket_rx: RwLock<watch::Receiver<bool>>,
    close_socket_tx: watch::Sender<bool>,
    //
    server: Arc<ServerManager>,
}

crate::di_service!(PostgresServer, []);

#[async_trait]
impl ProcessingLoop for PostgresServer {
    async fn processing_loop(&self) -> Result<(), CubeError> {
        let listener = TcpListener::bind(self.address.clone()).await?;

        println!("🔗 Cube SQL (pg) is listening on {}", self.address);

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

            let connection_id = connection_id_incr;
            connection_id_incr += 1;
            if connection_id_incr > 100_000_u32 {
                connection_id_incr = 1;
            }

            let user = Some("root".to_string());
            let auth_context = self.server.auth.authenticate(user.clone()).await?.context;

            let state = Arc::new(ConnectionState::new(
                connection_id,
                // FIXME: user, database!
                ConnectionProperties::new(
                    user,
                    Some("db".to_string()),
                ),
                // FIXME: auth_context!
                Some(auth_context),
            ));

            tokio::spawn(async move {
                if let Err(e) = AsyncPostgresShim::run_on(
                    socket,
                    server,
                    state,
                ).await {
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
    pub fn new(
        address: String,
        auth: Arc<dyn SqlAuthService>,
        transport: Arc<dyn TransportService>,
    ) -> Arc<Self> {
        let (close_socket_tx, close_socket_rx) = watch::channel(false);
        Arc::new(Self {
            address,
            server: Arc::new(ServerManager::new(auth, transport)),
            close_socket_rx: RwLock::new(close_socket_rx),
            close_socket_tx,
        })
    }
}
