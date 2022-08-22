use std::sync::Arc;

use async_trait::async_trait;
use log::{error, trace};
use pg_srv::protocol;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{watch, RwLock},
};

use crate::{
    config::processing_loop::ProcessingLoop,
    sql::{
        connection::PostgresConnection,
        server::{
            AuthenticateResponse, ConnectionError, InitialParameters, PostgresServerIntermediary,
            PostgresServerTrait, StartupState,
        },
        session::DatabaseProtocol,
        SessionManager,
    },
    telemetry::{ContextLogger, SessionLogger},
    CubeError,
};

pub struct PostgresServerService {
    // options
    address: String,
    close_socket_rx: RwLock<watch::Receiver<bool>>,
    close_socket_tx: watch::Sender<bool>,
    // reference
    session_manager: Arc<SessionManager>,
}

crate::di_service!(PostgresServerService, []);

struct PostgresServer {
    session_manager: Arc<SessionManager>,
}

#[derive(Debug)]
pub struct AuthPayload {}

#[async_trait]
impl PostgresServerTrait for PostgresServer {
    type AuthResponsePayload = AuthPayload;
    type ConnectionType = PostgresConnection;

    async fn create_connection(
        &self,
        socket: TcpStream,
        auth_payload: AuthPayload,
        _parameters: InitialParameters,
    ) -> PostgresConnection {
        let session = self.session_manager.create_session(
            // TODO: Support Redshift protocol
            DatabaseProtocol::PostgreSQL,
            socket.peer_addr().unwrap().to_string(),
        );

        // session.state.set_user()

        PostgresConnection { session, socket }
    }

    async fn authenticate(
        &self,
        messsage: protocol::PasswordMessage,
    ) -> Result<AuthenticateResponse<AuthPayload>, ConnectionError> {
        todo!()
    }

    async fn process_cancel(
        &self,
        cancel_message: protocol::CancelRequest,
    ) -> Result<StartupState, ConnectionError> {
        trace!("Cancel request {:?}", cancel_message);

        if let Some(s) = self.session_manager.get_session(cancel_message.process_id) {
            if s.state.secret == cancel_message.secret {
                s.state.cancel_query();
            } else {
                trace!(
                    "Unable to process cancel: wrong secret, {} != {}",
                    s.state.secret,
                    cancel_message.secret
                );
            }
        } else {
            trace!("Unable to process cancel: unknown session");
        }

        Ok(StartupState::CancelRequest)
    }
}

#[async_trait]
impl ProcessingLoop for PostgresServerService {
    async fn processing_loop(&self) -> Result<(), CubeError> {
        let listener = TcpListener::bind(self.address.clone()).await?;

        println!("🔗 Cube SQL (pg) is listening on {}", self.address);
        let server = Arc::new(PostgresServer {
            session_manager: self.session_manager.clone(),
        });

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

            trace!("[pg] New Handshake");
            let server_ref = server.clone();

            tokio::spawn(async move {
                // if let Err(e) = PostgresServerIntermediary::run_on(socket,server_ref).await {
                //     error!("Error during processing PostgreSQL connection: {}", e);
                // }

                let connection = PostgresServerIntermediary::run_on(socket, server_ref).await?;

                tokio::spawn({});

                connection.run().await
            });
        }
    }

    async fn stop_processing(&self) -> Result<(), CubeError> {
        self.close_socket_tx.send(true)?;
        Ok(())
    }
}

impl PostgresServerService {
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
