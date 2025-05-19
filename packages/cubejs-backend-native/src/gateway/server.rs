use crate::gateway::state::ApiGatewayStateRef;
use crate::gateway::ApiGatewayRouterBuilder;
use async_trait::async_trait;
use cubesql::config::processing_loop::{ProcessingLoop, ShutdownMode};
use cubesql::CubeError;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{watch, Mutex};

pub trait ApiGatewayServer: ProcessingLoop {}

struct InnerFactoryState {
    router: axum::Router<()>,
    // options
    address: String,
    close_socket_rx: watch::Receiver<bool>,
}

impl InnerFactoryState {
    fn split(self) -> (axum::Router<()>, String, watch::Receiver<bool>) {
        (self.router, self.address, self.close_socket_rx)
    }
}

pub struct ApiGatewayServerImpl {
    // processing_loop uses &self. split via Option::take
    inner_factory_state: Mutex<Option<InnerFactoryState>>,
    close_socket_tx: watch::Sender<bool>,
}

cubesql::di_service!(ApiGatewayServerImpl, [ApiGatewayServer]);

impl ApiGatewayServerImpl {
    pub fn new(
        router_builder: ApiGatewayRouterBuilder,
        address: String,
        state: ApiGatewayStateRef,
    ) -> Arc<Self> {
        let (close_socket_tx, close_socket_rx) = watch::channel(false);

        let router = router_builder.build().with_state(state);

        Arc::new(Self {
            inner_factory_state: Mutex::new(Some(InnerFactoryState {
                router,
                address,
                close_socket_rx,
            })),
            close_socket_tx,
        })
    }
}

impl ApiGatewayServer for ApiGatewayServerImpl {}

#[async_trait]
impl ProcessingLoop for ApiGatewayServerImpl {
    async fn processing_loop(&self) -> Result<(), CubeError> {
        let (router, address, mut close_socket_rx) = {
            let mut guard = self.inner_factory_state.lock().await;
            if let Some(r) = guard.take() {
                r.split()
            } else {
                return Err(CubeError::internal(
                    "ApiGatewayServer cannot be started twice".to_string(),
                ));
            }
        };

        println!("🔗 Cube (native api gateway) is listening on {}", address);

        let listener = TcpListener::bind(&address).await?;

        let shutdown_signal = || async move {
            let _ = close_socket_rx.changed().await;

            log::trace!("Shutdown signal received");
        };

        axum::serve(listener, router)
            .with_graceful_shutdown(shutdown_signal())
            .await
            .map_err(|err| CubeError::internal(err.to_string()))
    }

    async fn stop_processing(&self, _mode: ShutdownMode) -> Result<(), CubeError> {
        // ShutdownMode was added for Postgres protocol and its use here has not yet been considered.
        self.close_socket_tx.send(true).map_err(|err| {
            CubeError::internal(format!(
                "Failed to send close signal to ApiGatewayServer: {}",
                err
            ))
        })?;

        Ok(())
    }
}
