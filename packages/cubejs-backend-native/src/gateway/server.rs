use crate::gateway::{ApiGatewayRouterBuilder, ApiGatewayState};
use async_trait::async_trait;
use cubesql::config::injection::Injector;
use cubesql::config::processing_loop::ProcessingLoop;
use cubesql::CubeError;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{watch, Mutex, RwLock};

pub trait ApiGatewayServer: ProcessingLoop {}

pub struct ApiGatewayServerImpl {
    router: Mutex<Option<axum::Router<()>>>,
    // options
    address: String,
    close_socket_rx: RwLock<watch::Receiver<bool>>,
    close_socket_tx: watch::Sender<bool>,
}

cubesql::di_service!(ApiGatewayServerImpl, [ApiGatewayServer]);

impl ApiGatewayServerImpl {
    pub fn new(
        router_builder: ApiGatewayRouterBuilder,
        address: String,
        injector: Arc<Injector>,
    ) -> Arc<Self> {
        let (close_socket_tx, close_socket_rx) = watch::channel(false);

        let router = router_builder
            .build()
            .with_state(ApiGatewayState::new(injector));

        Arc::new(Self {
            router: Mutex::new(Some(router)),
            address,
            close_socket_rx: RwLock::new(close_socket_rx),
            close_socket_tx,
        })
    }
}

impl ApiGatewayServer for ApiGatewayServerImpl {}

#[async_trait]
impl ProcessingLoop for ApiGatewayServerImpl {
    async fn processing_loop(&self) -> Result<(), CubeError> {
        println!(
            "🔗 Cube (native api gateway) is listening on {}",
            self.address
        );

        let listener = TcpListener::bind(&self.address).await?;

        let router = {
            let mut guard = self.router.lock().await;
            if let Some(r) = guard.take() {
                r
            } else {
                return Err(CubeError::internal(
                    "ApiGatewayServer cannot be started twice".to_string(),
                ));
            }
        };

        let mut close_socket_rx_to_move = self.close_socket_tx.subscribe();

        let shutdown_signal = || async move {
            let _ = close_socket_rx_to_move.changed().await;

            log::trace!("Shutdown signal received");
        };

        axum::serve(listener, router)
            .with_graceful_shutdown(shutdown_signal())
            .await
            .map_err(|err| CubeError::internal(err.to_string()))
    }

    async fn stop_processing(&self) -> Result<(), CubeError> {
        self.close_socket_tx.send(true)?;
        Ok(())
    }
}
