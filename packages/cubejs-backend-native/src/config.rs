use crate::gateway::server::ApiGatewayServerImpl;
use crate::gateway::{
    ApiGatewayRouterBuilder, ApiGatewayServer, ApiGatewayState, GatewayAuthService,
};
use crate::{auth::NodeBridgeAuthService, transport::NodeBridgeTransport};
use async_trait::async_trait;
use cubesql::config::injection::Injector;
use cubesql::config::processing_loop::ShutdownMode;
use cubesql::{
    config::{Config, CubeServices},
    sql::SqlAuthService,
    transport::TransportService,
    CubeError,
};
use std::sync::Arc;
use tokio::task::JoinHandle;

pub type LoopHandle = JoinHandle<Result<(), CubeError>>;

pub struct NodeCubeServices {
    pub services: CubeServices,
}

impl NodeCubeServices {
    pub fn new(services: CubeServices) -> Self {
        Self { services }
    }

    pub fn injector(&self) -> &Arc<Injector> {
        &self.services.injector
    }

    pub async fn spawn_processing_loops(&self) -> Result<Vec<LoopHandle>, CubeError> {
        let mut futures = self.services.spawn_processing_loops().await?;

        if self
            .services
            .injector
            .has_service_typed::<dyn ApiGatewayServer>()
            .await
        {
            let gateway_server = self
                .services
                .injector
                .get_service_typed::<dyn ApiGatewayServer>()
                .await;

            futures.push(tokio::spawn(async move {
                if let Err(e) = gateway_server.processing_loop().await {
                    log::error!("{}", e.to_string());
                };

                Ok(())
            }));
        }

        Ok(futures)
    }

    pub async fn stop_processing_loops(
        &self,
        shutdown_mode: ShutdownMode,
    ) -> Result<(), CubeError> {
        self.services.stop_processing_loops(shutdown_mode).await?;

        if self
            .services
            .injector
            .has_service_typed::<dyn ApiGatewayServer>()
            .await
        {
            let gateway_server = self
                .services
                .injector
                .get_service_typed::<dyn ApiGatewayServer>()
                .await;

            gateway_server.stop_processing(shutdown_mode).await?;
        }

        Ok(())
    }

    pub async fn await_processing_loops(&self) -> Result<(), CubeError> {
        let mut handles = Vec::new();

        {
            let mut w = self.services.processing_loop_handles.write().await;
            std::mem::swap(&mut *w, &mut handles);
        }

        for h in handles {
            let _ = h.await;
        }

        Ok(())
    }
}

pub trait NativeConfiguration {
    fn api_gateway_address(&self) -> &Option<String>;
}

#[derive(Clone)]
pub struct NodeConfigurationImpl {
    pub config: Config,
    pub api_gateway_address: Option<String>,
}

#[derive(Debug)]
pub struct NodeConfigurationFactoryOptions {
    pub gateway_port: Option<u16>,
    pub pg_port: Option<u16>,
}

#[async_trait]
pub trait NodeConfiguration {
    fn new(options: NodeConfigurationFactoryOptions) -> Self;

    async fn configure(
        &self,
        transport: Arc<NodeBridgeTransport>,
        auth: Arc<NodeBridgeAuthService>,
    ) -> Arc<NodeCubeServices>;
}

#[async_trait]
impl NodeConfiguration for NodeConfigurationImpl {
    fn new(options: NodeConfigurationFactoryOptions) -> Self {
        let config = Config::default();
        let config = config.update_config(|mut c| {
            if let Some(p) = options.pg_port {
                c.postgres_bind_address = Some(format!("0.0.0.0:{}", p));
            };

            c
        });

        Self {
            config,
            api_gateway_address: options
                .gateway_port
                .map(|gateway_port| format!("0.0.0.0:{}", gateway_port)),
        }
    }

    async fn configure(
        &self,
        transport: Arc<NodeBridgeTransport>,
        auth: Arc<NodeBridgeAuthService>,
    ) -> Arc<NodeCubeServices> {
        let injector = self.config.injector();

        self.config.configure().await;

        injector
            .register_typed::<dyn TransportService, _, _, _>(|_| async move { transport })
            .await;

        let auth_to_move = auth.clone();
        injector
            .register_typed::<dyn SqlAuthService, _, _, _>(|_| async move { auth_to_move })
            .await;

        let auth_to_move = auth.clone();
        injector
            .register_typed::<dyn GatewayAuthService, _, _, _>(|_| async move { auth_to_move })
            .await;

        if let Some(api_gateway_address) = &self.api_gateway_address {
            let api_gateway_address = api_gateway_address.clone();

            injector
                .register_typed::<dyn ApiGatewayServer, _, _, _>(|i| async move {
                    let state = Arc::new(ApiGatewayState::new(i));

                    ApiGatewayServerImpl::new(
                        ApiGatewayRouterBuilder::new(state.clone()),
                        api_gateway_address,
                        state,
                    )
                })
                .await;
        }

        Arc::new(NodeCubeServices::new(self.config.cube_services().await))
    }
}
