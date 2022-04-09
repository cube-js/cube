use std::sync::Arc;

use crate::{auth::NodeBridgeAuthService, transport::NodeBridgeTransport};
use cubesql::{
    config::{Config, CubeServices},
    sql::SqlAuthService,
    transport::TransportService,
};

#[derive(Clone)]
pub struct NodeConfig {
    pub config: Config,
}

impl NodeConfig {
    pub fn new(port: Option<u16>, nonce: Option<String>) -> NodeConfig {
        let config = Config::default();
        let config = config.update_config(|mut c| {
            if let Some(p) = port {
                c.bind_address = Some(format!("0.0.0.0:{}", p));
            };

            if let Some(n) = nonce {
                c.nonce = Some(n.as_bytes().to_vec());
            }

            c
        });

        Self { config }
    }

    pub async fn configure(
        &self,
        transport: Arc<NodeBridgeTransport>,
        auth: Arc<NodeBridgeAuthService>,
    ) -> CubeServices {
        let injector = self.config.injector();
        self.config.configure_injector().await;

        injector
            .register_typed::<dyn TransportService, _, _, _>(async move |_| transport)
            .await;

        injector
            .register_typed::<dyn SqlAuthService, _, _, _>(async move |_| auth)
            .await;

        self.config.cube_services().await
    }
}
