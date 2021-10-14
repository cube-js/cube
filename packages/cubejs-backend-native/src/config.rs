use std::sync::Arc;

use crate::{auth::NodeBridgeAuthService, transport::NodeBridgeTransport};
use cubesql::{
    config::{Config, ConfigObj, CubeServices},
    mysql::SqlAuthService,
    schema::SchemaService,
};

#[derive(Clone)]
pub struct NodeConfig {
    pub config: Config,
}

impl NodeConfig {
    pub fn config(&self) -> Arc<dyn ConfigObj> {
        self.config.config_obj()
    }

    pub fn new() -> NodeConfig {
        let config = Config::default();

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
            .register_typed::<dyn SchemaService, _, _, _>(async move |_| transport)
            .await;

        injector
            .register_typed::<dyn SqlAuthService, _, _, _>(async move |_| auth)
            .await;

        self.config.cube_services().await
    }
}
