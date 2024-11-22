use cubesql::config::injection::Injector;
use std::sync::Arc;

#[derive(Clone)]
pub struct ApiGatewayState {
    injector: Arc<Injector>,
}

impl ApiGatewayState {
    pub fn new(injector: Arc<Injector>) -> Self {
        Self { injector }
    }

    pub fn injector_ref(&self) -> &Arc<Injector> {
        &self.injector
    }
}
