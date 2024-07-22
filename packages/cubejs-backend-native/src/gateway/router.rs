use crate::gateway::handlers::stream_handler_v2;
use crate::gateway::ApiGatewayState;
use axum::routing::{get, MethodRouter};
use axum::Router;

pub struct ApiGatewayRouterBuilder {
    router: Router<ApiGatewayState>,
}

impl Default for ApiGatewayRouterBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ApiGatewayRouterBuilder {
    pub fn new() -> Self {
        let router = Router::new();
        let router = router.route("/v2/stream", get(stream_handler_v2));

        Self { router }
    }

    pub fn route(self, path: &str, method_router: MethodRouter<ApiGatewayState>) -> Self {
        Self {
            router: self.router.route(path, method_router),
        }
    }

    pub fn build(self) -> Router<ApiGatewayState> {
        self.router
    }
}
