use crate::gateway::gateway_auth_middleware;
use crate::gateway::handlers::stream_handler_v2;
use crate::gateway::state::ApiGatewayStateRef;
use axum::routing::{get, MethodRouter};
use axum::Router;

pub type RApiGatewayRouter = Router<ApiGatewayStateRef>;

#[derive(Debug, Clone)]
pub struct ApiGatewayRouterBuilder {
    router: RApiGatewayRouter,
}

impl ApiGatewayRouterBuilder {
    pub fn new(state: ApiGatewayStateRef) -> Self {
        let router = Router::new();
        let router = router.route(
            "/v2/stream",
            get(stream_handler_v2).layer(axum::middleware::from_fn_with_state(
                state,
                gateway_auth_middleware,
            )),
        );

        Self { router }
    }

    pub fn route(self, path: &str, method_router: MethodRouter<ApiGatewayStateRef>) -> Self {
        Self {
            router: self.router.route(path, method_router),
        }
    }

    pub fn build(self) -> RApiGatewayRouter {
        self.router
    }
}
