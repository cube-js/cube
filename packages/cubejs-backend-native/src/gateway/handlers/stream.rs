use crate::gateway::{ApiGatewayRouterBuilder, ApiGatewayState};
use axum::extract::State;
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::Serialize;
use crate::gateway::auth_middleware::AuthExtension;

#[derive(Serialize)]
pub struct HandlerResponse {
    message: String,
}

pub async fn stream_handler_v2(
    State(_gateway_state): State<ApiGatewayState>,
    Extension(_auth): Extension<AuthExtension>,
) -> (StatusCode, Json<HandlerResponse>) {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(HandlerResponse {
            message: "Not implemented".to_string(),
        }),
    )
}
