use crate::gateway::ApiGatewayState;
use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use serde_derive::Serialize;

#[derive(Serialize)]
pub struct HandlerResponse {
    message: String,
}

pub async fn stream_handler_v2(
    State(_state): State<ApiGatewayState>,
) -> (StatusCode, Json<HandlerResponse>) {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(HandlerResponse {
            message: "Not implemented".to_string(),
        }),
    )
}
