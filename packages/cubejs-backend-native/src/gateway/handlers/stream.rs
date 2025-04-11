use crate::gateway::auth_middleware::AuthExtension;
use crate::gateway::http_error::HttpError;
use crate::gateway::state::ApiGatewayStateRef;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::{Extension, Json};
use serde::Serialize;

#[derive(Serialize)]
pub struct HandlerResponse {
    message: String,
}

pub async fn stream_handler_v2(
    State(gateway_state): State<ApiGatewayStateRef>,
    Extension(auth): Extension<AuthExtension>,
) -> Result<impl IntoResponse, HttpError> {
    gateway_state
        .assert_api_scope(auth.auth_context(), "data")
        .await?;

    Ok((
        StatusCode::NOT_IMPLEMENTED,
        Json(HandlerResponse {
            message: "/v2/stream is not implemented".to_string(),
        }),
    ))
}
