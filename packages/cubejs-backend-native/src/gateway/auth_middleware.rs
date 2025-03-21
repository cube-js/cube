use std::sync::Arc;
use axum::extract::State;
use axum::http::{header, HeaderMap, HeaderValue, Response, StatusCode};
use axum::response::IntoResponse;
use crate::gateway::{ApiGatewayState, GatewayAuthContextRef, GatewayAuthService, GatewayCheckAuthRequest};
use crate::gateway::http_error::HttpError;

#[derive(Debug, Clone)]
pub struct AuthExtension {
    auth_context: GatewayAuthContextRef,
}

pub async fn gateway_auth_middleware(
    State(state): State<ApiGatewayState>,
    mut req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Result<impl IntoResponse, HttpError> {
    let Some(token_header_value) = req.headers().get("authorization") else {
        return Err(HttpError::unauthorized("No authorization header".to_string()));
    };

    let auth = state
        .injector_ref()
        .get_service_typed::<dyn GatewayAuthService>()
        .await;

    let auth_fut = auth.authenticate(
        GatewayCheckAuthRequest {
            protocol: "http".to_string(),
        },
        token_header_value.to_str()?.to_string()
    );

    let auth_response = auth_fut.await.map_err(|err| {;
        HttpError::unauthorized("Authentication error".to_string())
    })?;

    req.extensions_mut().insert(AuthExtension { auth_context: auth_response.context });

    let response = next.run(req).await;
    Ok(response)
}
