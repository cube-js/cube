use crate::gateway::http_error::HttpError;
use crate::gateway::state::ApiGatewayStateRef;
use crate::gateway::{GatewayAuthContextRef, GatewayAuthService, GatewayCheckAuthRequest};
use axum::extract::State;
use axum::http::HeaderValue;
use axum::response::IntoResponse;

#[derive(Debug, Clone)]
pub struct AuthExtension {
    auth_context: GatewayAuthContextRef,
}

impl AuthExtension {
    pub fn auth_context(&self) -> &GatewayAuthContextRef {
        &self.auth_context
    }
}

fn parse_token(header_value: &HeaderValue) -> Result<&str, HttpError> {
    let trimmed = header_value.to_str()?.trim();

    let stripped = if let Some(stripped) = trimmed.strip_prefix("Bearer ") {
        stripped
    } else if let Some(stripped) = trimmed.strip_prefix("bearer ") {
        stripped
    } else {
        trimmed
    };

    if stripped.is_empty() {
        Err(HttpError::unauthorized(
            "Value for authorization header cannot be empty".to_string(),
        ))
    } else {
        Ok(stripped)
    }
}

pub async fn gateway_auth_middleware(
    State(state): State<ApiGatewayStateRef>,
    mut req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Result<impl IntoResponse, HttpError> {
    let Some(token_header_value) = req.headers().get("authorization") else {
        return Err(HttpError::unauthorized(
            "No authorization header".to_string(),
        ));
    };

    let bearer_token = parse_token(token_header_value)?;

    let auth = state
        .injector_ref()
        .get_service_typed::<dyn GatewayAuthService>()
        .await;

    let auth_fut = auth.authenticate(
        GatewayCheckAuthRequest {
            protocol: "http".to_string(),
        },
        bearer_token.to_string(),
    );

    let auth_response = auth_fut
        .await
        .map_err(|_err| HttpError::unauthorized("Authentication error".to_string()))?;

    req.extensions_mut().insert(AuthExtension {
        auth_context: auth_response.context,
    });

    let response = next.run(req).await;
    Ok(response)
}
