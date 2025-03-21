use crate::gateway::http_error::HttpError;
use crate::gateway::state::ApiGatewayStateRef;
use crate::gateway::{GatewayAuthContextRef, GatewayAuthService, GatewayCheckAuthRequest};
use axum::extract::State;
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

    let auth = state
        .injector_ref()
        .get_service_typed::<dyn GatewayAuthService>()
        .await;

    let auth_fut = auth.authenticate(
        GatewayCheckAuthRequest {
            protocol: "http".to_string(),
        },
        token_header_value.to_str()?.to_string(),
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
