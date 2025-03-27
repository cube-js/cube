use crate::gateway::http_error::{HttpError, HttpErrorCode, HttpStatusCode};
use crate::gateway::{GatewayAuthContextRef, GatewayAuthService};
use cubesql::config::injection::Injector;
use std::sync::Arc;

pub struct ApiGatewayState {
    injector: Arc<Injector>,
}

pub type ApiGatewayStateRef = Arc<ApiGatewayState>;

impl ApiGatewayState {
    pub fn new(injector: Arc<Injector>) -> Self {
        Self { injector }
    }

    pub fn injector_ref(&self) -> &Arc<Injector> {
        &self.injector
    }

    pub async fn assert_api_scope(
        &self,
        gateway_auth_context: &GatewayAuthContextRef,
        api_scope: &str,
    ) -> Result<(), HttpError> {
        let auth_service = self
            .injector_ref()
            .get_service_typed::<dyn GatewayAuthService>()
            .await;

        let api_scopes_res = auth_service
            .context_to_api_scopes(gateway_auth_context)
            .await
            .map_err(|err| {
                log::error!("Error getting API scopes: {}", err);

                HttpError::from_user_with_status_code(
                    err,
                    HttpErrorCode::StatusCode(HttpStatusCode::INTERNAL_SERVER_ERROR),
                )
            })?;
        if !api_scopes_res.scopes.contains(&api_scope.to_string()) {
            Err(HttpError::forbidden(format!(
                "API scope is missing: {}",
                api_scope
            )))
        } else {
            Ok(())
        }
    }
}
