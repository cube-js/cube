use async_trait::async_trait;
use cubesql::{
    di_service,
    sql::{AuthContext, AuthenticateResponse, SqlAuthService, SqlAuthServiceAuthenticateRequest},
    transport::LoadRequestMeta,
    CubeError,
};
use log::trace;
use neon::prelude::*;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::sync::Arc;
use uuid::Uuid;

use crate::channel::call_js_with_channel_as_callback;
use crate::gateway::auth_service::GatewayContextToApiScopesResponse;
use crate::gateway::{
    GatewayAuthContext, GatewayAuthContextRef, GatewayAuthService, GatewayAuthenticateResponse,
    GatewayCheckAuthRequest,
};

#[derive(Debug)]
pub struct NodeBridgeAuthService {
    channel: Arc<Channel>,
    check_auth: Arc<Root<JsFunction>>,
    check_sql_auth: Arc<Root<JsFunction>>,
    context_to_api_scopes: Arc<Root<JsFunction>>,
}

pub struct NodeBridgeAuthServiceOptions {
    pub check_auth: Root<JsFunction>,
    pub check_sql_auth: Root<JsFunction>,
    pub context_to_api_scopes: Root<JsFunction>,
}

impl NodeBridgeAuthService {
    pub fn new(channel: Channel, options: NodeBridgeAuthServiceOptions) -> Self {
        Self {
            channel: Arc::new(channel),
            check_auth: Arc::new(options.check_auth),
            check_sql_auth: Arc::new(options.check_sql_auth),
            context_to_api_scopes: Arc::new(options.context_to_api_scopes),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct TransportRequest {
    pub id: String,
    pub meta: Option<LoadRequestMeta>,
}

#[derive(Debug, Serialize)]
pub struct TransportAuthRequest {
    pub id: String,
    pub meta: Option<LoadRequestMeta>,
    pub protocol: String,
    pub method: String,
}

impl From<(TransportRequest, SqlAuthServiceAuthenticateRequest)> for TransportAuthRequest {
    fn from((t, a): (TransportRequest, SqlAuthServiceAuthenticateRequest)) -> Self {
        Self {
            id: t.id,
            meta: t.meta,
            protocol: a.protocol,
            method: a.method,
        }
    }
}

#[derive(Debug, Serialize)]
struct CheckSQLAuthTransportRequest {
    request: TransportAuthRequest,
    user: Option<String>,
    password: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CheckSQLAuthTransportResponse {
    password: Option<String>,
    superuser: bool,
    #[serde(rename = "securityContext", skip_serializing_if = "Option::is_none")]
    security_context: Option<serde_json::Value>,
    #[serde(rename = "skipPasswordCheck", skip_serializing_if = "Option::is_none")]
    skip_password_check: Option<bool>,
}

#[derive(Debug)]
pub struct NativeSQLAuthContext {
    pub user: Option<String>,
    pub superuser: bool,
    pub security_context: Option<serde_json::Value>,
}

impl AuthContext for NativeSQLAuthContext {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn user(&self) -> Option<&String> {
        self.user.as_ref()
    }

    fn security_context(&self) -> Option<&serde_json::Value> {
        self.security_context.as_ref()
    }
}

#[async_trait]
impl SqlAuthService for NodeBridgeAuthService {
    async fn authenticate(
        &self,
        request: SqlAuthServiceAuthenticateRequest,
        user: Option<String>,
        password: Option<String>,
    ) -> Result<AuthenticateResponse, CubeError> {
        trace!("[sql auth] Request ->");

        let request_id = Uuid::new_v4().to_string();

        let extra = serde_json::to_string(&CheckSQLAuthTransportRequest {
            request: TransportAuthRequest {
                id: format!("{}-span-1", request_id),
                meta: None,
                protocol: request.protocol,
                method: request.method,
            },
            user: user.clone(),
            password: password.clone(),
        })?;
        let response: CheckSQLAuthTransportResponse = call_js_with_channel_as_callback(
            self.channel.clone(),
            self.check_sql_auth.clone(),
            Some(extra),
        )
        .await?;
        trace!("[sql auth] Request <- {:?}", response);

        Ok(AuthenticateResponse {
            context: Arc::new(NativeSQLAuthContext {
                user,
                superuser: response.superuser,
                security_context: response.security_context,
            }),
            password: response.password,
            skip_password_check: response.skip_password_check.unwrap_or(false),
        })
    }
}

#[derive(Debug, Serialize)]
struct CheckAuthTransportRequest {
    request: GatewayCheckAuthRequest,
    token: String,
}

#[derive(Debug, Deserialize)]
struct CheckAuthTransportResponse {
    #[serde(rename = "securityContext", skip_serializing_if = "Option::is_none")]
    security_context: Option<serde_json::Value>,
}

#[derive(Debug)]
pub struct NativeGatewayAuthContext {
    pub security_context: Option<serde_json::Value>,
}

impl GatewayAuthContext for NativeGatewayAuthContext {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn user(&self) -> Option<&String> {
        None
    }

    fn security_context(&self) -> Option<&serde_json::Value> {
        self.security_context.as_ref()
    }
}

#[derive(Debug, Serialize)]
struct ContextToApiScopesTransportRequest<'ref_auth_context> {
    security_context: &'ref_auth_context Option<serde_json::Value>,
}

type ContextToApiScopesTransportResponse = Vec<String>;

#[async_trait]
impl GatewayAuthService for NodeBridgeAuthService {
    async fn authenticate(
        &self,
        request: GatewayCheckAuthRequest,
        token: String,
    ) -> Result<GatewayAuthenticateResponse, CubeError> {
        trace!("[auth] Request ->");

        let extra = serde_json::to_string(&CheckAuthTransportRequest {
            request,
            token: token.clone(),
        })?;
        let response: CheckAuthTransportResponse = call_js_with_channel_as_callback(
            self.channel.clone(),
            self.check_auth.clone(),
            Some(extra),
        )
        .await?;
        trace!("[auth] Request <- {:?}", response);

        Ok(GatewayAuthenticateResponse {
            context: Arc::new(NativeGatewayAuthContext {
                security_context: response.security_context,
            }),
        })
    }

    async fn context_to_api_scopes(
        &self,
        auth_context: &GatewayAuthContextRef,
    ) -> Result<GatewayContextToApiScopesResponse, CubeError> {
        trace!("[context_to_api_scopes] Request ->");

        let native_auth = auth_context
            .as_any()
            .downcast_ref::<NativeGatewayAuthContext>()
            .expect("Unable to cast AuthContext to NativeGatewayAuthContext");

        let extra = serde_json::to_string(&ContextToApiScopesTransportRequest {
            security_context: &native_auth.security_context,
        })?;
        let response: ContextToApiScopesTransportResponse = call_js_with_channel_as_callback(
            self.channel.clone(),
            self.context_to_api_scopes.clone(),
            Some(extra),
        )
        .await?;
        trace!("[context_to_api_scopes] Request <- {:?}", response);

        Ok(GatewayContextToApiScopesResponse { scopes: response })
    }
}

di_service!(NodeBridgeAuthService, [SqlAuthService, GatewayAuthService]);
