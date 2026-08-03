use std::{collections::HashMap, fmt::Debug, sync::Arc};

use async_trait::async_trait;

use crate::{
    sql::auth_service::SqlAuthServiceAuthenticateRequest,
    sql::{AuthContextRef, SqlAuthService},
    CubeError,
};

pub use pg_srv::{
    buffer as pg_srv_buffer,
    protocol::{
        AuthenticationRequest, AuthenticationRequestExtension, FrontendMessage,
        FrontendMessageExtension,
    },
    MessageTagParser, MessageTagParserDefaultImpl, ProtocolError,
};

#[derive(Debug)]
pub enum AuthenticationStatus {
    UnexpectedFrontendMessage,
    Failed(String),
    // User name + auth context
    Success(String, AuthContextRef),
}

#[async_trait]
pub trait PostgresAuthService: Sync + Send + Debug {
    fn get_auth_method(&self, parameters: &HashMap<String, String>) -> AuthenticationRequest;

    async fn authenticate(
        &self,
        service: Arc<dyn SqlAuthService>,
        request: AuthenticationRequest,
        secret: FrontendMessage,
        parameters: &HashMap<String, String>,
    ) -> AuthenticationStatus;

    fn get_pg_message_tag_parser(&self) -> Arc<dyn MessageTagParser>;
}

#[derive(Debug)]
pub struct PostgresAuthServiceDefaultImpl {
    pg_message_tag_parser: Arc<dyn MessageTagParser>,
}

impl PostgresAuthServiceDefaultImpl {
    pub fn new() -> Self {
        Self {
            pg_message_tag_parser: Arc::new(MessageTagParserDefaultImpl::default()),
        }
    }
}

#[async_trait]
impl PostgresAuthService for PostgresAuthServiceDefaultImpl {
    fn get_auth_method(&self, _: &HashMap<String, String>) -> AuthenticationRequest {
        AuthenticationRequest::CleartextPassword
    }

    async fn authenticate(
        &self,
        service: Arc<dyn SqlAuthService>,
        request: AuthenticationRequest,
        secret: FrontendMessage,
        parameters: &HashMap<String, String>,
    ) -> AuthenticationStatus {
        let FrontendMessage::PasswordMessage(password_message) = secret else {
            return AuthenticationStatus::UnexpectedFrontendMessage;
        };

        if !matches!(request, AuthenticationRequest::CleartextPassword) {
            return AuthenticationStatus::UnexpectedFrontendMessage;
        }

        let user = parameters.get("user").unwrap().clone();
        let sql_auth_request = SqlAuthServiceAuthenticateRequest {
            protocol: "postgres".to_string(),
            method: "password".to_string(),
        };
        let authenticate_response = service
            .authenticate(
                sql_auth_request,
                Some(user.clone()),
                Some(password_message.password.clone()),
            )
            .await;

        let auth_fail = || {
            AuthenticationStatus::Failed(format!(
                "password authentication failed for user \"{}\"",
                user
            ))
        };

        let Ok(authenticate_response) = authenticate_response else {
            return auth_fail();
        };

        if !authenticate_response.skip_password_check {
            let is_password_correct = match authenticate_response.password {
                None => false,
                Some(password) => password == password_message.password,
            };
            if !is_password_correct {
                return auth_fail();
            }
        }

        AuthenticationStatus::Success(user, authenticate_response.context)
    }

    fn get_pg_message_tag_parser(&self) -> Arc<dyn MessageTagParser> {
        Arc::clone(&self.pg_message_tag_parser)
    }
}

crate::di_service!(PostgresAuthServiceDefaultImpl, [PostgresAuthService]);
