use super::{connection::PostgresConnectionTrait, extended::PreparedStatement};
use crate::{
    compile::{
        convert_statement_to_cube_query,
        parser::{parse_sql_to_statement, parse_sql_to_statements},
        CompilationError, MetaContext, QueryPlan,
    },
    sql::{
        connection::PostgresConnection,
        df_type_to_pg_tid,
        extended::{Cursor, Portal, PortalFrom},
        session::DatabaseProtocol,
        statement::{
            PostgresStatementParamsFinder, SensitiveDataSanitizer, StatementPlaceholderReplacer,
        },
        types::CommandCompletion,
        writer::BatchWriter,
        AuthContextRef, ServerManager, Session, SessionManager, StatusFlags,
    },
    telemetry::ContextLogger,
    CubeError,
};
use async_trait::async_trait;
use log::{debug, error, trace};
use pg_srv::{
    buffer, protocol,
    protocol::{
        CancelRequest, ErrorCode, ErrorResponse, Format, InitialMessage, PasswordMessage,
        PortalCompletion,
    },
    PgType, PgTypeId, ProtocolError,
};
use sqlparser::ast::{self, CloseCursor, FetchDirection, Query, SetExpr, Statement, Value};
use std::{
    backtrace::Backtrace, collections::HashMap, io::ErrorKind, marker::PhantomData, sync::Arc,
};
use tokio::{io::AsyncWriteExt, net::TcpStream};
use tokio_util::sync::CancellationToken;

#[derive(PartialEq, Eq)]
pub enum StartupState {
    // Initial parameters which client sends in the first message, we use it later in auth method
    Success(HashMap<String, String>),
    SslRequested,
    Denied,
    CancelRequest,
}

pub trait QueryPlanExt {
    fn to_row_description(
        &self,
        required_format: protocol::Format,
    ) -> Result<Option<protocol::RowDescription>, ConnectionError>;
}

impl QueryPlanExt for QueryPlan {
    /// This method returns schema for response
    /// None is used for special queries, which doesnt have any data, for example: DISCARD ALL
    fn to_row_description(
        &self,
        required_format: protocol::Format,
    ) -> Result<Option<protocol::RowDescription>, ConnectionError> {
        match &self {
            QueryPlan::MetaOk(_, _) => Ok(None),
            QueryPlan::MetaTabular(_, frame) => {
                let mut result = vec![];

                for field in frame.get_columns() {
                    result.push(protocol::RowDescriptionField::new(
                        field.get_name(),
                        PgType::get_by_tid(PgTypeId::TEXT),
                        required_format,
                    ));
                }

                Ok(Some(protocol::RowDescription::new(result)))
            }
            QueryPlan::DataFusionSelect(_, logical_plan, _) => {
                let mut result = vec![];

                for field in logical_plan.schema().fields() {
                    result.push(protocol::RowDescriptionField::new(
                        field.name().clone(),
                        df_type_to_pg_tid(field.data_type())?.to_type(),
                        required_format,
                    ));
                }

                Ok(Some(protocol::RowDescription::new(result)))
            }
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ConnectionError {
    #[error(transparent)]
    Cube(#[from] CubeError),
    #[error(transparent)]
    CompilationError(#[from] CompilationError),
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
}

impl ConnectionError {
    /// Return Backtrace from any variant of Enum
    pub fn backtrace(&self) -> Option<&Backtrace> {
        match &self {
            ConnectionError::Cube(_) => None,
            ConnectionError::CompilationError(e) => e.clone().backtrace(),
            ConnectionError::Protocol(e) => e.backtrace(),
        }
    }

    /// Converts Error to protocol::ErrorResponse which is usefully for writing response to the client
    pub fn to_error_response(self) -> protocol::ErrorResponse {
        match self {
            ConnectionError::Cube(e) => {
                protocol::ErrorResponse::error(protocol::ErrorCode::InternalError, e.to_string())
            }
            ConnectionError::CompilationError(e) => {
                fn to_error_response(e: CompilationError) -> protocol::ErrorResponse {
                    match e {
                        CompilationError::Internal(_, _, _) => protocol::ErrorResponse::error(
                            protocol::ErrorCode::InternalError,
                            e.to_string(),
                        ),
                        CompilationError::User(_, _) => protocol::ErrorResponse::error(
                            protocol::ErrorCode::InvalidSqlStatement,
                            e.to_string(),
                        ),
                        CompilationError::Unsupported(_, _) => protocol::ErrorResponse::error(
                            protocol::ErrorCode::FeatureNotSupported,
                            e.to_string(),
                        ),
                    }
                }

                to_error_response(e)
            }
            ConnectionError::Protocol(e) => e.to_error_response(),
        }
    }
}

impl From<datafusion::error::DataFusionError> for ConnectionError {
    fn from(e: datafusion::error::DataFusionError) -> Self {
        ConnectionError::Cube(e.into())
    }
}

impl From<datafusion::arrow::error::ArrowError> for ConnectionError {
    fn from(e: datafusion::arrow::error::ArrowError) -> Self {
        ConnectionError::Cube(e.into())
    }
}

/// Auto converting for all kind of io:Error to ConnectionError, sugar
impl From<std::io::Error> for ConnectionError {
    fn from(e: std::io::Error) -> Self {
        ConnectionError::Protocol(e.into())
    }
}

/// Auto converting for all kind of io:Error to ConnectionError, sugar
impl From<ErrorResponse> for ConnectionError {
    fn from(e: ErrorResponse) -> Self {
        ConnectionError::Protocol(e.into())
    }
}

pub type InitialParameters = HashMap<String, String>;

pub enum AuthenticateResponse<T> {
    Success(T),
    Failed,
}

/// PostgresServer aims to handle initial handshake between Client and Server
/// We use this to split Connection (with session) and Handshake process (InitialStartupMessage, SSL, CancelRequest) without creating a session
/// Attention: It must not handle authenticate or any kind of commands excluding initial handshake
#[async_trait]
pub trait PostgresServerTrait {
    // You trait should define a custom structure, where you can put additional information in your authenticate method
    // After that, it will be available in create_connection method, which allows you to put it inside Connection (session)
    type AuthResponsePayload;

    type ConnectionType: PostgresConnectionTrait;

    async fn authenticate(
        &self,
        messsage: protocol::PasswordMessage,
    ) -> Result<AuthenticateResponse<Self::AuthResponsePayload>, ConnectionError>;

    /// Methods which allows to create custom object for connection which can store information about Session and Query Engine
    /// PostgresHandlerTrait will call this method to get a connection by moving socket to it
    async fn create_connection(
        &self,
        socket: TcpStream,
        auth_result: Self::AuthResponsePayload,
        parameters: InitialParameters,
    ) -> Self::ConnectionType;

    // Postgres supports canceling requests via sending special command at initial point called CancelRequest
    // It doesn't use default authentication process, because it uses
    async fn process_cancel(
        &self,
        cancel_message: protocol::CancelRequest,
    ) -> Result<StartupState, ConnectionError>;
}

/// Initial connection handler
pub struct PostgresServerIntermediary<S: PostgresServerTrait> {
    socket: TcpStream,
    server: Arc<S>,
}

impl<S: PostgresServerTrait> PostgresServerIntermediary<S> {
    pub async fn run_on(socket: TcpStream, server: Arc<S>) -> Result<(), ConnectionError> {
        let mut shim = Self { socket, server };

        let initial_parameters = match shim.process_initial_message().await? {
            StartupState::Success(parameters) => parameters,
            StartupState::SslRequested => match shim.process_initial_message().await? {
                StartupState::Success(parameters) => parameters,
                _ => return Ok(()),
            },
            StartupState::Denied | StartupState::CancelRequest => return Ok(()),
        };

        let auth_result = match buffer::read_message(&mut shim.socket).await? {
            protocol::FrontendMessage::PasswordMessage(password_message) => {
                match shim.server.authenticate(password_message).await? {
                    AuthenticateResponse::Success(r) => r,
                    AuthenticateResponse::Failed => {
                        return Ok(());
                    }
                }
            }
            _ => return Ok(()),
        };

        let mut connection = shim
            .server
            .create_connection(shim.socket, auth_result, initial_parameters)
            .await;

        connection.run();

        Ok(())
    }

    async fn process_initial_message(&mut self) -> Result<StartupState, ConnectionError> {
        let mut buffer = buffer::read_contents(&mut self.socket, 0).await?;

        let initial_message = protocol::InitialMessage::from(&mut buffer).await?;
        match initial_message {
            InitialMessage::Startup(startup) => self.process_startup_message(startup).await,
            InitialMessage::CancelRequest(cancel) => self.server.process_cancel(cancel).await,
            InitialMessage::Gssenc | InitialMessage::SslRequest => {
                self.write(protocol::SSLResponse::new()).await?;
                return Ok(StartupState::SslRequested);
            }
        }
    }

    async fn process_startup_message(
        &mut self,
        startup_message: protocol::StartupMessage,
    ) -> Result<StartupState, ConnectionError> {
        if startup_message.major != 3 || startup_message.minor != 0 {
            let error_response = protocol::ErrorResponse::new(
                protocol::ErrorSeverity::Fatal,
                protocol::ErrorCode::FeatureNotSupported,
                format!(
                    "unsupported frontend protocol {}.{}: server supports 3.0 to 3.0",
                    startup_message.major, startup_message.minor,
                ),
            );
            buffer::write_message(&mut self.socket, error_response).await?;
            return Ok(StartupState::Denied);
        }

        let mut parameters = startup_message.parameters;
        if !parameters.contains_key("user") {
            let error_response = protocol::ErrorResponse::new(
                protocol::ErrorSeverity::Fatal,
                protocol::ErrorCode::InvalidAuthorizationSpecification,
                "no PostgreSQL user name specified in startup packet".to_string(),
            );
            buffer::write_message(&mut self.socket, error_response).await?;
            return Ok(StartupState::Denied);
        }

        if !parameters.contains_key("database") {
            parameters.insert("database".to_string(), "db".to_string());
        }

        self.write(protocol::Authentication::new(
            protocol::AuthenticationRequest::CleartextPassword,
        ))
        .await?;

        Ok(StartupState::Success(parameters))
    }

    async fn write<Message: protocol::Serialize>(
        &mut self,
        message: Message,
    ) -> Result<(), ConnectionError> {
        buffer::write_message(&mut self.socket, message).await?;

        Ok(())
    }
}
