use std::{
    backtrace::Backtrace, collections::HashMap, io::ErrorKind, pin::pin, pin::Pin, sync::Arc,
    time::SystemTime,
};

use super::{extended::PreparedStatement, pg_auth_service::AuthenticationStatus};
use crate::{
    compile::{
        convert_statement_to_cube_query,
        parser::{parse_sql_to_statement, parse_sql_to_statements},
        qtrace::Qtrace,
        CommandCompletion, CompilationError, DatabaseProtocol, QueryPlan, StatusFlags,
    },
    sql::{
        compiler_cache::CompilerCacheEntry,
        df_type_to_pg_tid,
        extended::{Cursor, Portal, PortalBatch, PortalFrom},
        statement::{PostgresStatementParamsFinder, StatementPlaceholderReplacer},
        AuthContextRef, Session, SessionState,
    },
    telemetry::ContextLogger,
    transport::{MetaContext, SpanId},
    CubeError,
};
use futures::{FutureExt, StreamExt};
use log::{debug, error, trace};
use pg_srv::{
    buffer,
    protocol::{
        self, AuthenticationRequest, ErrorCode, ErrorResponse, Format, InitialMessage,
        PortalCompletion,
    },
    PgType, PgTypeId, ProtocolError,
};
use sqlparser::ast::{self, CloseCursor, FetchDirection, Query, SetExpr, Statement, Value};
use tokio::{io::AsyncWriteExt, net::TcpStream};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub struct AsyncPostgresShim {
    socket: TcpStream,
    // If empty, this means socket is on a message boundary.
    partial_write_buf: bytes::BytesMut,
    semifast_shutdown_interruptor: CancellationToken,
    // Extended query
    cursors: HashMap<String, Cursor>,
    portals: HashMap<String, Portal>,
    // Shared
    session: Arc<Session>,
    logger: Arc<dyn ContextLogger>,
}

pub enum StartupState {
    // Initial parameters which client sends in the first message, we use it later in auth method
    Success(HashMap<String, String>, AuthenticationRequest),
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
            QueryPlan::MetaOk(_, _) | QueryPlan::CreateTempTable(_, _, _, _) => Ok(None),
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
            QueryPlan::DataFusionSelect(logical_plan, _) => {
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
    #[error("CubeError: {0}")]
    Cube(CubeError, Option<Arc<SpanId>>),
    #[error("CompilationError: {0}")]
    CompilationError(CompilationError, Option<Arc<SpanId>>),
    #[error("ProtocolError: {0}")]
    Protocol(ProtocolError, Option<Arc<SpanId>>),
}

impl ConnectionError {
    /// Return Backtrace from any variant of Enum
    pub fn backtrace(&self) -> Option<&Backtrace> {
        match &self {
            ConnectionError::Cube(e, _) => e.backtrace(),
            ConnectionError::CompilationError(e, _) => e.backtrace(),
            ConnectionError::Protocol(e, _) => e.backtrace(),
        }
    }

    /// Converts Error to protocol::ErrorResponse which is usefully for writing response to the client
    pub fn to_error_response(self) -> protocol::ErrorResponse {
        match self {
            ConnectionError::Cube(e, _) => {
                protocol::ErrorResponse::error(protocol::ErrorCode::InternalError, e.to_string())
            }
            ConnectionError::CompilationError(e, _) => {
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
                        CompilationError::Fatal(_, _) => protocol::ErrorResponse::fatal(
                            protocol::ErrorCode::InternalError,
                            e.to_string(),
                        ),
                    }
                }

                to_error_response(e)
            }
            ConnectionError::Protocol(e, _) => e.to_error_response(),
        }
    }

    pub fn with_span_id(self, span_id: Option<Arc<SpanId>>) -> Self {
        match self {
            ConnectionError::Cube(e, _) => ConnectionError::Cube(e, span_id),
            ConnectionError::CompilationError(e, _) => {
                ConnectionError::CompilationError(e, span_id)
            }
            ConnectionError::Protocol(e, _) => ConnectionError::Protocol(e, span_id),
        }
    }

    pub fn span_id(&self) -> Option<Arc<SpanId>> {
        match self {
            ConnectionError::Cube(_, span_id) => span_id.clone(),
            ConnectionError::CompilationError(_, span_id) => span_id.clone(),
            ConnectionError::Protocol(_, span_id) => span_id.clone(),
        }
    }
}

impl From<CubeError> for ConnectionError {
    fn from(e: CubeError) -> Self {
        ConnectionError::Cube(e, None)
    }
}

impl From<CompilationError> for ConnectionError {
    fn from(e: CompilationError) -> Self {
        ConnectionError::CompilationError(e, None)
    }
}

impl From<ProtocolError> for ConnectionError {
    fn from(e: ProtocolError) -> Self {
        ConnectionError::Protocol(e, None)
    }
}

impl From<tokio::task::JoinError> for ConnectionError {
    fn from(e: tokio::task::JoinError) -> Self {
        ConnectionError::Cube(e.into(), None)
    }
}

impl From<datafusion::error::DataFusionError> for ConnectionError {
    fn from(e: datafusion::error::DataFusionError) -> Self {
        ConnectionError::Cube(e.into(), None)
    }
}

impl From<datafusion::arrow::error::ArrowError> for ConnectionError {
    fn from(e: datafusion::arrow::error::ArrowError) -> Self {
        ConnectionError::Cube(e.into(), None)
    }
}

/// Auto converting for all kind of io:Error to ConnectionError, sugar
impl From<std::io::Error> for ConnectionError {
    fn from(e: std::io::Error) -> Self {
        ConnectionError::Protocol(e.into(), None)
    }
}

/// Auto converting for all kind of io:Error to ConnectionError, sugar
impl From<ErrorResponse> for ConnectionError {
    fn from(e: ErrorResponse) -> Self {
        ConnectionError::Protocol(e.into(), None)
    }
}

impl AsyncPostgresShim {
    async fn flush_and_write_admin_shutdown_fatal_message(
        shim: &mut AsyncPostgresShim,
    ) -> Result<(), ConnectionError> {
        // We flush the partially written buf and add the fatal message -- it's another place's
        // responsibility to impose a timeout and abort us.
        shim.socket
            .write_all_buf(&mut shim.partial_write_buf)
            .await?;
        shim.partial_write_buf = bytes::BytesMut::new();
        shim.write_admin_shutdown_fatal_message().await?;
        return Ok(());
    }

    async fn get_cache_entry(&self) -> Result<Arc<CompilerCacheEntry>, CubeError> {
        self.session
            .session_manager
            .server
            .compiler_cache
            .get_cache_entry(self.auth_context()?, self.session.state.protocol.clone())
            .await
    }

    pub async fn run_on(
        fast_shutdown_interruptor: CancellationToken,
        semifast_shutdown_interruptor: CancellationToken,
        socket: TcpStream,
        session: Arc<Session>,
        logger: Arc<dyn ContextLogger>,
    ) -> Result<(), ConnectionError> {
        let mut shim = Self {
            semifast_shutdown_interruptor,
            socket,
            partial_write_buf: bytes::BytesMut::new(),
            cursors: HashMap::new(),
            portals: HashMap::new(),
            session,
            logger,
        };

        let run_result = tokio::select! {
            _ = fast_shutdown_interruptor.cancelled() => {
                Self::flush_and_write_admin_shutdown_fatal_message(&mut shim).await?;
                shim.socket.shutdown().await?;
                return Ok(());
            }
            res = shim.run() => res,
        };

        match run_result {
            Err(e) => {
                if let ConnectionError::Protocol(ProtocolError::IO { source, .. }, _) = &e {
                    if source.kind() == ErrorKind::BrokenPipe
                        || source.kind() == ErrorKind::UnexpectedEof
                    {
                        trace!("Error during processing PostgreSQL connection: {}", e);

                        return Ok(());
                    }
                } else if let ConnectionError::CompilationError(CompilationError::Fatal(_, _), _) =
                    &e
                {
                    assert!(shim.partial_write_buf.is_empty());
                    shim.write(e.to_error_response()).await?;
                    shim.socket.shutdown().await?;
                    return Ok(());
                }

                Err(e)
            }
            _ => {
                shim.socket.shutdown().await?;
                return Ok(());
            }
        }
    }

    fn session_state_is_semifast_shutdownable(session_state: &SessionState) -> bool {
        return !session_state.is_in_transaction() && !session_state.has_current_query();
    }

    fn is_semifast_shutdownable(&self) -> bool {
        return self.cursors.is_empty()
            && self.portals.is_empty()
            && Self::session_state_is_semifast_shutdownable(&self.session.state);
    }

    fn admin_shutdown_error() -> ConnectionError {
        ConnectionError::Protocol(
            ProtocolError::ErrorResponse {
                source: ErrorResponse::admin_shutdown(),
                backtrace: Backtrace::disabled(),
            },
            None,
        )
    }

    pub async fn run(&mut self) -> Result<(), ConnectionError> {
        let (initial_parameters, auth_method) = match self.process_initial_message().await? {
            StartupState::Success(parameters, auth_method) => (parameters, auth_method),
            StartupState::SslRequested => match self.process_initial_message().await? {
                StartupState::Success(parameters, auth_method) => (parameters, auth_method),
                _ => return Ok(()),
            },
            StartupState::Denied | StartupState::CancelRequest => return Ok(()),
        };

        let message_tag_parser = self.session.server.pg_auth.get_pg_message_tag_parser();
        let auth_secret =
            buffer::read_message(&mut self.socket, Arc::clone(&message_tag_parser)).await?;
        if !self
            .authenticate(auth_method, auth_secret, initial_parameters)
            .await?
        {
            return Ok(());
        }

        self.ready().await?;

        // When an error is detected while processing any extended-query message, the backend issues ErrorResponse,
        // then reads and discards messages until a Sync is reached, then issues ReadyForQuery and returns to normal message processing.
        let mut tracked_error: Option<ConnectionError> = None;

        // Clone here to avoid conflicting borrows of self in the tokio::select!.
        let semifast_shutdown_interruptor = self.semifast_shutdown_interruptor.clone();

        loop {
            let mut doing_extended_query_message = false;
            let semifast_shutdownable = self.is_semifast_shutdownable();

            let message: protocol::FrontendMessage = tokio::select! {
                true = async { semifast_shutdownable && { semifast_shutdown_interruptor.cancelled().await; true } } => {
                    return Self::flush_and_write_admin_shutdown_fatal_message(self).await;
                }
                message_result = buffer::read_message(&mut self.socket, Arc::clone(&message_tag_parser)) => message_result?
            };

            let result = match message {
                protocol::FrontendMessage::Query(body) => {
                    let span_id = Self::new_span_id(body.query.clone());
                    let mut qtrace = Qtrace::new(&body.query);
                    if let Some(qtrace) = &qtrace {
                        debug!("Assigned query UUID: {}", qtrace.uuid())
                    }
                    let result = self
                        .process_query(body.query, &mut qtrace, span_id.clone())
                        .await
                        .map_err(|e| e.with_span_id(span_id));
                    if let Some(qtrace) = &qtrace {
                        qtrace.save_json()
                    }
                    result
                }
                protocol::FrontendMessage::Flush => self.flush().await,
                protocol::FrontendMessage::Terminate => return Ok(()),
                // Extended
                protocol::FrontendMessage::Parse(body) => {
                    if tracked_error.is_some() {
                        continue;
                    }
                    doing_extended_query_message = true;
                    let mut qtrace = Qtrace::new(&body.query);
                    let span_id = Self::new_span_id(body.query.clone());
                    if let Some(qtrace) = &qtrace {
                        debug!("Assigned query UUID: {}", qtrace.uuid())
                    }
                    if let Some(auth_context) = self.session.state.auth_context() {
                        self.session
                            .session_manager
                            .server
                            .transport
                            .log_load_state(
                                span_id.clone(),
                                auth_context,
                                self.session.state.get_load_request_meta("sql"),
                                "Load Request".to_string(),
                                serde_json::json!({
                                    "query": span_id.as_ref().unwrap().query_key.clone(),
                                    // Hide query by default until Execute
                                    "isDataQuery": false,
                                }),
                            )
                            .await?;
                    }
                    let result = self
                        .parse(body, &mut qtrace, span_id.clone())
                        .await
                        .map_err(|e| e.with_span_id(span_id));
                    if let Err(err) = &result {
                        if let Some(qtrace) = &mut qtrace {
                            qtrace.set_query_error_message(&err.to_string())
                        }
                    };
                    if let Some(qtrace) = &qtrace {
                        qtrace.save_json()
                    }
                    result
                }
                protocol::FrontendMessage::Bind(body) => {
                    if tracked_error.is_none() {
                        doing_extended_query_message = true;
                    }
                    let span_id = {
                        let statements_guard = self.session.state.statements.read().await;
                        statements_guard
                            .get(&body.statement)
                            .and_then(|s| s.span_id())
                    };
                    self.bind(body, span_id).await
                }
                protocol::FrontendMessage::Execute(body) => {
                    let span_id = self
                        .portals
                        .get(&body.portal)
                        .and_then(|portal| portal.span_id());
                    if tracked_error.is_some() {
                        if let Some(auth_context) = self.session.state.auth_context() {
                            if let Some(span_id) = span_id {
                                // If there was an error, always show the query
                                self.session
                                    .session_manager
                                    .server
                                    .transport
                                    .log_load_state(
                                        Some(span_id.clone()),
                                        auth_context,
                                        self.session.state.get_load_request_meta("sql"),
                                        "Data Query Status".to_string(),
                                        serde_json::json!({
                                            "isDataQuery": true
                                        }),
                                    )
                                    .await?;
                            }
                        }
                        continue;
                    }
                    doing_extended_query_message = true;
                    let result = self
                        .execute(body)
                        .await
                        .map_err(|e| e.with_span_id(span_id.clone()));
                    if let Some(auth_context) = self.session.state.auth_context() {
                        if let Some(span_id) = span_id {
                            // Always indicate whether this is a data query
                            // Errors are always visible ("data queries")
                            if result.is_err() {
                                self.session
                                    .session_manager
                                    .server
                                    .transport
                                    .log_load_state(
                                        Some(span_id.clone()),
                                        auth_context.clone(),
                                        self.session.state.get_load_request_meta("sql"),
                                        "Data Query Status".to_string(),
                                        serde_json::json!({
                                            "isDataQuery": true,
                                        }),
                                    )
                                    .await?;
                            } else {
                                self.session
                                    .session_manager
                                    .server
                                    .transport
                                    .log_load_state(
                                        Some(span_id.clone()),
                                        auth_context,
                                        self.session.state.get_load_request_meta("sql"),
                                        "Load Request Success".to_string(),
                                        serde_json::json!({
                                            "query": span_id.query_key.clone(),
                                            "apiType": "sql",
                                            "duration": span_id.duration(),
                                            "isDataQuery": span_id.is_data_query().await
                                        }),
                                    )
                                    .await?;
                            }
                        }
                    }
                    result
                }
                protocol::FrontendMessage::Close(body) => {
                    if tracked_error.is_none() {
                        self.close(body).await
                    } else {
                        continue;
                    }
                }
                protocol::FrontendMessage::Describe(body) => {
                    if tracked_error.is_none() {
                        self.describe(body).await
                    } else {
                        continue;
                    }
                }
                protocol::FrontendMessage::Sync => {
                    if let Some(err) = tracked_error.take() {
                        self.handle_connection_error(err).await?;
                    };

                    self.write_ready().await?;

                    continue;
                }
                command_id => {
                    return Err(ConnectionError::Protocol(
                        ErrorResponse::error(
                            ErrorCode::InternalError,
                            format!("Unsupported operation: {:?}", command_id),
                        )
                        .into(),
                        None,
                    ))
                }
            };
            if let Err(err) = result {
                if doing_extended_query_message {
                    tracked_error = Some(err);
                } else {
                    self.handle_connection_error(err).await?;
                }
            }
        }
    }

    fn new_span_id(sql: String) -> Option<Arc<SpanId>> {
        Some(Arc::new(SpanId::new(
            Uuid::new_v4().to_string(),
            serde_json::json!({ "sql": sql }),
        )))
    }

    pub async fn handle_connection_error(
        &mut self,
        err: ConnectionError,
    ) -> Result<(), ConnectionError> {
        let (message, props) = match &err {
            ConnectionError::CompilationError(e, _) => match e {
                CompilationError::Unsupported(msg, meta)
                | CompilationError::User(msg, meta)
                | CompilationError::Internal(msg, _, meta) => (msg.clone(), meta.clone()),
                CompilationError::Fatal(_, _) => return Err(err),
            },
            ConnectionError::Protocol(ProtocolError::IO { source, .. }, _) => match source.kind() {
                // Propagate unrecoverable errors to top level - run_on
                ErrorKind::UnexpectedEof | ErrorKind::BrokenPipe => return Err(err),
                _ => (
                    format!("Error during processing PostgreSQL message: {}", err),
                    None,
                ),
            },
            _ => (
                format!("Error during processing PostgreSQL message: {}", err),
                None,
            ),
        };

        if let Some(bt) = err.backtrace() {
            trace!("{}", bt);
        } else {
            trace!("Backtrace: not found");
        }

        if let Some(auth_context) = self.session.state.auth_context() {
            if let Some(span_id) = err.span_id() {
                self.session
                    .session_manager
                    .server
                    .transport
                    .log_load_state(
                        Some(span_id.clone()),
                        auth_context,
                        self.session.state.get_load_request_meta("sql"),
                        "SQL API Error".to_string(),
                        serde_json::json!({
                            "query": span_id.query_key.clone(),
                            "error": message.clone(),
                            "duration": span_id.duration(),
                        }),
                    )
                    .await?;
            }
        }

        let err_response = match &props {
            Some(props) => {
                let query = props.get(&"query".to_string());
                let mut err_response = err.to_error_response();
                if let Some(query) = query {
                    err_response.message = format!("{}\nQUERY: {}", message, query);
                }

                err_response
            }
            None => err.to_error_response(),
        };

        self.logger.error(message.as_str(), props);

        self.write(err_response).await?;

        Ok(())
    }

    pub async fn write_multi<Message: protocol::Serialize>(
        &mut self,
        message: Vec<Message>,
    ) -> Result<(), ConnectionError> {
        buffer::write_messages(&mut self.partial_write_buf, &mut self.socket, message).await?;

        Ok(())
    }

    pub async fn write_completion(
        &mut self,
        completion: PortalCompletion,
    ) -> Result<(), ConnectionError> {
        match completion {
            PortalCompletion::Complete(c) => {
                buffer::write_message(&mut self.partial_write_buf, &mut self.socket, c).await?
            }
            PortalCompletion::Suspended(s) => {
                buffer::write_message(&mut self.partial_write_buf, &mut self.socket, s).await?
            }
        }

        Ok(())
    }

    pub async fn write<Message: protocol::Serialize>(
        &mut self,
        message: Message,
    ) -> Result<(), ConnectionError> {
        buffer::write_message(&mut self.partial_write_buf, &mut self.socket, message).await?;

        Ok(())
    }

    pub async fn write_admin_shutdown_fatal_message(&mut self) -> Result<(), ConnectionError> {
        buffer::write_message(
            &mut bytes::BytesMut::new(),
            &mut self.socket,
            Self::admin_shutdown_error().to_error_response(),
        )
        .await?;

        Ok(())
    }

    pub async fn process_initial_message(&mut self) -> Result<StartupState, ConnectionError> {
        let mut buffer = buffer::read_contents(&mut self.socket, 0).await?;

        let initial_message = protocol::InitialMessage::from(&mut buffer).await?;
        match initial_message {
            InitialMessage::Startup(startup) => self.process_startup_message(startup).await,
            InitialMessage::CancelRequest(cancel) => self.process_cancel(cancel).await,
            InitialMessage::Gssenc | InitialMessage::SslRequest => {
                self.write(protocol::SSLResponse::new()).await?;
                return Ok(StartupState::SslRequested);
            }
        }
    }

    pub async fn process_cancel(
        &mut self,
        cancel_message: protocol::CancelRequest,
    ) -> Result<StartupState, ConnectionError> {
        trace!("Cancel request {:?}", cancel_message);

        if let Some(s) = self
            .session
            .session_manager
            .get_session(cancel_message.process_id)
            .await
        {
            if s.state.secret == cancel_message.secret {
                s.state.cancel_query();
            } else {
                trace!(
                    "Unable to process cancel: wrong secret, {} != {}",
                    s.state.secret,
                    cancel_message.secret
                );
            }
        } else {
            trace!("Unable to process cancel: unknown session");
        }

        Ok(StartupState::CancelRequest)
    }

    pub async fn process_startup_message(
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
            buffer::write_message(
                &mut self.partial_write_buf,
                &mut self.socket,
                error_response,
            )
            .await?;
            return Ok(StartupState::Denied);
        }

        let parameters = startup_message.parameters;
        if !parameters.contains_key("user") {
            let error_response = protocol::ErrorResponse::new(
                protocol::ErrorSeverity::Fatal,
                protocol::ErrorCode::InvalidAuthorizationSpecification,
                "no PostgreSQL user name specified in startup packet".to_string(),
            );
            buffer::write_message(
                &mut self.partial_write_buf,
                &mut self.socket,
                error_response,
            )
            .await?;
            return Ok(StartupState::Denied);
        }

        let auth_method = self.session.server.pg_auth.get_auth_method(&parameters);
        self.write(protocol::Authentication::new(auth_method.clone()))
            .await?;

        Ok(StartupState::Success(parameters, auth_method))
    }

    pub async fn authenticate(
        &mut self,
        auth_request: AuthenticationRequest,
        auth_secret: protocol::FrontendMessage,
        parameters: HashMap<String, String>,
    ) -> Result<bool, ConnectionError> {
        let auth_service = self.session.server.auth.clone();
        let auth_status = self
            .session
            .server
            .pg_auth
            .authenticate(auth_service, auth_request, auth_secret, &parameters)
            .await;
        let result = match auth_status {
            AuthenticationStatus::UnexpectedFrontendMessage => Err((
                "invalid authorization specification".to_string(),
                protocol::ErrorCode::InvalidAuthorizationSpecification,
            )),
            AuthenticationStatus::Failed(err) => Err((err, protocol::ErrorCode::InvalidPassword)),
            AuthenticationStatus::Success(user, auth_context) => Ok((user, auth_context)),
        };

        match result {
            Err((message, code)) => {
                let error_response = protocol::ErrorResponse::fatal(code, message);
                buffer::write_message(
                    &mut self.partial_write_buf,
                    &mut self.socket,
                    error_response,
                )
                .await?;

                Ok(false)
            }
            Ok((user, auth_context)) => {
                let database = parameters
                    .get("database")
                    .cloned()
                    .unwrap_or("db".to_string());
                self.session.state.set_database(Some(database));
                self.session.state.set_user(Some(user));
                self.session.state.set_auth_context(Some(auth_context));

                self.write(protocol::Authentication::new(AuthenticationRequest::Ok))
                    .await?;

                Ok(true)
            }
        }
    }

    pub async fn ready(&mut self) -> Result<(), ConnectionError> {
        let params = vec![
            protocol::ParameterStatus::new(
                "server_version".to_string(),
                "14.2 (Cube SQL)".to_string(),
            ),
            protocol::ParameterStatus::new("server_encoding".to_string(), "UTF8".to_string()),
            protocol::ParameterStatus::new("client_encoding".to_string(), "UTF8".to_string()),
            protocol::ParameterStatus::new("DateStyle".to_string(), "ISO".to_string()),
            // Reports whether PostgreSQL was built with support for 64-bit-integer dates and times.
            protocol::ParameterStatus::new("integer_datetimes".to_string(), "on".to_string()),
            protocol::ParameterStatus::new("TimeZone".to_string(), "Etc/UTC".to_string()),
            protocol::ParameterStatus::new("IntervalStyle".to_string(), "postgres".to_string()),
            // Some drivers rely on it, for example, SQLAlchemy
            // https://github.com/sqlalchemy/sqlalchemy/blob/6104c163eb58e35e46b0bb6a237e824ec1ee1d15/lib/sqlalchemy/dialects/postgresql/base.py#L2994
            protocol::ParameterStatus::new(
                "standard_conforming_strings".to_string(),
                "on".to_string(),
            ),
        ];

        self.write_multi(params).await?;
        self.write(protocol::BackendKeyData::new(
            self.session.state.connection_id,
            self.session.state.secret,
        ))
        .await?;
        self.write(protocol::ReadyForQuery::new(
            protocol::TransactionStatus::Idle,
        ))
        .await?;

        Ok(())
    }

    pub async fn write_ready(&mut self) -> Result<(), ConnectionError> {
        self.write(protocol::ReadyForQuery::new(
            if self.session.state.is_in_transaction() {
                protocol::TransactionStatus::InTransactionBlock
            } else {
                protocol::TransactionStatus::Idle
            },
        ))
        .await
    }

    pub async fn flush(&mut self) -> Result<(), ConnectionError> {
        // TODO: flush network buffers here once buffering has been implemented
        Ok(())
    }

    pub async fn describe_portal(&mut self, name: String) -> Result<(), ConnectionError> {
        if let Some(portal) = self.portals.get(&name) {
            if portal.is_empty() {
                self.write(protocol::NoData::new()).await
            } else {
                match portal.get_description()? {
                    // If Query doesnt return data, no fields in response.
                    None => self.write(protocol::NoData::new()).await,
                    Some(packet) => self.write(packet).await,
                }
            }
        } else {
            self.write(protocol::ErrorResponse::new(
                protocol::ErrorSeverity::Error,
                protocol::ErrorCode::InvalidCursorName,
                "missing cursor".to_string(),
            ))
            .await
        }
    }

    pub async fn describe_statement(&mut self, name: String) -> Result<(), ConnectionError> {
        let session = self.session.clone();
        let statements_guard = session.state.statements.read().await;
        match statements_guard.get(&name) {
            None => {
                self.write(protocol::ErrorResponse::new(
                    protocol::ErrorSeverity::Error,
                    protocol::ErrorCode::InvalidSqlStatement,
                    "missing statement".to_string(),
                ))
                .await?;

                return Ok(());
            }
            Some(statement) => match statement {
                PreparedStatement::Empty { .. } => {
                    self.write(protocol::ParameterDescription::new(vec![]))
                        .await?;
                    self.write(protocol::NoData::new()).await
                }
                PreparedStatement::Query {
                    description,
                    parameters,
                    ..
                } => match description {
                    // If Query doesnt return data, no fields in response.
                    None => {
                        self.write(parameters.clone()).await?;
                        self.write(protocol::NoData::new()).await
                    }
                    Some(packet) => {
                        self.write(parameters.clone()).await?;
                        self.write(packet.clone()).await
                    }
                },
                PreparedStatement::Error { .. } => Err(CubeError::internal(
                    "Describe called on errored prepared statement (it's a bug)".to_string(),
                )
                .into()),
            },
        }
    }

    pub async fn describe(&mut self, body: protocol::Describe) -> Result<(), ConnectionError> {
        match body.typ {
            protocol::DescribeType::Statement => self.describe_statement(body.name).await,
            protocol::DescribeType::Portal => self.describe_portal(body.name).await,
        }
    }

    pub async fn close(&mut self, body: protocol::Close) -> Result<(), ConnectionError> {
        match body.typ {
            protocol::CloseType::Statement => {
                self.session
                    .state
                    .statements
                    .write()
                    .await
                    .remove(&body.name);
            }
            protocol::CloseType::Portal => {
                self.portals.remove(&body.name);
            }
        };

        self.write(protocol::CloseComplete::new()).await?;

        Ok(())
    }

    /// https://github.com/postgres/postgres/blob/REL_14_4/src/backend/commands/portalcmds.c#L167
    pub async fn execute(&mut self, execute: protocol::Execute) -> Result<(), ConnectionError> {
        if let Some(portal) = self.portals.get_mut(&execute.portal) {
            if portal.is_empty() {
                self.write(protocol::EmptyQueryResponse::new()).await?;
            } else {
                let cancel = self
                    .session
                    .state
                    .begin_query(format!("portal #{}", execute.portal));

                let mut portal = Pin::new(portal);
                let stream = portal.execute(execute.max_rows as usize);
                let mut stream = pin!(stream);

                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => {
                            self.session.state.end_query();

                            return Err(protocol::ErrorResponse::query_canceled().into());
                        },
                        chunk = stream.next() => {
                            let chunk = match chunk {
                                Some(chunk) => match chunk {
                                    Ok(chunk) => chunk,
                                    Err(_) => {
                                        self.session.state.end_query();
                                        chunk?
                                    }
                                },
                                None => return Ok(()),
                            };

                            if cancel.is_cancelled() {
                                self.session.state.end_query();

                                return Err(protocol::ErrorResponse::query_canceled().into());
                            }

                            match chunk {
                                PortalBatch::Rows(writer) if writer.has_data() => buffer::write_direct(&mut self.partial_write_buf, &mut self.socket, writer).await?,
                                PortalBatch::Completion(completion) => {
                                    self.session.state.end_query();

                                    // TODO:
                                    match completion {
                                        PortalCompletion::Complete(c) => buffer::write_message(&mut self.partial_write_buf, &mut self.socket, c).await?,
                                        PortalCompletion::Suspended(s) => buffer::write_message(&mut self.partial_write_buf, &mut self.socket, s).await?,
                                    }

                                    return Ok(());
                                },
                                _ => (),
                            }
                        },
                    }
                }
            };

            Ok(())
        } else {
            Err(ErrorResponse::error(
                ErrorCode::InvalidCursorName,
                format!(r#"Unknown portal: {}"#, execute.portal),
            )
            .into())
        }
    }

    pub async fn bind(
        &mut self,
        body: protocol::Bind,
        span_id: Option<Arc<SpanId>>,
    ) -> Result<(), ConnectionError> {
        if self.portals.len() >= self.session.server.configuration.connection_max_portals {
            return Err(ConnectionError::Protocol(
                protocol::ErrorResponse::error(
                    protocol::ErrorCode::ConfigurationLimitExceeded,
                    format!(
                        "Unable to allocate a new portal: max allocation reached, actual: {}, max: {}",
                        self.portals.len(),
                        self.session.server.configuration.connection_max_portals),
                )
                    .into(),
                span_id.clone(),
            ));
        }

        let statements_guard = self.session.state.statements.read().await;
        let source_statement = statements_guard.get(&body.statement).ok_or_else(|| {
            ErrorResponse::error(
                ErrorCode::InvalidSqlStatement,
                format!(r#"Unknown statement: {}"#, body.statement),
            )
        })?;

        let format = body.result_formats.first().copied().unwrap_or(Format::Text);
        let portal = match source_statement {
            PreparedStatement::Empty { .. } => {
                drop(statements_guard);

                Portal::new_empty(format, PortalFrom::Extended, span_id)
            }
            PreparedStatement::Query { parameters, .. } => {
                let prepared_statement =
                    source_statement.bind(body.to_bind_values(&parameters)?)?;
                drop(statements_guard);

                let cache_entry = self.get_cache_entry().await?;
                let meta = self.session.server.compiler_cache.meta(cache_entry).await?;

                let plan = convert_statement_to_cube_query(
                    prepared_statement,
                    meta,
                    self.session.clone(),
                    &mut None,
                    span_id.clone(),
                )
                .await?;

                Portal::new(plan, format, PortalFrom::Extended, span_id)
            }
            PreparedStatement::Error { .. } => {
                drop(statements_guard);

                Portal::new_empty(format, PortalFrom::Extended, span_id)
            }
        };

        self.portals.insert(body.portal, portal);
        self.write(protocol::BindComplete::new()).await?;

        Ok(())
    }

    pub async fn parse(
        &mut self,
        parse: protocol::Parse,
        qtrace: &mut Option<Qtrace>,
        span_id: Option<Arc<SpanId>>,
    ) -> Result<(), ConnectionError> {
        if parse.query.trim() == "" {
            let mut statements_guard = self.session.state.statements.write().await;
            statements_guard.insert(
                parse.name,
                PreparedStatement::Empty {
                    from_sql: false,
                    created: chrono::offset::Utc::now(),
                    span_id: span_id.clone(),
                },
            );
        } else {
            match parse_sql_to_statement(&parse.query, DatabaseProtocol::PostgreSQL, qtrace) {
                Ok(query) => {
                    if let Some(qtrace) = qtrace {
                        qtrace.push_statement(&query);
                    }
                    self.prepare_statement(
                        parse.name,
                        Ok(query),
                        &parse.param_types,
                        false,
                        qtrace,
                        span_id.clone(),
                    )
                    .await?;
                }
                Err(err) => {
                    self.prepare_statement(
                        parse.name,
                        Err(parse.query.to_string()),
                        &parse.param_types,
                        false,
                        qtrace,
                        span_id.clone(),
                    )
                    .await?;
                    Err(err)?;
                }
            }
        }

        self.write(protocol::ParseComplete::new()).await?;

        Ok(())
    }

    pub async fn prepare_statement(
        &mut self,
        name: String,
        query: Result<Statement, String>,
        param_types: &[u32],
        from_sql: bool,
        qtrace: &mut Option<Qtrace>,
        span_id: Option<Arc<SpanId>>,
    ) -> Result<(), ConnectionError> {
        let prepared_statements_count = self.session.state.statements.read().await.len();
        if prepared_statements_count
            >= self
                .session
                .server
                .configuration
                .connection_max_prepared_statements
        {
            return Err(ConnectionError::Protocol(
                protocol::ErrorResponse::error(
                    protocol::ErrorCode::ConfigurationLimitExceeded,
                    format!(
                        "Unable to allocate a new prepared statement: max allocation reached, actual: {}, max: {}",
                        prepared_statements_count,
                        self.session.server.configuration.connection_max_prepared_statements),
                )
                    .into(),
                span_id.clone(),
            ));
        }

        let (pstmt, result) = match query {
            Ok(query) => {
                let stmt_finder = PostgresStatementParamsFinder::new(param_types);
                let parameters: Vec<PgTypeId> = stmt_finder
                    .find(&query)?
                    .into_iter()
                    .map(|param| param.coltype.to_pg_tid())
                    .collect();

                let cache_entry = self.get_cache_entry().await?;
                let meta = self.session.server.compiler_cache.meta(cache_entry).await?;

                let stmt_replacer = StatementPlaceholderReplacer::new();
                let hacked_query = stmt_replacer.replace(query.clone())?;

                let plan = convert_statement_to_cube_query(
                    hacked_query,
                    meta,
                    self.session.clone(),
                    qtrace,
                    span_id.clone(),
                )
                .await;

                match plan {
                    Ok(plan) => {
                        let description =
                            plan.to_row_description(Format::Text)?
                                .and_then(|description| {
                                    if description.len() > 0 {
                                        Some(description)
                                    } else {
                                        None
                                    }
                                });

                        (
                            PreparedStatement::Query {
                                from_sql,
                                created: chrono::offset::Utc::now(),
                                query,
                                parameters: protocol::ParameterDescription::new(parameters),
                                description,
                                span_id,
                            },
                            Ok(()),
                        )
                    }
                    Err(err) => (
                        PreparedStatement::Error {
                            from_sql,
                            sql: query.to_string(),
                            created: chrono::offset::Utc::now(),
                            span_id,
                        },
                        Err(err.into()),
                    ),
                }
            }
            Err(sql) => (
                PreparedStatement::Error {
                    from_sql,
                    sql,
                    created: chrono::offset::Utc::now(),
                    span_id,
                },
                Ok(()),
            ),
        };
        self.session
            .state
            .statements
            .write()
            .await
            .insert(name, pstmt);

        result
    }

    pub fn end_transaction(&mut self) -> Result<bool, ConnectionError> {
        if let Some(_) = self.session.state.end_transaction() {
            // Portals + Cursors which we want to remove
            let mut to_remove = Vec::new();

            for (key, cursor) in &self.cursors {
                if !cursor.hold {
                    to_remove.push(key.clone());
                }
            }

            for key in &to_remove {
                self.cursors.remove(key);
                self.portals.remove(key);

                trace!("Closing cursor/portal {}", key);
            }

            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn handle_simple_query(
        &mut self,
        stmt: ast::Statement,
        meta: Arc<MetaContext>,
        qtrace: &mut Option<Qtrace>,
        span_id: Option<Arc<SpanId>>,
    ) -> Result<(), ConnectionError> {
        let cancel = self.session.state.begin_query(stmt.to_string());

        tokio::select! {
            _ = cancel.cancelled() => {
                self.session.state.end_query();

                // We don't return error, because query can contains multiple statements
                // then cancel request will cancel only one query
                self.write(protocol::ErrorResponse::query_canceled()).await?;
                if let Some(qtrace) = qtrace {
                    qtrace.set_statement_error_message("Execution cancelled by user");
                }

                Ok(())
            },
            res = self.process_simple_query(stmt, meta, cancel.clone(), qtrace, span_id) => {
                self.session.state.end_query();

                if cancel.is_cancelled() {
                    self.write(protocol::ErrorResponse::query_canceled()).await?;
                    if let Some(qtrace) = qtrace {
                        qtrace.set_statement_error_message("Execution cancelled by user");
                    }
                }

                res
            },
        }
    }

    pub async fn process_simple_query(
        &mut self,
        stmt: ast::Statement,
        meta: Arc<MetaContext>,
        cancel: CancellationToken,
        qtrace: &mut Option<Qtrace>,
        span_id: Option<Arc<SpanId>>,
    ) -> Result<(), ConnectionError> {
        match stmt {
            Statement::StartTransaction { .. } => {
                if !self.session.state.begin_transaction() {
                    self.write(protocol::NoticeResponse::warning(
                        ErrorCode::ActiveSqlTransaction,
                        "there is already a transaction in progress".to_string(),
                    ))
                    .await?
                };

                let plan = QueryPlan::MetaOk(StatusFlags::empty(), CommandCompletion::Begin);

                self.write_portal(
                    &mut Portal::new(plan, Format::Text, PortalFrom::Simple, span_id.clone()),
                    0,
                    cancel,
                )
                .await?;
            }
            Statement::Rollback { .. } => {
                if !self.end_transaction()? {
                    // PostgreSQL returns command completion anyway
                    self.write(protocol::NoticeResponse::warning(
                        ErrorCode::NoActiveSqlTransaction,
                        "there is no transaction in progress".to_string(),
                    ))
                    .await?
                };

                let plan = QueryPlan::MetaOk(StatusFlags::empty(), CommandCompletion::Rollback);

                self.write_portal(
                    &mut Portal::new(plan, Format::Text, PortalFrom::Simple, span_id.clone()),
                    0,
                    CancellationToken::new(),
                )
                .await?;
            }
            Statement::Commit { .. } => {
                if !self.end_transaction()? {
                    // PostgreSQL returns command completion anyway
                    self.write(protocol::NoticeResponse::warning(
                        ErrorCode::NoActiveSqlTransaction,
                        "there is no transaction in progress".to_string(),
                    ))
                    .await?
                };

                let plan = QueryPlan::MetaOk(StatusFlags::empty(), CommandCompletion::Commit);

                self.write_portal(
                    &mut Portal::new(plan, Format::Text, PortalFrom::Simple, span_id.clone()),
                    0,
                    CancellationToken::new(),
                )
                .await?;
            }
            Statement::Fetch {
                name,
                direction,
                into,
            } => {
                if into.is_some() {
                    return Err(ConnectionError::Protocol(
                        protocol::ErrorResponse::error(
                            protocol::ErrorCode::FeatureNotSupported,
                            "INTO is not supported for FETCH statement".to_string(),
                        )
                        .into(),
                        span_id.clone(),
                    ));
                };

                let limit: usize = match direction {
                    FetchDirection::Count { limit } => {
                        match limit {
                            Value::Number(v, negative) => {
                                if negative {
                                    // HINT:  Declare it with SCROLL option to enable backward scan.
                                    // But it's not supported right now!
                                    return Err(ConnectionError::Protocol(
                                        protocol::ErrorResponse::error(
                                            protocol::ErrorCode::ObjectNotInPrerequisiteState,
                                            "cursor can only scan forward".to_string(),
                                        )
                                        .into(),
                                        span_id.clone(),
                                    ));
                                }

                                v.parse::<usize>().map_err(|err| ConnectionError::Protocol(
                                protocol::ErrorResponse::error(
                                    protocol::ErrorCode::ProtocolViolation,
                                    format!(r#""Unable to parse number "{}" for fetch limit: {}"#, v, err),
                                )
                                    .into(),
                                span_id.clone(),
                            ))?
                            }
                            _ => unreachable!(),
                        }
                    }
                    other => {
                        return Err(ConnectionError::Protocol(
                            protocol::ErrorResponse::error(
                                protocol::ErrorCode::ProtocolViolation,
                                format!("Limit {} is not supported for FETCH statement", other),
                            )
                            .into(),
                            span_id.clone(),
                        ));
                    }
                };

                if let Some(mut portal) = self.portals.remove(&name.value) {
                    self.write_portal(&mut portal, limit, CancellationToken::new())
                        .await?;
                    self.portals.insert(name.value.clone(), portal);

                    return Ok(());
                } else {
                    trace!(
                        r#"Unable to find portal for cursor: "{}". Maybe it was not created. Opening..."#,
                        &name.value
                    );
                }

                if self.portals.len() >= self.session.server.configuration.connection_max_portals {
                    return Err(ConnectionError::Protocol(
                        protocol::ErrorResponse::error(
                            protocol::ErrorCode::ConfigurationLimitExceeded,
                            format!(
                                "Unable to allocate a new portal to open cursor: max allocation reached, actual: {}, max: {}",
                                self.portals.len(),
                                self.session.server.configuration.connection_max_portals),
                        )
                            .into(),
                        span_id.clone(),
                    ));
                }

                let cursor = self.cursors.get(&name.value).ok_or_else(|| {
                    ConnectionError::Protocol(
                        protocol::ErrorResponse::error(
                            protocol::ErrorCode::ProtocolViolation,
                            format!(r#"cursor "{}" does not exist"#, name.value),
                        )
                        .into(),
                        span_id.clone(),
                    )
                })?;

                let plan = convert_statement_to_cube_query(
                    cursor.query.clone(),
                    meta,
                    self.session.clone(),
                    qtrace,
                    span_id.clone(),
                )
                .await?;

                let mut portal =
                    Portal::new(plan, cursor.format, PortalFrom::Fetch, span_id.clone());

                self.write_portal(&mut portal, limit, cancel).await?;
                self.portals.insert(name.value, portal);
            }
            Statement::Declare {
                name,
                binary,
                query,
                scroll,
                sensitive,
                hold,
            } => {
                // The default is to allow scrolling in some cases; this is not the same as specifying SCROLL.
                if scroll.is_some() {
                    return Err(ConnectionError::Protocol(
                        protocol::ErrorResponse::error(
                            protocol::ErrorCode::FeatureNotSupported,
                            "SCROLL|NO SCROLL is not supported for DECLARE statement".to_string(),
                        )
                        .into(),
                        span_id.clone(),
                    ));
                };

                // In PostgreSQL, all cursors are insensitive
                if Some(true) == sensitive {
                    return Err(ConnectionError::Protocol(
                        protocol::ErrorResponse::error(
                            protocol::ErrorCode::FeatureNotSupported,
                            "INSENSITIVE|ASENSITIVE is not supported for DECLARE statement"
                                .to_string(),
                        )
                        .into(),
                        span_id.clone(),
                    ));
                };

                if self
                    .session
                    .state
                    .statements
                    .read()
                    .await
                    .contains_key(&name.value)
                {
                    return Err(ConnectionError::Protocol(
                        protocol::ErrorResponse::error(
                            protocol::ErrorCode::DuplicateCursor,
                            format!(r#"cursor "{}" already exists"#, name.value),
                        )
                        .into(),
                        span_id.clone(),
                    ));
                }

                let select_stmt = Statement::Query(query);
                // It's just a verification that we can compile that query.
                let _ = convert_statement_to_cube_query(
                    select_stmt.clone(),
                    meta.clone(),
                    self.session.clone(),
                    &mut None,
                    span_id.clone(),
                )
                .await?;

                let cursor = Cursor {
                    query: select_stmt,
                    hold: hold.unwrap_or(false),
                    format: if binary { Format::Binary } else { Format::Text },
                };

                if self.cursors.len() >= self.session.server.configuration.connection_max_cursors {
                    return Err(ConnectionError::Protocol(
                        protocol::ErrorResponse::error(
                            protocol::ErrorCode::ConfigurationLimitExceeded,
                            format!(
                                "Unable to allocate a new cursor: max allocation reached, actual: {}, max: {}",
                                self.cursors.len(),
                                self.session.server.configuration.connection_max_cursors),
                        )
                            .into(),
                        span_id.clone(),
                    ));
                }

                self.cursors.insert(name.value, cursor);

                let plan =
                    QueryPlan::MetaOk(StatusFlags::empty(), CommandCompletion::DeclareCursor);

                self.write_portal(
                    &mut Portal::new(plan, Format::Text, PortalFrom::Simple, span_id.clone()),
                    0,
                    cancel,
                )
                .await?;
            }
            Statement::Discard { object_type } => {
                self.session.state.clear_extended().await;
                self.portals = HashMap::new();
                self.cursors = HashMap::new();

                let plan = QueryPlan::MetaOk(
                    StatusFlags::empty(),
                    CommandCompletion::Discard(object_type.to_string()),
                );

                self.write_portal(
                    &mut Portal::new(plan, Format::Text, PortalFrom::Simple, span_id.clone()),
                    0,
                    cancel,
                )
                .await?;
            }
            Statement::Deallocate { name, .. } => {
                let plan = if name.value.eq_ignore_ascii_case(&"all") {
                    self.session.state.clear_prepared_statements().await;

                    Ok(QueryPlan::MetaOk(
                        StatusFlags::empty(),
                        CommandCompletion::DeallocateAll,
                    ))
                } else {
                    let mut statements_guard = self.session.state.statements.write().await;
                    if statements_guard.remove(&name.value).is_some() {
                        Ok(QueryPlan::MetaOk(
                            StatusFlags::empty(),
                            CommandCompletion::Deallocate,
                        ))
                    } else {
                        Err(ConnectionError::Protocol(
                            protocol::ErrorResponse::error(
                                protocol::ErrorCode::ProtocolViolation,
                                format!(r#"prepared statement "{}" does not exist"#, name.value),
                            )
                            .into(),
                            span_id.clone(),
                        ))
                    }
                }?;

                self.write_portal(
                    &mut Portal::new(plan, Format::Text, PortalFrom::Simple, span_id.clone()),
                    0,
                    cancel,
                )
                .await?;
            }
            Statement::Close { cursor } => {
                let plan = match cursor {
                    CloseCursor::All => {
                        for key in self.cursors.keys() {
                            self.portals.remove(key);
                        }
                        self.cursors.clear();

                        Ok(QueryPlan::MetaOk(
                            StatusFlags::empty(),
                            CommandCompletion::CloseCursorAll,
                        ))
                    }
                    CloseCursor::Specific { name } => {
                        if self.cursors.remove(&name.value).is_some() {
                            self.portals.remove(&name.value);

                            Ok(QueryPlan::MetaOk(
                                StatusFlags::empty(),
                                CommandCompletion::CloseCursor,
                            ))
                        } else {
                            Err(ConnectionError::Protocol(
                                protocol::ErrorResponse::error(
                                    protocol::ErrorCode::ProtocolViolation,
                                    format!(r#"cursor "{}" does not exist"#, name.value),
                                )
                                .into(),
                                span_id.clone(),
                            ))
                        }
                    }
                }?;

                self.write_portal(
                    &mut Portal::new(plan, Format::Text, PortalFrom::Simple, span_id.clone()),
                    0,
                    cancel,
                )
                .await?;
            }
            Statement::Prepare {
                name, statement, ..
            } => {
                // Ensure the statement isn't wrapped in extra parens
                let statement = match *statement.clone() {
                    Statement::Query(outer_query) => match *outer_query {
                        Query {
                            with: None,
                            body: SetExpr::Query(inner_query),
                            order_by,
                            limit: None,
                            offset: None,
                            fetch: None,
                            lock: None,
                        } if order_by.is_empty() => Statement::Query(inner_query),
                        _ => *statement,
                    },
                    _ => *statement,
                };

                self.prepare_statement(
                    name.value,
                    Ok(statement),
                    &[],
                    true,
                    qtrace,
                    span_id.clone(),
                )
                .await?;

                let plan = QueryPlan::MetaOk(StatusFlags::empty(), CommandCompletion::Prepare);

                self.write_portal(
                    &mut Portal::new(plan, Format::Text, PortalFrom::Simple, span_id.clone()),
                    0,
                    cancel,
                )
                .await?;
            }
            other => {
                let plan = convert_statement_to_cube_query(
                    other,
                    meta.clone(),
                    self.session.clone(),
                    qtrace,
                    span_id.clone(),
                )
                .await?;

                self.write_portal(
                    &mut Portal::new(plan, Format::Text, PortalFrom::Simple, span_id.clone()),
                    0,
                    cancel,
                )
                .await?;
            }
        };

        Ok(())
    }

    pub async fn write_portal(
        &mut self,
        portal: &mut Portal,
        max_rows: usize,
        cancel: CancellationToken,
    ) -> Result<(), ConnectionError> {
        let mut portal = Pin::new(portal);
        let stream = portal.execute(max_rows);
        let mut stream = pin!(stream);

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    // TODO: Cancellation handling via errors?
                    return Ok(());
                },
                chunk = stream.next() => {
                    let chunk = match chunk {
                        Some(chunk) => chunk?,
                        None => return Ok(()),
                    };

                    match chunk {
                        PortalBatch::Description(description) => match description.len() {
                            // Special handling for special queries, such as DISCARD ALL.
                            0 => self.write(protocol::NoData::new()).await?,
                            _ => self.write(description).await?,
                        },
                        PortalBatch::Rows(writer) => {
                            if writer.has_data() {
                                buffer::write_direct(&mut self.partial_write_buf, &mut self.socket, writer).await?
                            }
                        }
                        PortalBatch::Completion(completion) => return self.write_completion(completion).await,
                    }
                }
            }
        }
    }

    /// Pipeline of Execution
    /// process_query -> (&str)
    ///     execute_query -> (&str)
    ///         handle_simple_query
    ///             process_simple_query -> (portal)
    ///                 write_portal
    pub async fn execute_query(
        &mut self,
        query: &str,
        qtrace: &mut Option<Qtrace>,
        span_id: Option<Arc<SpanId>>,
    ) -> Result<(), ConnectionError> {
        let cache_entry = self.get_cache_entry().await?;
        let meta = self.session.server.compiler_cache.meta(cache_entry).await?;

        let statements = parse_sql_to_statements(query, DatabaseProtocol::PostgreSQL, qtrace)?;

        if statements.len() == 0 {
            self.write(protocol::EmptyQuery::new()).await?;
        } else {
            for statement in statements {
                if let Some(qtrace) = qtrace {
                    qtrace.push_statement(&statement);
                }
                match std::panic::AssertUnwindSafe(self.handle_simple_query(
                    statement,
                    meta.clone(),
                    qtrace,
                    span_id.clone(),
                ))
                .catch_unwind()
                .await
                {
                    Ok(res) => {
                        if let Some(qtrace) = qtrace {
                            if let Err(err) = &res {
                                qtrace.set_statement_error_message(&err.to_string());
                            }
                        }
                        res?
                    }
                    Err(err) => {
                        let err: ConnectionError = CubeError::panic(err).into();
                        if let Some(qtrace) = qtrace {
                            qtrace.set_statement_error_message(&err.to_string());
                        }
                        return Err(err);
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn process_query(
        &mut self,
        query: String,
        qtrace: &mut Option<Qtrace>,
        span_id: Option<Arc<SpanId>>,
    ) -> Result<(), ConnectionError> {
        let start_time = SystemTime::now();
        if let Some(auth_context) = self.session.state.auth_context() {
            self.session
                .session_manager
                .server
                .transport
                .log_load_state(
                    span_id.clone(),
                    auth_context,
                    self.session.state.get_load_request_meta("sql"),
                    "Load Request".to_string(),
                    serde_json::json!({
                        "query": {
                            "sql": query.clone(),
                        }
                    }),
                )
                .await?;
        }
        debug!("Query: {}", query);

        if let Err(err) = self.execute_query(&query, qtrace, span_id.clone()).await {
            if let Some(qtrace) = qtrace {
                qtrace.set_query_error_message(&err.to_string())
            }
            let err = err.with_span_id(span_id.clone());
            self.handle_connection_error(err).await?;
        } else {
            if let Some(auth_context) = self.session.state.auth_context() {
                if let Some(span_id) = span_id {
                    self.session
                        .session_manager
                        .server
                        .transport
                        .log_load_state(
                            Some(span_id.clone()),
                            auth_context,
                            self.session.state.get_load_request_meta("sql"),
                            "Load Request Success".to_string(),
                            serde_json::json!({
                                "query": {
                                    "sql": query,
                                },
                                "apiType": "sql",
                                "duration": start_time.elapsed().unwrap().as_millis() as u64,
                                "isDataQuery": span_id.is_data_query().await,
                            }),
                        )
                        .await?;
                }
            }
        };

        self.write_ready().await
    }

    pub(crate) fn auth_context(&self) -> Result<AuthContextRef, CubeError> {
        self.session
            .state
            .auth_context()
            .ok_or(CubeError::internal("must be auth".to_string()))
    }
}
