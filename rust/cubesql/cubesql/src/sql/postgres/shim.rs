use std::{backtrace::Backtrace, collections::HashMap, io::ErrorKind, sync::Arc};

use super::extended::PreparedStatement;
use crate::{
    compile::{
        convert_statement_to_cube_query,
        parser::{parse_sql_to_statement, parse_sql_to_statements},
        CompilationError, MetaContext, QueryPlan,
    },
    sql::{
        df_type_to_pg_tid,
        extended::{Cursor, Portal, PortalFrom},
        session::DatabaseProtocol,
        statement::{
            PostgresStatementParamsFinder, SensitiveDataSanitizer, StatementPlaceholderReplacer,
        },
        types::CommandCompletion,
        writer::BatchWriter,
        AuthContextRef, Session, StatusFlags,
    },
    telemetry::ContextLogger,
    CubeError,
};
use log::{debug, error, trace};
use pg_srv::{
    buffer, protocol,
    protocol::{ErrorCode, ErrorResponse, Format, InitialMessage, PortalCompletion},
    PgType, PgTypeId, ProtocolError,
};
use sqlparser::ast::{self, CloseCursor, FetchDirection, Query, SetExpr, Statement, Value};
use tokio::{io::AsyncWriteExt, net::TcpStream};
use tokio_util::sync::CancellationToken;

pub struct AsyncPostgresShim {
    socket: TcpStream,
    // Extended query
    statements: HashMap<String, Option<PreparedStatement>>,
    cursors: HashMap<String, Cursor>,
    portals: HashMap<String, Option<Portal>>,
    // Shared
    session: Arc<Session>,
    logger: Arc<dyn ContextLogger>,
}

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

impl AsyncPostgresShim {
    pub async fn run_on(
        socket: TcpStream,
        session: Arc<Session>,
        logger: Arc<dyn ContextLogger>,
    ) -> Result<(), std::io::Error> {
        let mut shim = Self {
            socket,
            cursors: HashMap::new(),
            portals: HashMap::new(),
            statements: HashMap::new(),
            session,
            logger,
        };

        match shim.run().await {
            Err(e) => {
                if let ConnectionError::Protocol(ProtocolError::IO { source, .. }) = &e {
                    if source.kind() == ErrorKind::BrokenPipe
                        || source.kind() == ErrorKind::UnexpectedEof
                    {
                        trace!("Error during processing PostgreSQL connection: {}", e);

                        return Ok(());
                    }
                }

                shim.logger.error(
                    format!("Error during processing PostgreSQL connection: {}", e).as_str(),
                    None,
                );

                if let Some(bt) = e.backtrace() {
                    trace!("{}", bt);
                } else {
                    trace!("Backtrace: not found");
                }

                Ok(())
            }
            _ => {
                shim.socket.shutdown().await?;
                return Ok(());
            }
        }
    }

    pub async fn run(&mut self) -> Result<(), ConnectionError> {
        let initial_parameters = match self.process_initial_message().await? {
            StartupState::Success(parameters) => parameters,
            StartupState::SslRequested => match self.process_initial_message().await? {
                StartupState::Success(parameters) => parameters,
                _ => return Ok(()),
            },
            StartupState::Denied | StartupState::CancelRequest => return Ok(()),
        };

        match buffer::read_message(&mut self.socket).await? {
            protocol::FrontendMessage::PasswordMessage(password_message) => {
                if !self
                    .authenticate(password_message, initial_parameters)
                    .await?
                {
                    return Ok(());
                }
            }
            _ => return Ok(()),
        }

        self.ready().await?;

        // When an error is detected while processing any extended-query message, the backend issues ErrorResponse,
        // then reads and discards messages until a Sync is reached, then issues ReadyForQuery and returns to normal message processing.
        let mut tracked_error: Option<ConnectionError> = None;
        let mut tracked_error_query: Option<String> = None;

        loop {
            let mut doing_extended_query_message = false;

            let (result, query) = match buffer::read_message(&mut self.socket).await? {
                protocol::FrontendMessage::Query(body) => {
                    let query = body.query.clone();
                    (self.process_query(body.query).await, Some(query))
                }
                protocol::FrontendMessage::Flush => (self.flush().await, None),
                protocol::FrontendMessage::Terminate => return Ok(()),
                // Extended
                protocol::FrontendMessage::Parse(body) => {
                    if tracked_error.is_none() {
                        doing_extended_query_message = true;
                        let query = body.query.clone();
                        (self.parse(body).await, Some(query))
                    } else {
                        continue;
                    }
                }
                protocol::FrontendMessage::Bind(body) => {
                    if tracked_error.is_none() {
                        doing_extended_query_message = true;
                        (self.bind(body).await, None)
                    } else {
                        continue;
                    }
                }
                protocol::FrontendMessage::Execute(body) => {
                    if tracked_error.is_none() {
                        doing_extended_query_message = true;
                        (self.execute(body).await, None)
                    } else {
                        continue;
                    }
                }
                protocol::FrontendMessage::Close(body) => {
                    if tracked_error.is_none() {
                        (self.close(body).await, None)
                    } else {
                        continue;
                    }
                }
                protocol::FrontendMessage::Describe(body) => {
                    if tracked_error.is_none() {
                        (self.describe(body).await, None)
                    } else {
                        continue;
                    }
                }
                protocol::FrontendMessage::Sync => {
                    if let Some(err) = tracked_error.take() {
                        self.handle_connection_error(err, tracked_error_query.take())
                            .await?;
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
                    ))
                }
            };
            if let Err(err) = result {
                if doing_extended_query_message {
                    tracked_error = Some(err);
                    tracked_error_query = query;
                } else {
                    self.handle_connection_error(err, query).await?;
                }
            }
        }
    }

    pub async fn handle_connection_error(
        &mut self,
        err: ConnectionError,
        query: Option<String>,
    ) -> Result<(), ConnectionError> {
        let (message, props) = match &err {
            ConnectionError::CompilationError(err) => match err {
                CompilationError::Unsupported(msg, meta)
                | CompilationError::User(msg, meta)
                | CompilationError::Internal(msg, _, meta) => (msg.clone(), meta.clone()),
            },
            ConnectionError::Protocol(ProtocolError::IO { source, .. }) => match source.kind() {
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

        let mut props = props.unwrap_or_default();
        match query {
            Some(query) => {
                if let Ok(statement) = parse_sql_to_statement(&query, DatabaseProtocol::PostgreSQL)
                {
                    props.insert(
                        "sanitizedQuery".to_string(),
                        SensitiveDataSanitizer::new()
                            .replace(&statement)
                            .to_string(),
                    );
                }
                props.insert("query".to_string(), query);
            }
            _ => (),
        }

        self.logger.error(message.as_str(), Some(props));

        if let Some(bt) = err.backtrace() {
            trace!("{}", bt);
        } else {
            trace!("Backtrace: not found");
        }

        self.write(err.to_error_response()).await?;

        Ok(())
    }

    pub async fn write_multi<Message: protocol::Serialize>(
        &mut self,
        message: Vec<Message>,
    ) -> Result<(), ConnectionError> {
        buffer::write_messages(&mut self.socket, message).await?;

        Ok(())
    }

    pub async fn write_completion(
        &mut self,
        completion: PortalCompletion,
    ) -> Result<(), ConnectionError> {
        match completion {
            PortalCompletion::Complete(c) => buffer::write_message(&mut self.socket, c).await?,
            PortalCompletion::Suspended(s) => buffer::write_message(&mut self.socket, s).await?,
        }

        Ok(())
    }

    pub async fn write<Message: protocol::Serialize>(
        &mut self,
        message: Message,
    ) -> Result<(), ConnectionError> {
        buffer::write_message(&mut self.socket, message).await?;

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

    pub async fn authenticate(
        &mut self,
        password_message: protocol::PasswordMessage,
        parameters: HashMap<String, String>,
    ) -> Result<bool, ConnectionError> {
        let user = parameters.get("user").unwrap().clone();
        let authenticate_response = self
            .session
            .server
            .auth
            .authenticate(Some(user.clone()))
            .await;

        let mut auth_context: Option<AuthContextRef> = None;

        let auth_success = match authenticate_response {
            Ok(authenticate_response) => {
                auth_context = Some(authenticate_response.context);
                match authenticate_response.password {
                    None => true,
                    Some(password) => password == password_message.password,
                }
            }
            _ => false,
        };

        if !auth_success {
            let error_response = protocol::ErrorResponse::fatal(
                protocol::ErrorCode::InvalidPassword,
                format!("password authentication failed for user \"{}\"", &user),
            );
            buffer::write_message(&mut self.socket, error_response).await?;

            return Ok(false);
        }

        self.session.state.set_user(Some(user));
        self.session.state.set_auth_context(auth_context);

        self.write(protocol::Authentication::new(
            protocol::AuthenticationRequest::Ok,
        ))
        .await?;

        Ok(true)
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
        match self.portals.get(&name) {
            None => {
                self.write(protocol::ErrorResponse::new(
                    protocol::ErrorSeverity::Error,
                    protocol::ErrorCode::InvalidCursorName,
                    "missing cursor".to_string(),
                ))
                .await?;

                return Ok(());
            }
            Some(portal) => match portal {
                // We use None for Portal on empty query
                None => self.write(protocol::NoData::new()).await,
                Some(named) => match named.get_description()? {
                    // If Query doesnt return data, no fields in response.
                    None => self.write(protocol::NoData::new()).await,
                    Some(packet) => self.write(packet).await,
                },
            },
        }
    }

    pub async fn describe_statement(&mut self, name: String) -> Result<(), ConnectionError> {
        match self.statements.get(&name) {
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
                // We use None for Statement on empty query
                None => {
                    self.write(protocol::ParameterDescription::new(vec![]))
                        .await?;
                    self.write(protocol::NoData::new()).await
                }
                Some(named) => {
                    match named.description.clone() {
                        // If Query doesnt return data, no fields in response.
                        None => {
                            #[allow(mutable_borrow_reservation_conflict)]
                            self.write(named.parameters.clone()).await?;
                            self.write(protocol::NoData::new()).await
                        }
                        Some(packet) => {
                            #[allow(mutable_borrow_reservation_conflict)]
                            self.write(named.parameters.clone()).await?;
                            self.write(packet).await
                        }
                    }
                }
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
                self.statements.remove(&body.name);
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
            match portal {
                // We use None for Statement on empty query
                None => {
                    self.write(protocol::EmptyQueryResponse::new()).await?;
                }
                Some(portal) => {
                    let cancel = self
                        .session
                        .state
                        .begin_query(format!("portal #{}", execute.portal));
                    let mut writer = BatchWriter::new(portal.get_format());

                    tokio::select! {
                        _ = cancel.cancelled() => {
                            self.session.state.end_query();

                            return Err(protocol::ErrorResponse::query_canceled().into());
                        },
                        res = portal.execute(&mut writer, execute.max_rows as usize) => {
                            self.session.state.end_query();

                            // Unwrap result after ending query
                            let completion = res?;

                            if cancel.is_cancelled() {
                                return Err(protocol::ErrorResponse::query_canceled().into());
                            }

                            if writer.has_data() {
                                buffer::write_direct(&mut self.socket, writer).await?
                            }

                            self.write_completion(completion).await?;
                        },
                    }
                }
            }

            Ok(())
        } else {
            Err(ErrorResponse::error(
                ErrorCode::InvalidCursorName,
                format!(r#"Unknown portal: {}"#, execute.portal),
            )
            .into())
        }
    }

    pub async fn bind(&mut self, body: protocol::Bind) -> Result<(), ConnectionError> {
        let source_statement = self.statements.get(&body.statement).ok_or_else(|| {
            ErrorResponse::error(
                ErrorCode::InvalidSqlStatement,
                format!(r#"Unknown statement: {}"#, body.statement),
            )
        })?;

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
            ));
        }

        let portal = if let Some(statement) = source_statement {
            let prepared_statement = statement.bind(body.to_bind_values(&statement.parameters)?)?;

            let meta = self
                .session
                .server
                .transport
                .meta(self.auth_context()?)
                .await?;

            let plan =
                convert_statement_to_cube_query(&prepared_statement, meta, self.session.clone())
                    .await?;

            let format = body.result_formats.first().unwrap_or(&Format::Text).clone();
            Some(Portal::new(plan, format, PortalFrom::Extended))
        } else {
            None
        };

        self.portals.insert(body.portal, portal);
        self.write(protocol::BindComplete::new()).await?;

        Ok(())
    }

    pub async fn parse(&mut self, parse: protocol::Parse) -> Result<(), ConnectionError> {
        if parse.query.trim() == "" {
            self.statements.insert(parse.name, None);
        } else {
            let query = parse_sql_to_statement(&parse.query, DatabaseProtocol::PostgreSQL)?;
            self.prepare_statement(parse.name, query).await?;
        }

        self.write(protocol::ParseComplete::new()).await?;

        Ok(())
    }

    pub async fn prepare_statement(
        &mut self,
        name: String,
        query: Statement,
    ) -> Result<(), ConnectionError> {
        if self.statements.len()
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
                        self.statements.len(),
                        self.session.server.configuration.connection_max_prepared_statements),
                )
                    .into(),
            ));
        }

        let stmt_finder = PostgresStatementParamsFinder::new();
        let parameters: Vec<PgTypeId> = stmt_finder
            .find(&query)?
            .into_iter()
            .map(|param| param.coltype.to_pg_tid())
            .collect();

        let meta = self
            .session
            .server
            .transport
            .meta(self.auth_context()?)
            .await?;

        let stmt_replacer = StatementPlaceholderReplacer::new();
        let hacked_query = stmt_replacer.replace(&query)?;

        let plan =
            convert_statement_to_cube_query(&hacked_query, meta, self.session.clone()).await?;

        let description = if let Some(description) = plan.to_row_description(Format::Text)? {
            if description.len() > 0 {
                Some(description)
            } else {
                None
            }
        } else {
            None
        };

        let pstmt = PreparedStatement {
            query,
            parameters: protocol::ParameterDescription::new(parameters),
            description,
        };
        self.statements.insert(name, Some(pstmt));

        Ok(())
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
    ) -> Result<(), ConnectionError> {
        let cancel = self.session.state.begin_query(stmt.to_string());

        tokio::select! {
            _ = cancel.cancelled() => {
                self.session.state.end_query();

                // We don't return error, because query can contains multiple statements
                // then cancel request will cancel only one query
                self.write(protocol::ErrorResponse::query_canceled()).await?;

                Ok(())
            },
            res = self.process_simple_query(stmt, meta, cancel.clone()) => {
                self.session.state.end_query();

                res
            },
        }
    }

    pub async fn process_simple_query(
        &mut self,
        stmt: ast::Statement,
        meta: Arc<MetaContext>,
        cancel: CancellationToken,
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
                    &mut Portal::new(plan, Format::Text, PortalFrom::Simple),
                    0,
                    cancel,
                )
                .await?;
            }
            Statement::Rollback { .. } => {
                if self.end_transaction()? == false {
                    // PostgreSQL returns command completion anyway
                    self.write(protocol::NoticeResponse::warning(
                        ErrorCode::NoActiveSqlTransaction,
                        "there is no transaction in progress".to_string(),
                    ))
                    .await?
                };

                let plan = QueryPlan::MetaOk(StatusFlags::empty(), CommandCompletion::Rollback);

                self.write_portal(
                    &mut Portal::new(plan, Format::Text, PortalFrom::Simple),
                    0,
                    CancellationToken::new(),
                )
                .await?;
            }
            Statement::Commit { .. } => {
                if self.end_transaction()? == false {
                    // PostgreSQL returns command completion anyway
                    self.write(protocol::NoticeResponse::warning(
                        ErrorCode::NoActiveSqlTransaction,
                        "there is no transaction in progress".to_string(),
                    ))
                    .await?
                };

                let plan = QueryPlan::MetaOk(StatusFlags::empty(), CommandCompletion::Commit);

                self.write_portal(
                    &mut Portal::new(plan, Format::Text, PortalFrom::Simple),
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
                                    ));
                                }

                                v.parse::<usize>().map_err(|err| ConnectionError::Protocol(
                                protocol::ErrorResponse::error(
                                    protocol::ErrorCode::ProtocolViolation,
                                    format!(r#""Unable to parse number "{}" for fetch limit: {}"#, v, err),
                                )
                                    .into(),
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
                        ));
                    }
                };

                if let Some(portal) = self.portals.remove(&name.value) {
                    if let Some(mut portal) = portal {
                        self.write_portal(&mut portal, limit, CancellationToken::new())
                            .await?;
                        self.portals.insert(name.value.clone(), Some(portal));

                        return Ok(());
                    } else {
                        return Err(ConnectionError::Protocol(
                            protocol::ErrorResponse::error(
                                protocol::ErrorCode::InternalError,
                                "Unable to unwrap Plan without plan, unexpected error".to_string(),
                            )
                            .into(),
                        ));
                    }
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
                    ));
                }

                let cursor = self.cursors.get(&name.value).ok_or_else(|| {
                    ConnectionError::Protocol(
                        protocol::ErrorResponse::error(
                            protocol::ErrorCode::ProtocolViolation,
                            format!(r#"cursor "{}" does not exist"#, name.value),
                        )
                        .into(),
                    )
                })?;

                let plan =
                    convert_statement_to_cube_query(&cursor.query, meta, self.session.clone())
                        .await?;

                let mut portal = Portal::new(plan, cursor.format, PortalFrom::Fetch);

                self.write_portal(&mut portal, limit, cancel).await?;
                self.portals.insert(name.value, Some(portal));
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
                    ));
                };

                if self.statements.contains_key(&name.value) {
                    return Err(ConnectionError::Protocol(
                        protocol::ErrorResponse::error(
                            protocol::ErrorCode::DuplicateCursor,
                            format!(r#"cursor "{}" already exists"#, name.value),
                        )
                        .into(),
                    ));
                }

                let select_stmt = Statement::Query(query);
                // It's just a verification that we can compile that query.
                let _ = convert_statement_to_cube_query(
                    &select_stmt,
                    meta.clone(),
                    self.session.clone(),
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
                    ));
                }

                self.cursors.insert(name.value, cursor);

                let plan =
                    QueryPlan::MetaOk(StatusFlags::empty(), CommandCompletion::DeclareCursor);

                self.write_portal(
                    &mut Portal::new(plan, Format::Text, PortalFrom::Simple),
                    0,
                    cancel,
                )
                .await?;
            }
            Statement::Discard { object_type } => {
                self.statements = HashMap::new();
                self.portals = HashMap::new();
                self.cursors = HashMap::new();

                let plan = QueryPlan::MetaOk(
                    StatusFlags::empty(),
                    CommandCompletion::Discard(object_type.to_string()),
                );

                self.write_portal(
                    &mut Portal::new(plan, Format::Text, PortalFrom::Simple),
                    0,
                    cancel,
                )
                .await?;
            }
            Statement::Close { cursor } => {
                let plan = match cursor {
                    CloseCursor::All => {
                        let mut portals_to_remove = Vec::new();

                        for (key, _) in &self.cursors {
                            portals_to_remove.push(key.clone());
                        }

                        self.cursors = HashMap::new();

                        for key in portals_to_remove {
                            self.portals.remove(&key);
                        }

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
                            ))
                        }
                    }
                }?;

                self.write_portal(
                    &mut Portal::new(plan, Format::Text, PortalFrom::Simple),
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

                self.prepare_statement(name.value, statement).await?;

                let plan = QueryPlan::MetaOk(StatusFlags::empty(), CommandCompletion::Prepare);

                self.write_portal(
                    &mut Portal::new(plan, Format::Text, PortalFrom::Simple),
                    0,
                    cancel,
                )
                .await?;
            }
            other => {
                let plan =
                    convert_statement_to_cube_query(&other, meta.clone(), self.session.clone())
                        .await?;

                self.write_portal(
                    &mut Portal::new(plan, Format::Text, PortalFrom::Simple),
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
        let mut writer = BatchWriter::new(portal.get_format());
        let completion = portal.execute(&mut writer, max_rows).await?;

        if cancel.is_cancelled() {
            return Ok(());
        }

        // Special handling for special queries, such as DISCARD ALL.
        if let Some(description) = portal.get_description()? {
            match description.len() {
                0 => self.write(protocol::NoData::new()).await?,
                _ => self.write(description).await?,
            };
        }

        if writer.has_data() {
            buffer::write_direct(&mut self.socket, writer).await?;
        };

        self.write_completion(completion).await
    }

    /// Pipeline of Execution
    /// process_query -> (&str)
    ///     execute_query -> (&str)
    ///         handle_simple_query
    ///             process_simple_query -> (portal)
    ///                 write_portal
    pub async fn execute_query(&mut self, query: &str) -> Result<(), ConnectionError> {
        let meta = self
            .session
            .server
            .transport
            .meta(self.auth_context()?)
            .await?;

        let statements = parse_sql_to_statements(&query.to_string(), DatabaseProtocol::PostgreSQL)?;

        if statements.len() == 0 {
            self.write(protocol::EmptyQuery::new()).await?;
        } else {
            for statement in statements {
                self.handle_simple_query(statement, meta.clone()).await?;
            }
        }

        Ok(())
    }

    pub async fn process_query(&mut self, query: String) -> Result<(), ConnectionError> {
        debug!("Query: {}", query);

        if let Err(err) = self.execute_query(&query).await {
            self.handle_connection_error(err, Some(query)).await?;
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

impl Drop for AsyncPostgresShim {
    fn drop(&mut self) {
        trace!(
            "[pg] Droping connection {}",
            self.session.state.connection_id
        );

        self.session
            .session_manager
            .drop_session(self.session.state.connection_id)
    }
}
