use std::{backtrace::Backtrace, collections::HashMap, sync::Arc};

use super::extended::PreparedStatement;
use crate::{
    compile::{
        convert_sql_to_cube_query, convert_statement_to_cube_query, parser::parse_sql_to_statement,
        CompilationError, QueryPlan,
    },
    sql::{
        df_type_to_pg_tid,
        extended::Portal,
        session::DatabaseProtocol,
        statement::{StatementParamsFinder, StatementPlaceholderReplacer},
        writer::BatchWriter,
        AuthContext, Session,
    },
    CubeError,
};
use log::{debug, error, trace};
use pg_srv::{
    buffer, protocol,
    protocol::{ErrorCode, ErrorResponse, Format},
    PgType, PgTypeId, ProtocolError,
};
use tokio::{io::AsyncWriteExt, net::TcpStream};

pub struct AsyncPostgresShim {
    socket: TcpStream,
    // Extended query
    statements: HashMap<String, Option<PreparedStatement>>,
    portals: HashMap<String, Option<Portal>>,
    // Shared
    session: Arc<Session>,
}

#[derive(PartialEq, Eq)]
pub enum StartupState {
    // Initial parameters which client sends in the first message, we use it later in auth method
    Success(HashMap<String, String>),
    SslRequested,
    Denied,
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
            ConnectionError::CompilationError(e) => match e {
                CompilationError::Internal(_, _) => protocol::ErrorResponse::error(
                    protocol::ErrorCode::InternalError,
                    e.to_string(),
                ),
                CompilationError::User(_) => protocol::ErrorResponse::error(
                    protocol::ErrorCode::InvalidSqlStatement,
                    e.to_string(),
                ),
                CompilationError::Unsupported(_) => protocol::ErrorResponse::error(
                    protocol::ErrorCode::FeatureNotSupported,
                    e.to_string(),
                ),
            },
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
    pub async fn run_on(socket: TcpStream, session: Arc<Session>) -> Result<(), std::io::Error> {
        let mut shim = Self {
            socket,
            portals: HashMap::new(),
            statements: HashMap::new(),
            session,
        };

        match shim.run().await {
            Err(e) => {
                error!("Error during processing PostgreSQL connection: {}", e);

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
        let initial_parameters = match self.process_startup_message().await? {
            StartupState::Success(parameters) => parameters,
            StartupState::SslRequested => match self.process_startup_message().await? {
                StartupState::Success(parameters) => parameters,
                _ => return Ok(()),
            },
            StartupState::Denied => return Ok(()),
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

        loop {
            let result = match buffer::read_message(&mut self.socket).await? {
                protocol::FrontendMessage::Query(body) => self.process_query(body.query).await,
                protocol::FrontendMessage::Parse(body) => self.parse(body).await,
                protocol::FrontendMessage::Bind(body) => self.bind(body).await,
                protocol::FrontendMessage::Execute(body) => self.execute(body).await,
                protocol::FrontendMessage::Close(body) => self.close(body).await,
                protocol::FrontendMessage::Describe(body) => self.describe(body).await,
                protocol::FrontendMessage::Sync => self.sync().await,
                protocol::FrontendMessage::Terminate => return Ok(()),
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
                self.handle_connection_error(err).await?;
            }
        }
    }

    pub async fn handle_connection_error(
        &mut self,
        err: ConnectionError,
    ) -> Result<(), ConnectionError> {
        error!("Error during processing PostgreSQL message: {}", err);

        if let Some(bt) = err.backtrace() {
            trace!("{}", bt);
        } else {
            trace!("Backtrace: not found");
        }

        self.write(err.to_error_response()).await?;

        Ok(())
    }

    pub async fn write<Message: protocol::Serialize>(
        &mut self,
        message: Message,
    ) -> Result<(), ConnectionError> {
        buffer::write_message(&mut self.socket, message).await?;

        Ok(())
    }

    pub async fn process_startup_message(&mut self) -> Result<StartupState, ConnectionError> {
        let mut buffer = buffer::read_contents(&mut self.socket, 0).await?;

        let startup_message = protocol::StartupMessage::from(&mut buffer).await?;

        if startup_message.protocol_version.major == protocol::SSL_REQUEST_PROTOCOL {
            self.write(protocol::SSLResponse::new()).await?;
            return Ok(StartupState::SslRequested);
        }

        if startup_message.protocol_version.major != 3
            || startup_message.protocol_version.minor != 0
        {
            let error_response = protocol::ErrorResponse::new(
                protocol::ErrorSeverity::Fatal,
                protocol::ErrorCode::FeatureNotSupported,
                format!(
                    "unsupported frontend protocol {}.{}: server supports 3.0 to 3.0",
                    startup_message.protocol_version.major, startup_message.protocol_version.minor,
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

        return Ok(StartupState::Success(parameters));
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

        let mut auth_context: Option<AuthContext> = None;
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
        let params = [
            ("server_version".to_string(), "14.2 (Cube SQL)".to_string()),
            ("server_encoding".to_string(), "UTF8".to_string()),
            ("client_encoding".to_string(), "UTF8".to_string()),
            ("DateStyle".to_string(), "ISO".to_string()),
        ];

        for (key, value) in params {
            self.write(protocol::ParameterStatus::new(key, value))
                .await?;
        }

        self.write(protocol::ReadyForQuery::new(
            protocol::TransactionStatus::Idle,
        ))
        .await?;

        Ok(())
    }

    pub async fn sync(&mut self) -> Result<(), ConnectionError> {
        self.write(protocol::ReadyForQuery::new(
            protocol::TransactionStatus::Idle,
        ))
        .await?;

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
                Some(named) => match named.get_description().clone() {
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

    pub async fn execute(&mut self, execute: protocol::Execute) -> Result<(), ConnectionError> {
        match self.portals.get_mut(&execute.portal) {
            Some(portal) => match portal {
                // We use None for Statement on empty query
                None => {
                    self.write(protocol::EmptyQueryResponse::new()).await?;
                }
                Some(portal) => {
                    let mut writer = BatchWriter::new(portal.get_format());
                    let completion = portal
                        .execute(&mut writer, execute.max_rows as usize)
                        .await?;

                    if writer.has_data() {
                        buffer::write_direct(&mut self.socket, writer).await?
                    }

                    self.write(completion).await?;
                }
            },
            None => {
                self.write(protocol::ReadyForQuery::new(
                    protocol::TransactionStatus::Idle,
                ))
                .await?;
            }
        }

        Ok(())
    }

    pub async fn bind(&mut self, body: protocol::Bind) -> Result<(), ConnectionError> {
        let source_statement = self.statements.get(&body.statement).ok_or_else(|| {
            ErrorResponse::error(
                ErrorCode::InvalidSqlStatement,
                "Unknown statement".to_string(),
            )
        })?;

        let portal = if let Some(statement) = source_statement {
            let prepared_statement = statement.bind(body.to_bind_values());

            let meta = self
                .session
                .server
                .transport
                .meta(self.auth_context()?)
                .await?;

            let plan =
                convert_statement_to_cube_query(&prepared_statement, meta, self.session.clone())?;

            let fields = self.query_plan_to_row_description(&plan).await?;
            let description = if fields.len() > 0 {
                Some(protocol::RowDescription::new(
                    self.query_plan_to_row_description(&plan).await?,
                ))
            } else {
                None
            };

            let format = body.result_formats.first().unwrap_or(&Format::Text).clone();

            Some(Portal::new(plan, format, description))
        } else {
            None
        };

        self.portals.insert(body.portal, portal);
        self.write(protocol::BindComplete::new()).await?;

        Ok(())
    }

    async fn query_plan_to_row_description(
        &mut self,
        plan: &QueryPlan,
    ) -> Result<Vec<protocol::RowDescriptionField>, ConnectionError> {
        match plan {
            QueryPlan::MetaOk(_, _) => Ok(vec![]),
            QueryPlan::MetaTabular(_, frame) => {
                let mut result = vec![];

                for field in frame.get_columns() {
                    result.push(protocol::RowDescriptionField::new(
                        field.get_name(),
                        PgType::get_by_tid(PgTypeId::TEXT),
                    ));
                }

                Ok(result)
            }
            QueryPlan::DataFusionSelect(_, logical_plan, _) => {
                let mut result = vec![];

                for field in logical_plan.schema().fields() {
                    result.push(protocol::RowDescriptionField::new(
                        field.name().clone(),
                        df_type_to_pg_tid(field.data_type())?.to_type(),
                    ));
                }

                Ok(result)
            }
        }
    }

    pub async fn parse(&mut self, parse: protocol::Parse) -> Result<(), ConnectionError> {
        let prepared = if parse.query.trim() == "" {
            None
        } else {
            let query = parse_sql_to_statement(&parse.query, DatabaseProtocol::PostgreSQL)?;

            let stmt_finder = StatementParamsFinder::new();
            let parameters: Vec<PgTypeId> = stmt_finder
                .find(&query)
                .into_iter()
                .map(|_p| PgTypeId::TEXT)
                .collect();

            let meta = self
                .session
                .server
                .transport
                .meta(self.auth_context()?)
                .await?;

            let stmt_replacer = StatementPlaceholderReplacer::new();
            let hacked_query = stmt_replacer.replace(&query);

            let result = convert_statement_to_cube_query(&hacked_query, meta, self.session.clone());
            if let Err(_) = &result {
                log::error!("Failed SQL: {}", parse.query);
            }
            let plan = result?;
            let fields: Vec<protocol::RowDescriptionField> =
                self.query_plan_to_row_description(&plan).await?;
            let description = if fields.len() > 0 {
                Some(protocol::RowDescription::new(fields))
            } else {
                None
            };

            Some(PreparedStatement {
                query,
                parameters: protocol::ParameterDescription::new(parameters),
                description,
            })
        };

        self.statements.insert(parse.name, prepared);

        self.write(protocol::ParseComplete::new()).await?;

        Ok(())
    }

    pub async fn execute_query(&mut self, query: &str) -> Result<(), ConnectionError> {
        let meta = self
            .session
            .server
            .transport
            .meta(self.auth_context()?)
            .await?;

        let plan = convert_sql_to_cube_query(&query.to_string(), meta, self.session.clone())?;

        let description = self.query_plan_to_row_description(&plan).await?;
        match description.len() {
            0 => self.write(protocol::NoData::new()).await?,
            _ => {
                self.write(protocol::RowDescription::new(description))
                    .await?
            }
        };

        // Re-usage of Portal functionality
        let mut portal = Portal::new(plan, Format::Text, None);

        let mut writer = BatchWriter::new(portal.get_format());
        let completion = portal.execute(&mut writer, 0).await?;

        if writer.has_data() {
            buffer::write_direct(&mut self.socket, writer).await?;
        };

        self.write(completion).await?;

        Ok(())
    }

    pub async fn process_query(&mut self, query: String) -> Result<(), ConnectionError> {
        debug!("Query: {}", query);

        if let Err(err) = self.execute_query(&query).await {
            log::error!("Failed SQL: {}", query);
            self.handle_connection_error(err).await?;
        };

        self.write(protocol::ReadyForQuery::new(
            protocol::TransactionStatus::Idle,
        ))
        .await?;

        Ok(())
    }

    pub(crate) fn auth_context(&self) -> Result<Arc<AuthContext>, CubeError> {
        if let Some(ctx) = self.session.state.auth_context() {
            Ok(Arc::new(ctx))
        } else {
            Err(CubeError::internal("must be auth".to_string()))
        }
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
