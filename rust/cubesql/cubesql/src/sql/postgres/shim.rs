use std::{
    collections::HashMap,
    io::{Error, ErrorKind},
    sync::Arc,
};

use datafusion::dataframe::DataFrame as DFDataFrame;
use log::{debug, error, trace};
use tokio::{io::AsyncWriteExt, net::TcpStream};

use crate::sql::dataframe::DataFrame;
use crate::sql::statement::StatementPlaceholderReplacer;
use crate::{
    compile::{
        convert_sql_to_cube_query, convert_statement_to_cube_query, parser::parse_sql_to_statement,
        QueryPlan,
    },
    sql::{
        dataframe::{batch_to_dataframe, TableValue},
        session::DatabaseProtocol,
        statement::StatementParamsFinder,
        AuthContext, PgType, PgTypeId, Session,
    },
    CubeError,
};

use super::{
    buffer,
    protocol::{self, FrontendMessage, RowDescriptionField, SSL_REQUEST_PROTOCOL},
    statement::PreparedStatement,
};

pub struct Portal {
    plan: QueryPlan,
}

pub struct AsyncPostgresShim {
    socket: TcpStream,
    #[allow(unused)]
    parameters: HashMap<String, String>,
    statements: HashMap<String, PreparedStatement>,
    portals: HashMap<String, Portal>,
    // Shared
    session: Arc<Session>,
}

#[derive(PartialEq, Eq)]
pub enum StartupState {
    Success,
    SslRequested,
    Denied,
}

impl AsyncPostgresShim {
    pub async fn run_on(socket: TcpStream, session: Arc<Session>) -> Result<(), Error> {
        let mut shim = Self {
            socket,
            parameters: HashMap::new(),
            portals: HashMap::new(),
            statements: HashMap::new(),
            session,
        };
        match shim.run().await {
            Err(e) => {
                if e.kind() == ErrorKind::UnexpectedEof
                    && shim.session.state.auth_context().is_none()
                {
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

    pub async fn run(&mut self) -> Result<(), Error> {
        match self.process_startup_message().await? {
            StartupState::Success => {}
            StartupState::SslRequested => {
                if self.process_startup_message().await? != StartupState::Success {
                    return Ok(());
                }
            }
            StartupState::Denied => return Ok(()),
        }

        match buffer::read_message(&mut self.socket).await? {
            FrontendMessage::PasswordMessage(password_message) => {
                if !self.authenticate(password_message).await? {
                    return Ok(());
                }
            }
            _ => return Ok(()),
        }
        self.ready().await?;

        loop {
            match buffer::read_message(&mut self.socket).await? {
                FrontendMessage::Query(body) => self.process_query(body.query).await?,
                FrontendMessage::Parse(body) => self.parse(body).await?,
                FrontendMessage::Bind(body) => self.bind(body).await?,
                FrontendMessage::Execute(body) => self.execute(body).await?,
                FrontendMessage::Close(body) => self.close(body).await?,
                FrontendMessage::Describe(body) => self.describe(body).await?,
                FrontendMessage::Sync => self.sync().await?,
                FrontendMessage::Terminate => return Ok(()),
                command_id => {
                    return Err(Error::new(
                        ErrorKind::Unsupported,
                        format!("Unsupported operation: {:?}", command_id),
                    ))
                }
            }
        }
    }

    pub async fn write<Message: protocol::Serialize>(
        &mut self,
        message: Message,
    ) -> Result<(), Error> {
        buffer::write_message(&mut self.socket, message).await
    }

    pub async fn process_startup_message(&mut self) -> Result<StartupState, Error> {
        let mut buffer = buffer::read_contents(&mut self.socket, 0).await?;

        let startup_message = protocol::StartupMessage::from(&mut buffer).await?;

        if startup_message.protocol_version.major == SSL_REQUEST_PROTOCOL {
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

        self.parameters = startup_message.parameters;
        if !self.parameters.contains_key("user") {
            let error_response = protocol::ErrorResponse::new(
                protocol::ErrorSeverity::Fatal,
                protocol::ErrorCode::InvalidAuthorizationSpecification,
                "no PostgreSQL user name specified in startup packet".to_string(),
            );
            buffer::write_message(&mut self.socket, error_response).await?;
            return Ok(StartupState::Denied);
        }
        if !self.parameters.contains_key("database") {
            self.parameters.insert(
                "database".to_string(),
                self.parameters.get("user").unwrap().clone(),
            );
        }

        self.write(protocol::Authentication::new(
            protocol::AuthenticationRequest::CleartextPassword,
        ))
        .await?;

        return Ok(StartupState::Success);
    }

    pub async fn authenticate(
        &mut self,
        password_message: protocol::PasswordMessage,
    ) -> Result<bool, Error> {
        let user = self.parameters.get("user").unwrap().clone();
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
            let error_response = protocol::ErrorResponse::new(
                protocol::ErrorSeverity::Fatal,
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

    pub async fn ready(&mut self) -> Result<(), Error> {
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

    pub async fn sync(&mut self) -> Result<(), Error> {
        self.write(protocol::ReadyForQuery::new(
            protocol::TransactionStatus::Idle,
        ))
        .await?;

        Ok(())
    }

    pub async fn describe(&mut self, describe: protocol::Describe) -> Result<(), Error> {
        let (parameters, description) = match describe.typ {
            protocol::DescribeType::Statement => {
                let stmt = self.statements.get(&describe.name);

                if let Some(s) = stmt {
                    (s.parameters.clone(), s.description.clone())
                } else {
                    self.write(protocol::ErrorResponse::new(
                        protocol::ErrorSeverity::Error,
                        protocol::ErrorCode::InvalidSqlStatement,
                        "missing statement".to_string(),
                    ))
                    .await?;

                    return Ok(());
                }
            }
            protocol::DescribeType::Portal => {
                unimplemented!("Unable to describe portal");
            }
        };

        self.write(parameters).await?;
        self.write(description).await?;

        Ok(())
    }

    pub async fn close(&mut self, body: protocol::Close) -> Result<(), Error> {
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

    pub async fn execute(&mut self, execute: protocol::Execute) -> Result<(), Error> {
        let portal = self.portals.get(&execute.portal);
        match portal {
            Some(portal) => {
                if execute.max_rows == 0 {
                    match self.execute_plan(portal.plan.clone(), false).await {
                        Err(e) => {
                            self.write(protocol::ErrorResponse::new(
                                protocol::ErrorSeverity::Error,
                                protocol::ErrorCode::InternalError,
                                e.message,
                            ))
                            .await?;
                        }
                        Ok(_) => {}
                    }
                } else {
                    self.write(protocol::ErrorResponse::new(
                        protocol::ErrorSeverity::Error,
                        protocol::ErrorCode::InternalError,
                        "Execute with limited rows is not supported".to_string(),
                    ))
                    .await?;
                }
            }
            None => {
                self.write(protocol::ReadyForQuery::new(
                    protocol::TransactionStatus::Idle,
                ))
                .await?;
            }
        }

        Ok(())
    }

    pub async fn bind(&mut self, bind: protocol::Bind) -> Result<(), Error> {
        let source_statement = self
            .statements
            .get(&bind.statement)
            .ok_or_else(|| Error::new(ErrorKind::Other, "Unknown statement"))?;

        let prepared_statement = source_statement.bind(vec![]);

        let meta = self
            .session
            .server
            .transport
            .meta(self.auth_context().unwrap())
            .await
            .unwrap();

        let plan = convert_statement_to_cube_query(&prepared_statement, meta, self.session.clone())
            .unwrap();

        let portal = Portal { plan };

        self.portals.insert(bind.portal, portal);

        self.write(protocol::BindComplete::new()).await?;

        Ok(())
    }

    pub async fn parse(&mut self, parse: protocol::Parse) -> Result<(), Error> {
        let mut query = parse_sql_to_statement(&parse.query, DatabaseProtocol::PostgreSQL).unwrap();

        let stmt_finder = StatementParamsFinder::new();
        let parameters: Vec<PgTypeId> = stmt_finder
            .prepare(&mut query)
            .into_iter()
            .map(|_p| PgTypeId::TEXT)
            .collect();

        let meta = self
            .session
            .server
            .transport
            .meta(self.auth_context().unwrap())
            .await
            .unwrap();

        let stmt_replacer = StatementPlaceholderReplacer::new();
        let hacked_query = stmt_replacer.replace(&mut query);

        let plan =
            convert_statement_to_cube_query(&hacked_query, meta, self.session.clone()).unwrap();

        let fields: Vec<RowDescriptionField> = match plan {
            QueryPlan::MetaOk(_) => vec![],
            QueryPlan::MetaTabular(_, frame) => {
                let mut result = vec![];

                for _field in frame.get_columns() {
                    result.push(RowDescriptionField::new(
                        "?column?".to_string(),
                        PgType::get_by_tid(PgTypeId::TEXT),
                    ));
                }

                result
            }
            QueryPlan::DataFusionSelect(_, logical_plan, _) => {
                let mut result = vec![];

                for _field in logical_plan.schema().fields() {
                    result.push(RowDescriptionField::new(
                        "?column?".to_string(),
                        PgType::get_by_tid(PgTypeId::TEXT),
                    ));
                }

                result
            }
        };

        self.statements.insert(
            parse.name,
            PreparedStatement {
                query,
                parameters: protocol::ParameterDescription::new(parameters),
                description: protocol::RowDescription::new(fields),
            },
        );

        self.write(protocol::ParseComplete::new()).await?;

        Ok(())
    }

    async fn write_batch(
        &mut self,
        frame: Arc<DataFrame>,
        description: bool,
    ) -> Result<(), CubeError> {
        if description {
            let mut fields = Vec::new();

            for column in frame.get_columns().iter() {
                fields.push(protocol::RowDescriptionField::new(
                    column.get_name(),
                    PgType::get_by_tid(PgTypeId::TEXT),
                ))
            }

            self.write(protocol::RowDescription::new(fields)).await?;
        }

        for row in frame.get_rows().iter() {
            let mut values = Vec::new();
            for value in row.values().iter() {
                let value = match value {
                    TableValue::Null => None,
                    TableValue::String(v) => Some(v.clone()),
                    TableValue::Int64(v) => Some(format!("{}", v)),
                    TableValue::Boolean(v) => Some((if *v { "t" } else { "v" }).to_string()),
                    TableValue::Float64(v) => Some(format!("{}", v)),
                    TableValue::Timestamp(v) => Some(v.to_string()),
                };
                values.push(value);
            }

            self.write(protocol::DataRow::new(values)).await?;
        }

        self.write(protocol::CommandComplete::new(
            protocol::CommandCompleteTag::Select,
            0,
        ))
        .await?;

        Ok(())
    }

    async fn execute_plan(&mut self, plan: QueryPlan, description: bool) -> Result<(), CubeError> {
        match plan {
            QueryPlan::MetaOk(_) => {
                self.write(protocol::CommandComplete::new(
                    protocol::CommandCompleteTag::Select,
                    0,
                ))
                .await?;
            }
            QueryPlan::MetaTabular(_, data_frame) => {
                self.write_batch(data_frame, description).await?;
            }
            QueryPlan::DataFusionSelect(_, plan, ctx) => {
                let df = DFDataFrame::new(ctx.state, &plan);
                let batches = df.collect().await?;
                let data_frame = batch_to_dataframe(&batches)?;

                self.write_batch(Arc::new(data_frame), description).await?;
            }
        };

        Ok(())
    }

    pub async fn execute_query(&mut self, query: &str) -> Result<(), CubeError> {
        let meta = self
            .session
            .server
            .transport
            .meta(self.auth_context()?)
            .await?;

        let plan = convert_sql_to_cube_query(&query.to_string(), meta, self.session.clone())?;
        self.execute_plan(plan, true).await
    }

    pub async fn process_query(&mut self, query: String) -> Result<(), Error> {
        debug!("Query: {}", query);

        match self.execute_query(&query).await {
            Err(e) => {
                let error_message = e.to_string();
                error!("Error during processing {}: {}", query, error_message);
                self.write(protocol::ErrorResponse::new(
                    protocol::ErrorSeverity::Error,
                    protocol::ErrorCode::InternalError,
                    error_message,
                ))
                .await?;
            }
            Ok(_) => {
                self.write(protocol::CommandComplete::new(
                    protocol::CommandCompleteTag::Select,
                    0,
                ))
                .await?;
            }
        }

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
