use datafusion::arrow::datatypes::DataType;
use std::{
    collections::HashMap,
    io,
    io::{Error, ErrorKind},
    sync::Arc,
};

use datafusion::dataframe::DataFrame as DFDataFrame;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use log::{debug, error, trace};

use tokio::{io::AsyncWriteExt, net::TcpStream};

use crate::sql::dataframe::DataFrame;
use crate::sql::extended::Portal;
use crate::sql::protocol::Format;
use crate::sql::statement::StatementPlaceholderReplacer;
use crate::sql::writer::BatchWriter;
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
    extended::PreparedStatement,
    protocol::{self, FrontendMessage, RowDescriptionField, SSL_REQUEST_PROTOCOL},
};

pub struct AsyncPostgresShim {
    socket: TcpStream,
    #[allow(unused)]
    parameters: HashMap<String, String>,
    // Extended query
    statements: HashMap<String, Option<PreparedStatement>>,
    portals: HashMap<String, Option<Portal>>,
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

    pub async fn describe_portal(&mut self, name: String) -> Result<(), Error> {
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
                Some(named) => match named.description.clone() {
                    // If Query doesnt return data, no fields in response.
                    None => self.write(protocol::NoData::new()).await,
                    Some(packet) => self.write(packet).await,
                },
            },
        }
    }

    pub async fn describe_statement(&mut self, name: String) -> Result<(), Error> {
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

    pub async fn describe(&mut self, body: protocol::Describe) -> Result<(), Error> {
        match body.typ {
            protocol::DescribeType::Statement => self.describe_statement(body.name).await,
            protocol::DescribeType::Portal => self.describe_portal(body.name).await,
        }
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
        match self.portals.get(&execute.portal) {
            Some(portal) => match portal {
                // We use None for Statement on empty query
                None => {
                    self.write(protocol::EmptyQueryResponse::new()).await?;
                }
                Some(portal) => match portal.description {
                    // If query doesnt return any fields, we can return complete without execution
                    None => {
                        self.write(protocol::CommandComplete::new(
                            protocol::CommandCompleteTag::Select,
                            0,
                        ))
                        .await?;
                    }
                    Some(_) => {
                        if execute.max_rows == 0 {
                            // TODO: I will rewrite this code later, it's just a prototype
                            #[allow(mutable_borrow_reservation_conflict)]
                            match self
                                .execute_plan(portal.plan.clone(), false, portal.format)
                                .await
                            {
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
                },
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

    pub async fn bind(&mut self, body: protocol::Bind) -> Result<(), Error> {
        let source_statement = self
            .statements
            .get(&body.statement)
            .ok_or_else(|| Error::new(ErrorKind::Other, "Unknown statement"))?;

        let portal = if let Some(statement) = source_statement {
            let prepared_statement = statement.bind(body.to_bind_values());

            let meta = self
                .session
                .server
                .transport
                .meta(self.auth_context().unwrap())
                .await
                .unwrap();

            let plan =
                convert_statement_to_cube_query(&prepared_statement, meta, self.session.clone())
                    .unwrap();

            let format = body
                .result_formats
                .first()
                .clone()
                .unwrap_or(&Format::Text)
                .clone();

            let fields = self.query_plan_to_row_description(&plan).await?;
            let description = if fields.len() > 0 {
                Some(protocol::RowDescription::new(
                    self.query_plan_to_row_description(&plan).await?,
                ))
            } else {
                None
            };

            Some(Portal {
                plan,
                format,
                description,
            })
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
    ) -> Result<Vec<RowDescriptionField>, Error> {
        match plan {
            QueryPlan::MetaOk(_) => Ok(vec![]),
            QueryPlan::MetaTabular(_, frame) => {
                let mut result = vec![];

                for field in frame.get_columns() {
                    result.push(RowDescriptionField::new(
                        field.get_name(),
                        PgType::get_by_tid(PgTypeId::TEXT),
                    ));
                }

                Ok(result)
            }
            QueryPlan::DataFusionSelect(_, logical_plan, _) => {
                let mut result = vec![];

                for field in logical_plan.schema().fields() {
                    result.push(RowDescriptionField::new(
                        field.name().clone(),
                        match field.data_type() {
                            DataType::Boolean => PgType::get_by_tid(PgTypeId::BOOL),
                            DataType::Int16 => PgType::get_by_tid(PgTypeId::INT2),
                            DataType::Int32 => PgType::get_by_tid(PgTypeId::INT4),
                            DataType::Int64 => PgType::get_by_tid(PgTypeId::INT8),
                            DataType::UInt16 => PgType::get_by_tid(PgTypeId::INT8),
                            DataType::UInt32 => PgType::get_by_tid(PgTypeId::INT8),
                            DataType::UInt64 => PgType::get_by_tid(PgTypeId::INT8),
                            DataType::Float32 => PgType::get_by_tid(PgTypeId::FLOAT4),
                            DataType::Float64 => PgType::get_by_tid(PgTypeId::FLOAT8),
                            DataType::Utf8 => PgType::get_by_tid(PgTypeId::TEXT),
                            DataType::LargeUtf8 => PgType::get_by_tid(PgTypeId::TEXT),
                            DataType::Null => PgType::get_by_tid(PgTypeId::BOOL),
                            data_type => {
                                let message =
                                    format!("Unsupported data type for pg-wire: {:?}", data_type);

                                self.write(protocol::ErrorResponse::new(
                                    protocol::ErrorSeverity::Error,
                                    protocol::ErrorCode::InternalError,
                                    message.clone(),
                                ))
                                .await?;

                                return Err(io::Error::new(io::ErrorKind::Other, message));
                            }
                        },
                    ));
                }

                Ok(result)
            }
        }
    }

    pub async fn parse(&mut self, parse: protocol::Parse) -> Result<(), Error> {
        let prepared = if parse.query.trim() == "" {
            None
        } else {
            let query = parse_sql_to_statement(&parse.query, DatabaseProtocol::PostgreSQL).unwrap();

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
                .meta(self.auth_context().unwrap())
                .await
                .unwrap();

            let stmt_replacer = StatementPlaceholderReplacer::new();
            let hacked_query = stmt_replacer.replace(&query);

            let plan =
                convert_statement_to_cube_query(&hacked_query, meta, self.session.clone()).unwrap();
            let fields: Vec<RowDescriptionField> =
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

    async fn write_data_frame(
        &mut self,
        frame: Arc<DataFrame>,
        description: bool,
        format: Format,
    ) -> Result<u32, CubeError> {
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

        let mut total: u32 = 0;
        let mut batch_writer = BatchWriter::new(format);

        for row in frame.get_rows() {
            for value in row.values() {
                match value {
                    TableValue::Null => batch_writer.write_value::<Option<bool>>(None)?,
                    TableValue::String(v) => batch_writer.write_value(v.clone())?,
                    TableValue::Int64(v) => batch_writer.write_value(*v)?,
                    TableValue::Boolean(v) => batch_writer.write_value(*v)?,
                    TableValue::Float64(v) => batch_writer.write_value(*v)?,
                    // @todo Support value
                    TableValue::Timestamp(v) => batch_writer.write_value(v.to_string())?,
                };
            }

            total += 1;
            batch_writer.end_row()?;
        }

        buffer::write_direct(&mut self.socket, batch_writer).await?;

        Ok(total)
    }

    async fn write_stream(
        &mut self,
        mut stream: SendableRecordBatchStream,
        description: bool,
        format: Format,
    ) -> Result<u32, CubeError> {
        let mut total: u32 = 0;
        let mut first = true;

        loop {
            match stream.next().await {
                None => {
                    return Ok(total);
                }
                Some(res) => match res {
                    Ok(batch) => {
                        let frame = Arc::new(batch_to_dataframe(&vec![batch])?);
                        total += self
                            .write_data_frame(frame, description && first, format)
                            .await?;

                        first = false;
                    }
                    Err(err) => {
                        error!("Error during processing: {}", err);

                        self.write(protocol::ErrorResponse::new(
                            protocol::ErrorSeverity::Error,
                            protocol::ErrorCode::DataException,
                            err.to_string(),
                        ))
                        .await?;

                        return Err(err.into());
                    }
                },
            }
        }
    }

    async fn execute_plan(
        &mut self,
        plan: QueryPlan,
        description: bool,
        format: Format,
    ) -> Result<(), CubeError> {
        match plan {
            QueryPlan::MetaOk(_) => {
                self.write(protocol::CommandComplete::new(
                    protocol::CommandCompleteTag::Select,
                    0,
                ))
                .await?;
            }
            QueryPlan::MetaTabular(_, data_frame) => {
                let total_rows = self
                    .write_data_frame(data_frame, description, format)
                    .await?;

                self.write(protocol::CommandComplete::new(
                    protocol::CommandCompleteTag::Select,
                    total_rows,
                ))
                .await?;
            }
            QueryPlan::DataFusionSelect(_, plan, ctx) => {
                let df = DFDataFrame::new(ctx.state, &plan);
                let stream = df.execute_stream().await?;
                let total_rows = self.write_stream(stream, description, format).await?;

                self.write(protocol::CommandComplete::new(
                    protocol::CommandCompleteTag::Select,
                    total_rows,
                ))
                .await?;
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
        self.execute_plan(plan, true, Format::Text).await
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
            Ok(_) => {}
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
