use std::{
    collections::HashMap,
    io::{Error, ErrorKind},
    sync::Arc,
};

use datafusion::{dataframe::DataFrame, execution::dataframe_impl::DataFrameImpl};
use log::{debug, error};
use tokio::{io::AsyncWriteExt, net::TcpStream};

use crate::{
    compile::convert_sql_to_cube_query,
    sql::{
        dataframe::{batch_to_dataframe, TableValue},
        AuthContext, QueryResponse, Session,
    },
    CubeError,
};

use super::{
    buffer,
    protocol::{self, FrontendMessage},
};

pub struct AsyncPostgresShim {
    socket: TcpStream,
    #[allow(unused)]
    parameters: HashMap<String, String>,
    session: Arc<Session>,
}

impl AsyncPostgresShim {
    pub async fn run_on(socket: TcpStream, session: Arc<Session>) -> Result<(), Error> {
        let mut shim = Self {
            socket,
            parameters: HashMap::new(),
            session,
        };
        match shim.run().await {
            Err(e) => {
                if e.kind() == ErrorKind::Unsupported {
                    shim.socket.shutdown().await?;
                    return Ok(());
                }
                Err(e)
            }
            _ => Ok(()),
        }
    }

    pub async fn run(&mut self) -> Result<(), Error> {
        self.process_startup_message().await?;
        loop {
            match buffer::read_message(&mut self.socket).await? {
                FrontendMessage::Query(query) => self.process_query(query).await?,
                FrontendMessage::Terminate => return Ok(()),
            }
        }
    }

    pub async fn write<Message: protocol::Serialize>(
        &mut self,
        message: Message,
    ) -> Result<(), Error> {
        buffer::write_message(&mut self.socket, message).await
    }

    pub async fn process_startup_message(&mut self) -> Result<(), Error> {
        let mut buffer = buffer::read_contents(&mut self.socket).await?;

        let startup_message = protocol::StartupMessage::from(&mut buffer).await?;
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
            return Err(Error::new(
                ErrorKind::Unsupported,
                "unsupported frontend protocol version",
            ));
        }

        self.parameters = startup_message.parameters;
        // TODO: throw an error on lack of "user" and default "database" to that if no value is provided
        // See StartupMessage: https://www.postgresql.org/docs/14/protocol-message-formats.html
        self.write(protocol::AuthenticationOk::new()).await?;

        self.write(protocol::ParameterStatus::new(
            "server_version".to_string(),
            "14.2 (Cube SQL)".to_string(),
        ))
        .await?;

        self.write(protocol::ReadyForQuery::new(
            protocol::TransactionStatus::Idle,
        ))
        .await?;
        Ok(())
    }

    pub async fn process_query(&mut self, query: protocol::Query) -> Result<(), Error> {
        let query = query.query;
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
            Ok(QueryResponse::Ok(_)) => {
                self.write(protocol::CommandComplete::new(
                    protocol::CommandCompleteTag::Select,
                    0,
                ))
                .await?;
            }
            Ok(QueryResponse::ResultSet(_, frame)) => {
                let mut fields = Vec::new();
                for column in frame.get_columns().iter() {
                    fields.push(protocol::RowDescriptionField::new(column.get_name()))
                }

                self.write(protocol::RowDescription::new(fields)).await?;

                for row in frame.get_rows().iter() {
                    let mut values = Vec::new();
                    for value in row.values().iter() {
                        let value = match value {
                            TableValue::Null => None,
                            TableValue::String(v) => Some(v.clone()),
                            TableValue::Int64(v) => Some(format!("{}", v)),
                            TableValue::Boolean(v) => {
                                Some((if *v { "t" } else { "v" }).to_string())
                            }
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
            }
        }
        self.write(protocol::ReadyForQuery::new(
            protocol::TransactionStatus::Idle,
        ))
        .await?;
        Ok(())
    }

    pub async fn execute_query(&mut self, query: &str) -> Result<QueryResponse, CubeError> {
        let meta = self
            .session
            .server
            .transport
            .meta(self.auth_context()?)
            .await?;

        let plan =
            convert_sql_to_cube_query(&query.to_string(), Arc::new(meta), self.session.clone())?;
        match plan {
            crate::compile::QueryPlan::MetaOk(status) => {
                return Ok(QueryResponse::Ok(status));
            }
            crate::compile::QueryPlan::MetaTabular(status, data_frame) => {
                return Ok(QueryResponse::ResultSet(status, data_frame));
            }
            crate::compile::QueryPlan::DataFusionSelect(status, plan, ctx) => {
                let df = DataFrameImpl::new(ctx.state, &plan);
                let batches = df.collect().await?;
                let response = batch_to_dataframe(&batches)?;

                return Ok(QueryResponse::ResultSet(status, Arc::new(response)));
            }
        }
    }

    pub(crate) fn auth_context(&self) -> Result<Arc<AuthContext>, CubeError> {
        if let Some(ctx) = self.session.state.auth_context() {
            Ok(Arc::new(ctx))
        } else {
            Err(CubeError::internal("must be auth".to_string()))
        }
    }
}
