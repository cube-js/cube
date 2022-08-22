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
        server::ConnectionError,
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
    protocol::{CancelRequest, ErrorCode, ErrorResponse, Format, InitialMessage, PortalCompletion},
    PgType, PgTypeId, ProtocolError,
};
use sqlparser::ast::{self, CloseCursor, FetchDirection, Query, SetExpr, Statement, Value};
use std::{
    backtrace::Backtrace, collections::HashMap, io::ErrorKind, marker::PhantomData, sync::Arc,
};
use tokio::{io::AsyncWriteExt, net::TcpStream};
use tokio_util::sync::CancellationToken;

type InitialParameters = HashMap<String, String>;

/// PostgresConnectionTrait handles connection with session and engine
#[async_trait]
pub trait PostgresConnectionTrait {
    async fn run(&mut self) -> Result<(), ConnectionError>;
}

pub struct PostgresConnection {
    pub session: Arc<Session>,
    pub socket: TcpStream,
}

impl PostgresConnection {
    async fn ready(&mut self) -> Result<(), ConnectionError> {
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

    async fn write_multi<Message: protocol::Serialize>(
        &mut self,
        message: Vec<Message>,
    ) -> Result<(), ConnectionError> {
        buffer::write_messages(&mut self.socket, message).await?;

        Ok(())
    }

    async fn write<Message: protocol::Serialize>(
        &mut self,
        message: Message,
    ) -> Result<(), ConnectionError> {
        buffer::write_message(&mut self.socket, message).await?;

        Ok(())
    }
}

#[async_trait]
impl PostgresConnectionTrait for PostgresConnection {
    async fn run(&mut self) -> Result<(), ConnectionError> {
        todo!()
    }
}
