//! Implementation of PostgreSQL protocol.
//! Specification for all frontend/backend messages: <https://www.postgresql.org/docs/14/protocol-message-formats.html>
//! Message Data Types: <https://www.postgresql.org/docs/14/protocol-message-types.html>

use std::{
    collections::HashMap,
    convert::TryFrom,
    fmt::{self, Display, Formatter},
    io::{Cursor, Error},
};

use async_trait::async_trait;

use bytes::BufMut;
use tokio::io::AsyncReadExt;

use crate::{buffer, BindValue, FromProtocolValue, PgType, PgTypeId, ProtocolError};

const DEFAULT_CAPACITY: usize = 64;

#[derive(Debug, PartialEq, Clone)]
pub struct StartupMessage {
    pub major: i16,
    pub minor: i16,
    pub parameters: HashMap<String, String>,
}

impl StartupMessage {
    async fn from(mut buffer: &mut Cursor<Vec<u8>>) -> Result<Self, Error> {
        let major = buffer.read_i16().await?;
        let minor = buffer.read_i16().await?;

        let mut parameters = HashMap::new();

        loop {
            let name = buffer::read_string(&mut buffer).await?;
            if name.is_empty() {
                break;
            }
            let value = buffer::read_string(&mut buffer).await?;
            parameters.insert(name, value);
        }

        Ok(Self {
            major,
            minor,
            parameters,
        })
    }
}

#[derive(Debug, PartialEq)]
pub struct CancelRequest {
    pub process_id: i32,
    pub secret: i32,
}

impl CancelRequest {
    async fn from(buffer: &mut Cursor<Vec<u8>>) -> Result<Self, Error> {
        Ok(Self {
            process_id: buffer.read_i32().await?,
            secret: buffer.read_i32().await?,
        })
    }
}

pub enum InitialMessage {
    Startup(StartupMessage),
    CancelRequest(CancelRequest),
    SslRequest,
    Gssenc,
}

// The value is chosen to contain 1234 in the most significant 16 bits, this code must not be the same as any protocol version number.
pub const VERSION_MAJOR_SPECIAL: i16 = 1234;
pub const VERSION_MINOR_CANCEL: i16 = 5678;
pub const VERSION_MINOR_SSL: i16 = 5679;
pub const VERSION_MINOR_GSSENC: i16 = 5680;

impl InitialMessage {
    pub async fn from(buffer: &mut Cursor<Vec<u8>>) -> Result<InitialMessage, ProtocolError> {
        let major = buffer.read_i16().await?;
        let minor = buffer.read_i16().await?;

        match major {
            VERSION_MAJOR_SPECIAL => match minor {
                VERSION_MINOR_CANCEL => Ok(InitialMessage::CancelRequest(
                    CancelRequest::from(buffer).await?,
                )),
                VERSION_MINOR_SSL => Ok(InitialMessage::SslRequest),
                VERSION_MINOR_GSSENC => Ok(InitialMessage::Gssenc),
                _ => Err(ErrorResponse::error(
                    ErrorCode::ProtocolViolation,
                    format!(
                        r#"Unsupported special version in initial message with code "{}""#,
                        minor
                    ),
                )
                .into()),
            },
            _ => {
                buffer.set_position(0);
                Ok(InitialMessage::Startup(StartupMessage::from(buffer).await?))
            }
        }
    }
}

impl Serialize for StartupMessage {
    const CODE: u8 = 0x00;

    fn serialize(&self) -> Option<Vec<u8>> {
        let mut buffer = Vec::with_capacity(DEFAULT_CAPACITY);
        buffer.put_i16(self.major);
        buffer.put_i16(self.minor);

        for (name, value) in &self.parameters {
            buffer::write_string(&mut buffer, &name);
            buffer::write_string(&mut buffer, &value);
        }

        buffer.push(0);

        Some(buffer)
    }
}

#[derive(Debug)]
pub struct NoticeResponse {
    // https://www.postgresql.org/docs/14/protocol-error-fields.html
    pub severity: NoticeSeverity,
    pub code: ErrorCode,
    pub message: String,
}

impl NoticeResponse {
    pub fn warning(code: ErrorCode, message: String) -> Self {
        Self {
            severity: NoticeSeverity::Warning,
            code,
            message,
        }
    }
}

impl Serialize for NoticeResponse {
    const CODE: u8 = b'N';

    fn serialize(&self) -> Option<Vec<u8>> {
        let mut buffer = Vec::with_capacity(DEFAULT_CAPACITY);

        let severity = self.severity.to_string();
        buffer.push(b'S');
        buffer::write_string(&mut buffer, &severity);

        buffer.push(b'C');
        buffer::write_string(&mut buffer, &self.code.to_string());

        buffer.push(b'M');
        buffer::write_string(&mut buffer, &self.message);
        buffer.push(0);

        Some(buffer)
    }
}

#[derive(thiserror::Error, Debug)]
pub struct ErrorResponse {
    // https://www.postgresql.org/docs/14/protocol-error-fields.html
    pub severity: ErrorSeverity,
    pub code: ErrorCode,
    pub message: String,
}

impl Display for ErrorResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ErrorResponse")
    }
}

impl ErrorResponse {
    pub fn new(severity: ErrorSeverity, code: ErrorCode, message: String) -> Self {
        Self {
            severity,
            code,
            message,
        }
    }

    pub fn error(code: ErrorCode, message: String) -> Self {
        Self {
            severity: ErrorSeverity::Error,
            code,
            message,
        }
    }

    pub fn fatal(code: ErrorCode, message: String) -> Self {
        Self {
            severity: ErrorSeverity::Fatal,
            code,
            message,
        }
    }

    pub fn query_canceled() -> Self {
        Self {
            severity: ErrorSeverity::Error,
            code: ErrorCode::QueryCanceled,
            message: "canceling statement due to user request".to_string(),
        }
    }
}

impl Serialize for ErrorResponse {
    const CODE: u8 = b'E';

    fn serialize(&self) -> Option<Vec<u8>> {
        let mut buffer = Vec::with_capacity(DEFAULT_CAPACITY);

        let severity = self.severity.to_string();
        buffer.push(b'S');
        buffer::write_string(&mut buffer, &severity);
        buffer.push(b'V');
        buffer::write_string(&mut buffer, &severity);
        buffer.push(b'C');
        buffer::write_string(&mut buffer, &self.code.to_string());
        buffer.push(b'M');
        buffer::write_string(&mut buffer, &self.message);
        buffer.push(0);

        Some(buffer)
    }
}

pub struct SSLResponse {}

impl SSLResponse {
    pub fn new() -> Self {
        Self {}
    }
}

impl Serialize for SSLResponse {
    const CODE: u8 = b'N';

    fn serialize(&self) -> Option<Vec<u8>> {
        None
    }
}

pub struct Authentication {
    response: AuthenticationRequest,
}

impl Authentication {
    pub fn new(response: AuthenticationRequest) -> Self {
        Self { response }
    }
}

impl Serialize for Authentication {
    const CODE: u8 = b'R';

    fn serialize(&self) -> Option<Vec<u8>> {
        Some(self.response.to_bytes())
    }
}

pub struct ReadyForQuery {
    transaction_status: TransactionStatus,
}

impl ReadyForQuery {
    pub fn new(transaction_status: TransactionStatus) -> Self {
        Self { transaction_status }
    }
}

impl Serialize for ReadyForQuery {
    const CODE: u8 = b'Z';

    fn serialize(&self) -> Option<Vec<u8>> {
        Some(vec![self.transaction_status.to_byte()])
    }
}

pub struct EmptyQuery {}

impl EmptyQuery {
    pub fn new() -> Self {
        Self {}
    }
}

impl Serialize for EmptyQuery {
    const CODE: u8 = b'I';

    fn serialize(&self) -> Option<Vec<u8>> {
        Some(vec![])
    }
}

pub struct BackendKeyData {
    process_id: i32,
    secret: i32,
}

impl BackendKeyData {
    pub fn new(process_id: i32, secret: i32) -> Self {
        Self { process_id, secret }
    }
}

impl Serialize for BackendKeyData {
    const CODE: u8 = b'K';

    fn serialize(&self) -> Option<Vec<u8>> {
        let mut buffer = Vec::with_capacity(4 + 4);
        buffer.put_i32(self.process_id);
        buffer.put_i32(self.secret);

        Some(buffer)
    }
}

/// (B) Alternative reply for Execute command before completing the execution of a portal (due to reaching a nonzero result-row count)
#[derive(Debug, PartialEq)]
pub struct PortalSuspended {}

impl PortalSuspended {
    pub fn new() -> Self {
        Self {}
    }
}

impl Serialize for PortalSuspended {
    const CODE: u8 = b's';

    fn serialize(&self) -> Option<Vec<u8>> {
        Some(vec![])
    }
}

pub struct ParameterStatus {
    name: String,
    value: String,
}

impl ParameterStatus {
    pub fn new(name: String, value: String) -> Self {
        Self { name, value }
    }
}

impl Serialize for ParameterStatus {
    const CODE: u8 = b'S';

    fn serialize(&self) -> Option<Vec<u8>> {
        let mut buffer = Vec::with_capacity(DEFAULT_CAPACITY);
        buffer::write_string(&mut buffer, &self.name);
        buffer::write_string(&mut buffer, &self.value);
        Some(buffer)
    }
}

/// (B) Success reply for Bind command.
pub struct BindComplete {}

impl BindComplete {
    pub fn new() -> Self {
        Self {}
    }
}

impl Serialize for BindComplete {
    const CODE: u8 = b'2';

    fn serialize(&self) -> Option<Vec<u8>> {
        // Use empty vec as workaround to write length
        Some(vec![])
    }
}

/// (B) Success reply for Close command.
pub struct CloseComplete {}

impl CloseComplete {
    pub fn new() -> Self {
        Self {}
    }
}

impl Serialize for CloseComplete {
    const CODE: u8 = b'3';

    fn serialize(&self) -> Option<Vec<u8>> {
        // Use empty vec as workaround to write length
        Some(vec![])
    }
}

/// (B) Success reply for Parse command.
#[derive(Debug)]
pub struct ParseComplete {}

impl ParseComplete {
    pub fn new() -> Self {
        Self {}
    }
}

impl Serialize for ParseComplete {
    const CODE: u8 = b'1';

    fn serialize(&self) -> Option<Vec<u8>> {
        // Use empty vec as workaround to write length
        Some(vec![])
    }
}

#[derive(Debug, PartialEq)]
pub enum PortalCompletion {
    Complete(CommandComplete),
    Suspended(PortalSuspended),
}

/// It's used to describe client that changes was done.
/// The command tag. This is usually a single word that identifies which SQL command was completed.
/// See more variants from sources: <https://github.com/postgres/postgres/blob/REL_14_4/src/include/tcop/cmdtaglist.h#L27>
#[derive(Debug, PartialEq)]
pub enum CommandComplete {
    Select(u32),
    Fetch(u32),
    Plain(String),
}

impl CommandComplete {
    pub fn new_selection(is_select: bool, rows: u32) -> Self {
        match is_select {
            true => CommandComplete::Select(rows),
            false => CommandComplete::Fetch(rows),
        }
    }
}

impl Serialize for CommandComplete {
    const CODE: u8 = b'C';

    fn serialize(&self) -> Option<Vec<u8>> {
        let mut buffer = Vec::with_capacity(DEFAULT_CAPACITY);
        match self {
            CommandComplete::Select(rows) => {
                buffer::write_string(&mut buffer, &format!("SELECT {}", rows))
            }
            CommandComplete::Fetch(rows) => {
                buffer::write_string(&mut buffer, &format!("FETCH {}", rows))
            }
            CommandComplete::Plain(tag) => buffer::write_string(&mut buffer, &tag),
        }

        Some(buffer)
    }
}

pub struct NoData {}

impl NoData {
    pub fn new() -> Self {
        Self {}
    }
}

impl Serialize for NoData {
    const CODE: u8 = b'n';

    fn serialize(&self) -> Option<Vec<u8>> {
        Some(vec![])
    }
}

pub struct EmptyQueryResponse {}

impl EmptyQueryResponse {
    pub fn new() -> Self {
        Self {}
    }
}

impl Serialize for EmptyQueryResponse {
    const CODE: u8 = b'I';

    fn serialize(&self) -> Option<Vec<u8>> {
        Some(vec![])
    }
}

#[derive(Debug, Clone)]
pub struct ParameterDescription {
    parameters: Vec<PgTypeId>,
}

impl ParameterDescription {
    pub fn new(parameters: Vec<PgTypeId>) -> Self {
        Self { parameters }
    }

    pub fn get(&self, i: usize) -> Option<&PgTypeId> {
        self.parameters.get(i)
    }
}

impl Serialize for ParameterDescription {
    const CODE: u8 = b't';

    fn serialize(&self) -> Option<Vec<u8>> {
        let mut buffer: Vec<u8> = Vec::with_capacity(6 * self.parameters.len());
        // FIXME!
        let size = i16::try_from(self.parameters.len()).unwrap();
        buffer.put_i16(size);

        for parameter in &self.parameters {
            buffer.put_i32(*parameter as i32);
        }

        Some(buffer)
    }
}

#[derive(Debug, Clone)]
pub struct RowDescription {
    fields: Vec<RowDescriptionField>,
}

impl RowDescription {
    pub fn new(fields: Vec<RowDescriptionField>) -> Self {
        Self { fields }
    }
    pub fn len(&self) -> usize {
        self.fields.len()
    }
}

impl Serialize for RowDescription {
    const CODE: u8 = b'T';

    fn serialize(&self) -> Option<Vec<u8>> {
        // FIXME!
        let size = u16::try_from(self.fields.len()).unwrap();
        let mut buffer = Vec::with_capacity(DEFAULT_CAPACITY);
        buffer.extend_from_slice(&size.to_be_bytes());

        for field in self.fields.iter() {
            buffer::write_string(&mut buffer, &field.name);
            buffer.extend_from_slice(&field.table_oid.to_be_bytes());
            buffer.extend_from_slice(&field.attribute_number.to_be_bytes());
            buffer.extend_from_slice(&field.data_type_oid.to_be_bytes());
            buffer.extend_from_slice(&field.data_type_size.to_be_bytes());
            buffer.extend_from_slice(&field.type_modifier.to_be_bytes());
            buffer.extend_from_slice(&(field.format as i16).to_be_bytes());
        }

        Some(buffer)
    }
}

#[derive(Debug, Clone)]
pub struct RowDescriptionField {
    name: String,
    /// If the field can be identified as a column of a specific table, the object ID of the table; otherwise zero.
    table_oid: i32,
    /// If the field can be identified as a column of a specific table, the attribute number of the column; otherwise zero.
    attribute_number: i16,
    // The object ID of the field's data type. PgTypeId
    data_type_oid: i32,
    /// The data type size (see pg_type.typlen). Note that negative values denote variable-width types.
    data_type_size: i16,
    /// The type modifier (see pg_attribute.atttypmod). The meaning of the modifier is type-specific.
    /// select attrelid, attname, atttypmod from pg_attribute;
    type_modifier: i32,
    /// The format code being used for the field. It depends on the client request and binary ecconding for specific type
    format: Format,
}

impl RowDescriptionField {
    pub fn new(name: String, typ: &PgType, format: Format) -> Self {
        Self {
            name,
            // TODO: REWORK!
            table_oid: 0,
            // TODO: REWORK!
            attribute_number: 0,
            data_type_oid: typ.oid as i32,
            data_type_size: typ.typlen,
            type_modifier: -1,
            format: if format == Format::Binary && typ.is_binary_supported() {
                Format::Binary
            } else {
                Format::Text
            },
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct PasswordMessage {
    pub password: String,
}

#[async_trait]
impl Deserialize for PasswordMessage {
    async fn deserialize(mut buffer: Cursor<Vec<u8>>) -> Result<Self, ProtocolError>
    where
        Self: Sized,
    {
        Ok(Self {
            password: buffer::read_string(&mut buffer).await?,
        })
    }
}

/// (F) Extended Query. Contains a textual query string, optionally some information about data
/// types of parameter placeholders, and the name of a destination prepared-statement object
/// (an empty string selects the unnamed prepared statement)
///
/// The response is either ParseComplete or ErrorResponse.
#[derive(Debug, PartialEq)]
pub struct Parse {
    /// The name of the prepared statement. Empty string is used for unamed statements
    pub name: String,
    /// SQL query with placeholders ($1)
    pub query: String,
    // Types for parameters
    pub param_types: Vec<u32>,
}

#[async_trait]
impl Deserialize for Parse {
    async fn deserialize(mut buffer: Cursor<Vec<u8>>) -> Result<Self, ProtocolError>
    where
        Self: Sized,
    {
        let name = buffer::read_string(&mut buffer).await?;
        let query = buffer::read_string(&mut buffer).await?;

        let total = buffer.read_i16().await?;
        let mut param_types = Vec::with_capacity(total as usize);

        for _ in 0..total {
            param_types.push(buffer.read_u32().await?);
        }

        Ok(Self {
            name,
            query,
            param_types,
        })
    }
}

/// (F) Extended Query. The Execute message specifies the portal name (empty string denotes the unnamed portal) and a maximum result-row count (zero meaning “fetch all rows”).
#[derive(Debug, PartialEq)]
pub struct Execute {
    // The name of the portal to execute (an empty string selects the unnamed portal).
    pub portal: String,
    // Maximum number of rows to return, if portal contains a query that returns rows (ignored otherwise). Zero denotes “no limit”.
    pub max_rows: i32,
}

#[async_trait]
impl Deserialize for Execute {
    async fn deserialize(mut buffer: Cursor<Vec<u8>>) -> Result<Self, ProtocolError>
    where
        Self: Sized,
    {
        let portal = buffer::read_string(&mut buffer).await?;
        let max_rows = buffer.read_i32().await?;

        Ok(Self { portal, max_rows })
    }
}

#[derive(Debug, PartialEq)]
pub enum CloseType {
    Statement,
    Portal,
}

#[derive(Debug, PartialEq)]
pub struct Close {
    pub typ: CloseType,
    // The name of the prepared statement or portal to close (an empty string selects the unnamed prepared statement or portal).
    pub name: String,
}

#[async_trait]
impl Deserialize for Close {
    async fn deserialize(mut buffer: Cursor<Vec<u8>>) -> Result<Self, ProtocolError>
    where
        Self: Sized,
    {
        let typ = match buffer.read_u8().await? {
            b'S' => CloseType::Statement,
            b'P' => CloseType::Portal,
            code => {
                return Err(ErrorResponse::error(
                    ErrorCode::ProtocolViolation,
                    format!("Unknown close code: {}", code),
                )
                .into());
            }
        };

        let name = buffer::read_string(&mut buffer).await?;

        Ok(Self { typ, name })
    }
}

/// (F) Extended Query.
#[derive(Debug, PartialEq)]
pub struct Bind {
    /// The name of the destination portal (an empty string selects the unnamed portal).
    pub portal: String,
    /// The name of the source prepared statement (an empty string selects the unnamed prepared statement).
    pub statement: String,
    /// Format for parameters
    pub parameter_formats: Vec<Format>,
    /// Raw values for parameters
    pub parameter_values: Vec<Option<Vec<u8>>>,
    /// Format for results
    pub result_formats: Vec<Format>,
}

impl Bind {
    pub fn to_bind_values(
        &self,
        description: &ParameterDescription,
    ) -> Result<Vec<BindValue>, ProtocolError> {
        let mut values = Vec::with_capacity(self.parameter_values.len());

        for (idx, raw_value) in self.parameter_values.iter().enumerate() {
            let param_tid = description.get(idx).ok_or::<ProtocolError>({
                ErrorResponse::error(
                    ErrorCode::InternalError,
                    format!("Unknown type for parameter: {}", idx),
                )
                .into()
            })?;

            let param_format = match self.parameter_formats.len() {
                0 => Format::Text,
                1 => self.parameter_formats[0],
                _ => self.parameter_formats[idx],
            };

            values.push(match raw_value {
                None => BindValue::Null,
                Some(raw_value) => match param_tid {
                    PgTypeId::TEXT => {
                        BindValue::String(String::from_protocol(raw_value, param_format)?)
                    }
                    PgTypeId::INT8 => {
                        BindValue::Int64(i64::from_protocol(raw_value, param_format)?)
                    }
                    _ => {
                        return Err(ErrorResponse::error(
                            ErrorCode::FeatureNotSupported,
                            format!(
                                r#"Type "{:?}" is not supported for parameters decoding"#,
                                param_tid
                            ),
                        )
                        .into())
                    }
                },
            })
        }

        Ok(values)
    }
}

#[async_trait]
impl Deserialize for Bind {
    async fn deserialize(mut buffer: Cursor<Vec<u8>>) -> Result<Self, ProtocolError>
    where
        Self: Sized,
    {
        let portal = buffer::read_string(&mut buffer).await?;
        let statement = buffer::read_string(&mut buffer).await?;

        let mut parameter_formats = Vec::new();
        {
            let total = buffer.read_i16().await?;
            for _ in 0..total {
                parameter_formats.push(buffer::read_format(&mut buffer).await?);
            }
        }

        let mut parameter_values = Vec::new();
        {
            let total = buffer.read_i16().await?;
            for _ in 0..total {
                let len = buffer.read_i32().await?;
                if len == -1 {
                    parameter_values.push(None);
                } else {
                    let mut value = Vec::with_capacity(len as usize);
                    for _ in 0..len {
                        value.push(buffer.read_u8().await?);
                    }

                    parameter_values.push(Some(value));
                }
            }
        }

        let mut result_formats = Vec::new();
        {
            let total = buffer.read_i16().await?;

            for _ in 0..total {
                result_formats.push(buffer::read_format(&mut buffer).await?);
            }
        }

        Ok(Self {
            portal,
            statement,
            parameter_formats,
            parameter_values,
            result_formats,
        })
    }
}

#[derive(Debug, PartialEq)]
pub enum DescribeType {
    Statement,
    Portal,
}

// (F) Extended Query.
#[derive(Debug, PartialEq)]
pub struct Describe {
    pub typ: DescribeType,
    pub name: String,
}

#[async_trait]
impl Deserialize for Describe {
    async fn deserialize(mut buffer: Cursor<Vec<u8>>) -> Result<Self, ProtocolError>
    where
        Self: Sized,
    {
        let typ = match buffer.read_u8().await? {
            b'S' => DescribeType::Statement,
            b'P' => DescribeType::Portal,
            code => {
                return Err(ErrorResponse::error(
                    ErrorCode::ProtocolViolation,
                    format!("Unknown describe code: {}", code),
                )
                .into());
            }
        };
        let name = buffer::read_string(&mut buffer).await?;

        Ok(Self { typ, name })
    }
}

#[derive(Debug, PartialEq)]
pub struct Query {
    pub query: String,
}

#[async_trait]
impl Deserialize for Query {
    async fn deserialize(mut buffer: Cursor<Vec<u8>>) -> Result<Self, ProtocolError>
    where
        Self: Sized,
    {
        Ok(Self {
            query: buffer::read_string(&mut buffer).await?,
        })
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
#[repr(u8)]
pub enum Format {
    Text,
    Binary,
}

/// All frontend messages (request which client sends to the server).
#[derive(Debug, PartialEq)]
pub enum FrontendMessage {
    PasswordMessage(PasswordMessage),
    /// Simple Query
    Query(Query),
    /// Flush network buffer
    Flush,
    /// Close connection
    Terminate,
    /// Sync primitive in Extended Query for error recovery.
    Sync,
    /// Extended Query. Create Statement.
    Parse(Parse),
    /// Extended Query. Creating Portal from specific Statement by replacing placeholders by real values.
    Bind(Bind),
    /// Extended Query. Describe Portal/Statement
    Describe(Describe),
    /// Extended Query. Select n rows from existed Portal
    Execute(Execute),
    /// Extended Query. Close Portal/Statement
    Close(Close),
}

/// <https://www.postgresql.org/docs/14/errcodes-appendix.html>
#[derive(Debug)]
#[allow(dead_code)]
pub enum ErrorCode {
    // 0A — Feature Not Supported
    FeatureNotSupported,
    // 8 -  Connection Exception
    ProtocolViolation,
    // 28 - Invalid Authorization Specification
    InvalidAuthorizationSpecification,
    InvalidPassword,
    // 22
    DataException,
    // Class 25 — Invalid Transaction State
    ActiveSqlTransaction,
    NoActiveSqlTransaction,
    // 26
    InvalidSqlStatement,
    // 34
    InvalidCursorName,
    // Class 42 — Syntax Error or Access Rule Violation
    DuplicateCursor,
    SyntaxError,
    // Class 53 — Insufficient Resources
    ConfigurationLimitExceeded,
    // Class 55 — Object Not In Prerequisite State
    ObjectNotInPrerequisiteState,
    // Class 57 - Operator Intervention
    QueryCanceled,
    // XX - Internal Error
    InternalError,
}

impl Display for ErrorCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let string = match self {
            Self::FeatureNotSupported => "0A000",
            Self::ProtocolViolation => "08P01",
            Self::InvalidAuthorizationSpecification => "28000",
            Self::InvalidPassword => "28P01",
            Self::DataException => "22000",
            Self::ActiveSqlTransaction => "25001",
            Self::NoActiveSqlTransaction => "25P01",
            Self::InvalidSqlStatement => "26000",
            Self::InvalidCursorName => "34000",
            Self::DuplicateCursor => "42P03",
            Self::SyntaxError => "42601",
            Self::ConfigurationLimitExceeded => "53400",
            Self::ObjectNotInPrerequisiteState => "55000",
            Self::QueryCanceled => "57014",
            Self::InternalError => "XX000",
        };
        write!(f, "{}", string)
    }
}

#[derive(Debug)]
pub enum NoticeSeverity {
    // https://www.postgresql.org/docs/14/protocol-error-fields.html
    Warning,
    Notice,
    Debug,
    Info,
    Log,
}

impl Display for NoticeSeverity {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let string = match self {
            Self::Warning => "WARNING",
            Self::Notice => "NOTICE",
            Self::Debug => "DEBUG",
            Self::Info => "INFO",
            Self::Log => "LOG",
        };
        write!(f, "{}", string)
    }
}

#[derive(Debug)]
pub enum ErrorSeverity {
    // https://www.postgresql.org/docs/14/protocol-error-fields.html
    Error,
    Fatal,
    Panic,
}

impl Display for ErrorSeverity {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let string = match self {
            Self::Error => "ERROR",
            Self::Fatal => "FATAL",
            Self::Panic => "PANIC",
        };
        write!(f, "{}", string)
    }
}

pub enum TransactionStatus {
    Idle,
    InTransactionBlock,
    // InFailedTransactionBlock,
}

impl TransactionStatus {
    pub fn to_byte(&self) -> u8 {
        match self {
            Self::Idle => b'I',
            Self::InTransactionBlock => b'T',
            // Self::InFailedTransactionBlock => b'E',
        }
    }
}

pub enum AuthenticationRequest {
    Ok,
    CleartextPassword,
}

impl AuthenticationRequest {
    pub fn to_bytes(&self) -> Vec<u8> {
        self.to_code().to_be_bytes().to_vec()
    }

    pub fn to_code(&self) -> i32 {
        match self {
            Self::Ok => 0,
            Self::CleartextPassword => 3,
        }
    }
}

pub trait Serialize {
    const CODE: u8;

    fn serialize(&self) -> Option<Vec<u8>>;

    fn code(&self) -> u8 {
        Self::CODE
    }
}

#[async_trait]
pub trait Deserialize {
    async fn deserialize(mut buffer: Cursor<Vec<u8>>) -> Result<Self, ProtocolError>
    where
        Self: Sized;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{read_message, ProtocolError};

    use std::io::Cursor;

    fn parse_hex_dump(input: String) -> Vec<u8> {
        let mut result: Vec<Vec<u8>> = vec![];

        for line in input.trim().split("\n") {
            let splitted = line.trim().split("   ").collect::<Vec<&str>>();
            let row = splitted.first().unwrap().to_string().replace(" ", "");

            let tmp = hex::decode(row).unwrap();
            result.push(tmp);
        }

        result.concat()
    }

    #[tokio::test]
    async fn test_startup_message_duplex() -> Result<(), ProtocolError> {
        // 00 00 00 4c 00 03 00 00 75 73 65 72 00 74 65 73   ...L....user.tes
        // 74 00 64 61 74 61 62 61 73 65 00 74 65 73 74 00   t.database.test.
        // 61 70 70 6c 69 63 61 74 69 6f 6e 5f 6e 61 6d 65   application_name
        // 00 70 73 71 6c 00 63 6c 69 65 6e 74 5f 65 6e 63   .psql.client_enc
        // 6f 64 69 6e 67 00 55 54 46 38 00 00               oding.UTF8..

        let expected_message = {
            let mut parameters = HashMap::new();
            parameters.insert("database".to_string(), "test".to_string());
            parameters.insert("application_name".to_string(), "psql".to_string());
            parameters.insert("user".to_string(), "test".to_string());
            parameters.insert("client_encoding".to_string(), "UTF8".to_string());

            StartupMessage {
                major: 3,
                minor: 0,
                parameters,
            }
        };

        // First step, We write struct to the buffer
        let mut cursor = Cursor::new(vec![]);
        buffer::write_message(&mut cursor, expected_message.clone()).await?;

        // Second step, We read form the buffer and output structure must be the same as original
        let buffer = cursor.get_ref()[..].to_vec();
        let mut cursor = Cursor::new(buffer);
        // skipping length
        cursor.read_u32().await?;

        let actual_message = StartupMessage::from(&mut cursor).await?;
        assert_eq!(actual_message, expected_message);

        Ok(())
    }

    #[tokio::test]
    async fn test_frontend_message_parse_parse() -> Result<(), ProtocolError> {
        let buffer = parse_hex_dump(
            r#"
            50 00 00 00 77 6e 61 6d 65 64 2d 73 74 6d 74 00   P...wnamed-stmt.
            0a 20 20 20 20 20 20 53 45 4c 45 43 54 20 6e 75   .      SELECT nu
            6d 2c 20 73 74 72 2c 20 62 6f 6f 6c 0a 20 20 20   m, str, bool.
            20 20 20 46 52 4f 4d 20 74 65 73 74 64 61 74 61      FROM testdata
            0a 20 20 20 20 20 20 57 48 45 52 45 20 6e 75 6d   .      WHERE num
            20 3d 20 24 31 20 41 4e 44 20 73 74 72 20 3d 20    = $1 AND str =
            24 32 20 41 4e 44 20 62 6f 6f 6c 20 3d 20 24 33   $2 AND bool = $3
            0a 20 20 20 20 00 00 00                           .    ...
            "#
            .to_string(),
        );
        let mut cursor = Cursor::new(buffer);

        let message = read_message(&mut cursor).await?;
        match message {
            FrontendMessage::Parse(parse) => {
                assert_eq!(
                    parse,
                    Parse {
                        name: "named-stmt".to_string(),
                        query: "\n      SELECT num, str, bool\n      FROM testdata\n      WHERE num = $1 AND str = $2 AND bool = $3\n    ".to_string(),
                        param_types: vec![],
                    },
                )
            }
            _ => panic!("Wrong message, must be Parse"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_frontend_message_parse_bind_variant1() -> Result<(), ProtocolError> {
        let buffer = parse_hex_dump(
            r#"
            42 00 00 00 2d 00 6e 61 6d 65 64 2d 73 74 6d 74   B...-.named-stmt
            00 00 00 00 03 00 00 00 01 35 00 00 00 04 74 65   .........5....te
            73 74 00 00 00 04 74 72 75 65 00 01 00 00         st....true....
            "#
            .to_string(),
        );
        let mut cursor = Cursor::new(buffer);

        let message = read_message(&mut cursor).await?;
        match message {
            FrontendMessage::Bind(bind) => {
                assert_eq!(
                    bind,
                    Bind {
                        portal: "".to_string(),
                        statement: "named-stmt".to_string(),
                        parameter_formats: vec![],
                        parameter_values: vec![
                            Some(vec![53]),
                            Some(vec![116, 101, 115, 116]),
                            Some(vec![116, 114, 117, 101]),
                        ],
                        result_formats: vec![Format::Text]
                    },
                );
            }
            _ => panic!("Wrong message, must be Bind"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_frontend_message_parse_bind_variant2() -> Result<(), ProtocolError> {
        let buffer = parse_hex_dump(
            r#"
            42 00 00 00 1a 00 73 30 00 00 01 00 01 00 01 00   B.....s0........
            00 00 04 74 65 73 74 00 01 00 01                  ...test....
            "#
            .to_string(),
        );
        let mut cursor = Cursor::new(buffer);

        let message = read_message(&mut cursor).await?;
        match message {
            FrontendMessage::Bind(body) => {
                assert_eq!(
                    body,
                    Bind {
                        portal: "".to_string(),
                        statement: "s0".to_string(),
                        parameter_formats: vec![Format::Binary],
                        parameter_values: vec![Some(vec![116, 101, 115, 116])],
                        result_formats: vec![Format::Binary]
                    },
                );

                assert_eq!(
                    body.to_bind_values(&ParameterDescription::new(vec![PgTypeId::TEXT]))
                        .unwrap(),
                    vec![BindValue::String("test".to_string())]
                );
            }
            _ => panic!("Wrong message, must be Bind"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_frontend_message_parse_describe() -> Result<(), ProtocolError> {
        let buffer = parse_hex_dump(
            r#"
            44 00 00 00 08 53 73 30 00                        D....Ss0.
            "#
            .to_string(),
        );
        let mut cursor = Cursor::new(buffer);

        let message = read_message(&mut cursor).await?;
        match message {
            FrontendMessage::Describe(desc) => {
                assert_eq!(
                    desc,
                    Describe {
                        typ: DescribeType::Statement,
                        name: "s0".to_string(),
                    },
                )
            }
            _ => panic!("Wrong message, must be Describe"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_frontend_message_parse_password_message() -> Result<(), ProtocolError> {
        let buffer = parse_hex_dump(
            r#"
            70 00 00 00 09 74 65 73 74 00                     p....test.
            "#
            .to_string(),
        );
        let mut cursor = Cursor::new(buffer);

        let message = read_message(&mut cursor).await?;
        match message {
            FrontendMessage::PasswordMessage(body) => {
                assert_eq!(
                    body,
                    PasswordMessage {
                        password: "test".to_string()
                    },
                )
            }
            _ => panic!("Wrong message, must be Describe"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_frontend_message_execute() -> Result<(), ProtocolError> {
        let buffer = parse_hex_dump(
            r#"
            45 00 00 00 09 00 00 00 00 00                     E.........
            "#
            .to_string(),
        );
        let mut cursor = Cursor::new(buffer);

        let message = read_message(&mut cursor).await?;
        match message {
            FrontendMessage::Execute(body) => {
                assert_eq!(
                    body,
                    Execute {
                        portal: "".to_string(),
                        max_rows: 0
                    },
                )
            }
            _ => panic!("Wrong message, must be Describe"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_frontend_message_parse_sequence_sync() -> Result<(), ProtocolError> {
        let buffer = parse_hex_dump(
            r#"
            53 00 00 00 04                                    S....
            53 00 00 00 04                                    S....
            "#
            .to_string(),
        );
        let mut cursor = Cursor::new(buffer);

        // This test demonstrates that protocol can decode two
        // simple messages without body in sequence
        read_message(&mut cursor).await?;
        read_message(&mut cursor).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_frontend_message_write_complete_parse() -> Result<(), ProtocolError> {
        let mut cursor = Cursor::new(vec![]);

        buffer::write_message(&mut cursor, ParseComplete {}).await?;

        assert_eq!(cursor.get_ref()[0..], vec![49, 0, 0, 0, 4]);

        Ok(())
    }

    #[tokio::test]
    async fn test_frontend_message_write_row_description() -> Result<(), ProtocolError> {
        let mut cursor = Cursor::new(vec![]);
        let desc = RowDescription::new(vec![
            RowDescriptionField::new(
                "num".to_string(),
                PgType::get_by_tid(PgTypeId::INT8),
                Format::Text,
            ),
            RowDescriptionField::new(
                "str".to_string(),
                PgType::get_by_tid(PgTypeId::INT8),
                Format::Text,
            ),
            RowDescriptionField::new(
                "bool".to_string(),
                PgType::get_by_tid(PgTypeId::INT8),
                Format::Text,
            ),
        ]);
        buffer::write_message(&mut cursor, desc).await?;

        assert_eq!(
            cursor.get_ref()[0..],
            vec![
                84, 0, 0, 0, 73, 0, 3, 110, 117, 109, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 20, 0, 8, 255,
                255, 255, 255, 0, 0, 115, 116, 114, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 20, 0, 8, 255,
                255, 255, 255, 0, 0, 98, 111, 111, 108, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 20, 0, 8,
                255, 255, 255, 255, 0, 0
            ]
        );

        Ok(())
    }
}
