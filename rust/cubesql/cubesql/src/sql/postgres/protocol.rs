use std::{
    collections::HashMap,
    convert::TryFrom,
    fmt::{self, Display, Formatter},
    io::{Cursor, Error},
    marker::Send,
};

use async_trait::async_trait;
use tokio::io::AsyncReadExt;

use super::buffer;

const DEFAULT_CAPACITY: usize = 64;

pub const SSL_REQUEST_PROTOCOL: u16 = 1234;

pub struct StartupMessage {
    pub protocol_version: ProtocolVersion,
    pub parameters: HashMap<String, String>,
}

impl StartupMessage {
    pub async fn from(mut buffer: &mut Cursor<Vec<u8>>) -> Result<Self, Error> {
        let major_protocol_version = buffer.read_u16().await?;
        let minor_protocol_version = buffer.read_u16().await?;
        let protocol_version = ProtocolVersion::new(major_protocol_version, minor_protocol_version);

        let mut parameters = HashMap::new();

        if major_protocol_version != SSL_REQUEST_PROTOCOL {
            loop {
                let name = buffer::read_string(&mut buffer).await?;
                if name.is_empty() {
                    break;
                }
                let value = buffer::read_string(&mut buffer).await?;
                parameters.insert(name, value);
            }
        }

        Ok(Self {
            protocol_version,
            parameters,
        })
    }
}

pub struct ErrorResponse {
    // https://www.postgresql.org/docs/14/protocol-error-fields.html
    pub severity: ErrorSeverity,
    pub code: ErrorCode,
    pub message: String,
}

impl ErrorResponse {
    pub fn new(severity: ErrorSeverity, code: ErrorCode, message: String) -> Self {
        Self {
            severity,
            code,
            message,
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

pub struct CommandComplete {
    tag: CommandCompleteTag,
    rows: u32,
}

impl CommandComplete {
    pub fn new(tag: CommandCompleteTag, rows: u32) -> Self {
        Self { tag, rows }
    }
}

impl Serialize for CommandComplete {
    const CODE: u8 = b'C';

    fn serialize(&self) -> Option<Vec<u8>> {
        let string = format!("{} {}", self.tag, self.rows);
        let mut buffer = Vec::with_capacity(DEFAULT_CAPACITY);
        buffer::write_string(&mut buffer, &string);
        Some(buffer)
    }
}

pub struct RowDescription {
    fields: Vec<RowDescriptionField>,
}

impl RowDescription {
    pub fn new(fields: Vec<RowDescriptionField>) -> Self {
        Self { fields }
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
            buffer.extend_from_slice(&field.format_code.to_be_bytes());
        }
        Some(buffer)
    }
}

pub struct RowDescriptionField {
    name: String,
    // TODO: REWORK!
    table_oid: i32,
    attribute_number: i16,
    data_type_oid: i32,
    data_type_size: i16,
    type_modifier: i32,
    format_code: i16,
}

impl RowDescriptionField {
    pub fn new(name: String) -> Self {
        Self {
            name,
            table_oid: 0,
            attribute_number: 0,
            data_type_oid: 25,
            data_type_size: -1,
            type_modifier: -1,
            format_code: 0,
        }
    }
}

pub struct DataRow {
    values: Vec<Option<String>>,
}

impl DataRow {
    pub fn new(values: Vec<Option<String>>) -> Self {
        Self { values }
    }
}

impl Serialize for DataRow {
    const CODE: u8 = b'D';

    fn serialize(&self) -> Option<Vec<u8>> {
        let mut buffer = Vec::with_capacity(DEFAULT_CAPACITY);
        let size = u16::try_from(self.values.len()).unwrap();
        buffer.extend_from_slice(&size.to_be_bytes());
        for value in self.values.iter() {
            match value {
                None => buffer.extend_from_slice(&(-1_i32).to_be_bytes()),
                Some(value) => {
                    let size = u32::try_from(value.len()).unwrap();
                    buffer.extend_from_slice(&size.to_be_bytes());
                    buffer.extend_from_slice(value.as_bytes());
                }
            };
        }
        Some(buffer)
    }
}

pub struct PasswordMessage {
    pub password: String,
}

#[async_trait]
impl Deserialize for PasswordMessage {
    async fn deserialize(mut buffer: Cursor<Vec<u8>>) -> Result<Self, Error>
    where
        Self: Sized,
    {
        Ok(Self {
            password: buffer::read_string(&mut buffer).await?,
        })
    }
}

pub struct Query {
    pub query: String,
}

#[async_trait]
impl Deserialize for Query {
    async fn deserialize(mut buffer: Cursor<Vec<u8>>) -> Result<Self, Error>
    where
        Self: Sized,
    {
        Ok(Self {
            query: buffer::read_string(&mut buffer).await?,
        })
    }
}

pub struct ProtocolVersion {
    pub major: u16,
    pub minor: u16,
}

impl ProtocolVersion {
    pub fn new(major: u16, minor: u16) -> Self {
        Self { major, minor }
    }
}

pub enum FrontendMessage {
    PasswordMessage(PasswordMessage),
    Query(Query),
    Terminate,
}

pub enum ErrorCode {
    // https://www.postgresql.org/docs/14/errcodes-appendix.html

    // 0A - Feature Not Supported
    FeatureNotSupported,
    // 28 - Invalid Authorization Specification
    InvalidAuthorizationSpecification,
    InvalidPassword,
    // XX - Internal Error
    InternalError,
}

impl Display for ErrorCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let string = match self {
            Self::FeatureNotSupported => "0A000",

            Self::InvalidAuthorizationSpecification => "28000",
            Self::InvalidPassword => "28P01",

            Self::InternalError => "XX000",
        };
        write!(f, "{}", string)
    }
}

pub enum ErrorSeverity {
    // https://www.postgresql.org/docs/14/protocol-error-fields.html
    Error,
    Fatal,
    // Panic,
}

impl Display for ErrorSeverity {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let string = match self {
            Self::Error => "ERROR",
            Self::Fatal => "FATAL",
            // Self::Panic => "PANIC",
        };
        write!(f, "{}", string)
    }
}

pub enum TransactionStatus {
    Idle,
    // InTransactionBlock,
    // InFailedTransactionBlock,
}

impl TransactionStatus {
    pub fn to_byte(&self) -> u8 {
        match self {
            Self::Idle => b'I',
            // Self::InTransactionBlock => b'T',
            // Self::InFailedTransactionBlock => b'E',
        }
    }
}

pub enum CommandCompleteTag {
    Select,
}

impl Display for CommandCompleteTag {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let string = match self {
            Self::Select => "SELECT",
        };
        write!(f, "{}", string)
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

    pub fn to_code(&self) -> u32 {
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
    async fn deserialize(mut buffer: Cursor<Vec<u8>>) -> Result<Self, Error>
    where
        Self: Sized;

    async fn read_from<Reader: AsyncReadExt + Unpin + Send>(
        mut reader: &mut Reader,
    ) -> Result<Self, Error>
    where
        Self: Sized,
    {
        let cursor = buffer::read_contents(&mut reader).await?;
        Ok(Self::deserialize(cursor).await?)
    }
}
