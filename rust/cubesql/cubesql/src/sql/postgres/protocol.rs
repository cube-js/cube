use std::{
    collections::HashMap,
    convert::TryFrom,
    fmt::{self, Display, Formatter},
    io::{Cursor, Error},
};

use async_trait::async_trait;

use bytes::BufMut;

use crate::sql::statement::BindValue;
use tokio::io::AsyncReadExt;

use super::{buffer, PgType, PgTypeId};

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

#[derive(Debug, Clone)]
pub struct ParameterDescription {
    parameters: Vec<PgTypeId>,
}

impl ParameterDescription {
    pub fn new(parameters: Vec<PgTypeId>) -> Self {
        Self { parameters }
    }
}

impl Serialize for ParameterDescription {
    const CODE: u8 = b't';

    fn serialize(&self) -> Option<Vec<u8>> {
        let mut buffer: Vec<u8> = vec![];
        // FIXME!
        let size = i16::try_from(self.parameters.len()).unwrap();
        buffer.put_i16(size);

        for parameter in &self.parameters {
            buffer.put_i32((*parameter as u32) as i32);
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
            buffer.extend_from_slice(&0_i16.to_be_bytes());
        }

        Some(buffer)
    }
}

#[derive(Debug, Clone)]
pub struct RowDescriptionField {
    name: String,
    // TODO: REWORK!
    table_oid: i32,
    attribute_number: i16,
    data_type_oid: i32,
    data_type_size: i16,
    type_modifier: i32,
}

impl RowDescriptionField {
    pub fn new(name: String, typ: &PgType) -> Self {
        Self {
            name,
            table_oid: 0,
            attribute_number: 0,
            data_type_oid: typ.oid as i32,
            data_type_size: typ.typlen,
            type_modifier: -1,
        }
    }
}

#[derive(Debug, PartialEq)]
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

/// This command is used for prepared statement creation on the server side
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
    async fn deserialize(mut buffer: Cursor<Vec<u8>>) -> Result<Self, Error>
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

#[derive(Debug, PartialEq)]
pub struct Execute {
    // The name of the portal to execute (an empty string selects the unnamed portal).
    pub portal: String,
    // Maximum number of rows to return, if portal contains a query that returns rows (ignored otherwise). Zero denotes “no limit”.
    pub max_rows: i32,
}

#[async_trait]
impl Deserialize for Execute {
    async fn deserialize(mut buffer: Cursor<Vec<u8>>) -> Result<Self, Error>
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
    async fn deserialize(mut buffer: Cursor<Vec<u8>>) -> Result<Self, Error>
    where
        Self: Sized,
    {
        let typ = match buffer.read_u8().await? {
            b'S' => CloseType::Statement,
            b'P' => CloseType::Portal,
            t => {
                return Err(Error::new(
                    std::io::ErrorKind::Other,
                    format!("Unknown describe code: {}", t),
                ));
            }
        };

        let name = buffer::read_string(&mut buffer).await?;

        Ok(Self { typ, name })
    }
}

/// This command is used for prepared statement creation on the server side
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
    pub(crate) fn to_bind_values(&self) -> Vec<BindValue> {
        let mut values = vec![];

        for param_value in &self.parameter_values {
            values.push(match param_value {
                None => BindValue::Null,
                Some(raw_value) => {
                    let decoded = String::from_utf8(raw_value.clone())
                        .expect("Unable to unpack raw parameter to string");

                    BindValue::String(decoded)
                }
            })
        }

        values
    }
}

#[async_trait]
impl Deserialize for Bind {
    async fn deserialize(mut buffer: Cursor<Vec<u8>>) -> Result<Self, Error>
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

#[derive(Debug, PartialEq)]
pub struct Describe {
    pub typ: DescribeType,
    pub name: String,
}

#[async_trait]
impl Deserialize for Describe {
    async fn deserialize(mut buffer: Cursor<Vec<u8>>) -> Result<Self, Error>
    where
        Self: Sized,
    {
        let typ = match buffer.read_u8().await? {
            b'S' => DescribeType::Statement,
            b'P' => DescribeType::Portal,
            t => {
                return Err(Error::new(
                    std::io::ErrorKind::Other,
                    format!("Unknown describe code: {}", t),
                ));
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
    async fn deserialize(mut buffer: Cursor<Vec<u8>>) -> Result<Self, Error>
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

#[derive(Debug, PartialEq)]
pub struct ProtocolVersion {
    pub major: u16,
    pub minor: u16,
}

impl ProtocolVersion {
    pub fn new(major: u16, minor: u16) -> Self {
        Self { major, minor }
    }
}

#[derive(Debug, PartialEq)]
pub enum FrontendMessage {
    PasswordMessage(PasswordMessage),
    Query(Query),
    Parse(Parse),
    Bind(Bind),
    Describe(Describe),
    Execute(Execute),
    Close(Close),
    /// Close connection
    Terminate,
    /// Finish
    Sync,
}

/// https://www.postgresql.org/docs/14/errcodes-appendix.html
pub enum ErrorCode {
    // 0A — Feature Not Supported
    FeatureNotSupported,
    // 28 - Invalid Authorization Specification
    InvalidAuthorizationSpecification,
    InvalidPassword,
    // 26
    InvalidSqlStatement,
    // XX - Internal Error
    InternalError,
}

impl Display for ErrorCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let string = match self {
            Self::FeatureNotSupported => "0A000",
            Self::InvalidAuthorizationSpecification => "28000",
            Self::InvalidPassword => "28P01",
            Self::InvalidSqlStatement => "26000",
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
}

#[cfg(test)]
mod tests {
    use crate::{
        sql::{postgres::buffer::read_message, PgTypeId},
        CubeError,
    };
    use std::io::Cursor;

    use super::*;

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
    async fn test_frontend_message_parse_parse() -> Result<(), CubeError> {
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
    async fn test_frontend_message_parse_bind_variant1() -> Result<(), CubeError> {
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
                )
            }
            _ => panic!("Wrong message, must be Bind"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_frontend_message_parse_bind_variant2() -> Result<(), CubeError> {
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
                )
            }
            _ => panic!("Wrong message, must be Bind"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_frontend_message_parse_describe() -> Result<(), CubeError> {
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
    async fn test_frontend_message_parse_password_message() -> Result<(), CubeError> {
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
    async fn test_frontend_message_execute() -> Result<(), CubeError> {
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
    async fn test_frontend_message_parse_sequence_sync() -> Result<(), CubeError> {
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
    async fn test_frontend_message_write_complete_parse() -> Result<(), CubeError> {
        let mut cursor = Cursor::new(vec![]);

        buffer::write_message(&mut cursor, ParseComplete {}).await?;

        assert_eq!(cursor.get_ref()[0..], vec![49, 0, 0, 0, 4]);

        Ok(())
    }

    #[tokio::test]
    async fn test_frontend_message_write_row_description() -> Result<(), CubeError> {
        let mut cursor = Cursor::new(vec![]);
        let desc = RowDescription::new(vec![
            RowDescriptionField::new("num".to_string(), PgType::get_by_tid(PgTypeId::INT8)),
            RowDescriptionField::new("str".to_string(), PgType::get_by_tid(PgTypeId::INT8)),
            RowDescriptionField::new("bool".to_string(), PgType::get_by_tid(PgTypeId::INT8)),
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
