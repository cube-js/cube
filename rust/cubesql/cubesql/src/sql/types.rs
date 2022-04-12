use bitflags::bitflags;
use msql_srv::{
    ColumnFlags as MysqlColumnFlags, ColumnType as MysqlColumnType, StatusFlags as MysqlStatusFlags,
};

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum ColumnType {
    String,
    VarStr,
    Double,
    Boolean,
    Int8,
    Int32,
    Int64,
    Blob,
    Timestamp,
}

impl ColumnType {
    pub fn to_mysql(&self) -> MysqlColumnType {
        match self {
            ColumnType::String => MysqlColumnType::MYSQL_TYPE_STRING,
            ColumnType::VarStr => MysqlColumnType::MYSQL_TYPE_VAR_STRING,
            ColumnType::Double => MysqlColumnType::MYSQL_TYPE_DOUBLE,
            ColumnType::Boolean => MysqlColumnType::MYSQL_TYPE_TINY,
            ColumnType::Int8 | ColumnType::Int32 => MysqlColumnType::MYSQL_TYPE_LONG,
            ColumnType::Int64 => MysqlColumnType::MYSQL_TYPE_LONGLONG,
            _ => MysqlColumnType::MYSQL_TYPE_BLOB,
        }
    }
}

bitflags! {
    pub struct ColumnFlags: u8 {
        const NOT_NULL  = 0b00000001;
        const UNSIGNED  = 0b00000010;
    }
}

impl ColumnFlags {
    pub fn to_mysql(&self) -> MysqlColumnFlags {
        MysqlColumnFlags::empty()
    }
}

bitflags! {
    pub struct StatusFlags: u8 {
        const SERVER_STATE_CHANGED = 0b00000001;
        const AUTOCOMMIT           = 0b00000010;
    }
}

impl StatusFlags {
    pub fn to_mysql_flags(&self) -> MysqlStatusFlags {
        MysqlStatusFlags::empty()
    }
}

/*
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
#[repr(u8)]
pub enum ColumnType {
    MYSQL_TYPE_DECIMAL = 0,
    MYSQL_TYPE_TINY,
    MYSQL_TYPE_SHORT,
    MYSQL_TYPE_LONG,
    MYSQL_TYPE_FLOAT,
    MYSQL_TYPE_DOUBLE,
    MYSQL_TYPE_NULL,
    MYSQL_TYPE_TIMESTAMP,
    MYSQL_TYPE_LONGLONG,
    MYSQL_TYPE_INT24,
    MYSQL_TYPE_DATE,
    MYSQL_TYPE_TIME,
    MYSQL_TYPE_DATETIME,
    MYSQL_TYPE_YEAR,
    MYSQL_TYPE_NEWDATE, // Internal to MySql
    MYSQL_TYPE_VARCHAR,
    MYSQL_TYPE_BIT,
    MYSQL_TYPE_TIMESTAMP2,
    MYSQL_TYPE_DATETIME2,
    MYSQL_TYPE_TIME2,
    MYSQL_TYPE_JSON = 245,
    MYSQL_TYPE_NEWDECIMAL = 246,
    MYSQL_TYPE_ENUM = 247,
    MYSQL_TYPE_SET = 248,
    MYSQL_TYPE_TINY_BLOB = 249,
    MYSQL_TYPE_MEDIUM_BLOB = 250,
    MYSQL_TYPE_LONG_BLOB = 251,
    MYSQL_TYPE_BLOB = 252,
    MYSQL_TYPE_VAR_STRING = 253,
    MYSQL_TYPE_STRING = 254,
    MYSQL_TYPE_GEOMETRY = 255,
}

impl From<u8> for ColumnType {
    fn from(x: u8) -> ColumnType {
        match x {
            0x00_u8 => ColumnType::MYSQL_TYPE_DECIMAL,
            0x01_u8 => ColumnType::MYSQL_TYPE_TINY,
            0x02_u8 => ColumnType::MYSQL_TYPE_SHORT,
            0x03_u8 => ColumnType::MYSQL_TYPE_LONG,
            0x04_u8 => ColumnType::MYSQL_TYPE_FLOAT,
            0x05_u8 => ColumnType::MYSQL_TYPE_DOUBLE,
            0x06_u8 => ColumnType::MYSQL_TYPE_NULL,
            0x07_u8 => ColumnType::MYSQL_TYPE_TIMESTAMP,
            0x08_u8 => ColumnType::MYSQL_TYPE_LONGLONG,
            0x09_u8 => ColumnType::MYSQL_TYPE_INT24,
            0x0a_u8 => ColumnType::MYSQL_TYPE_DATE,
            0x0b_u8 => ColumnType::MYSQL_TYPE_TIME,
            0x0c_u8 => ColumnType::MYSQL_TYPE_DATETIME,
            0x0d_u8 => ColumnType::MYSQL_TYPE_YEAR,
            0x0f_u8 => ColumnType::MYSQL_TYPE_VARCHAR,
            0x10_u8 => ColumnType::MYSQL_TYPE_BIT,
            0x11_u8 => ColumnType::MYSQL_TYPE_TIMESTAMP2,
            0x12_u8 => ColumnType::MYSQL_TYPE_DATETIME2,
            0x13_u8 => ColumnType::MYSQL_TYPE_TIME2,
            0xf5_u8 => ColumnType::MYSQL_TYPE_JSON,
            0xf6_u8 => ColumnType::MYSQL_TYPE_NEWDECIMAL,
            0xf7_u8 => ColumnType::MYSQL_TYPE_ENUM,
            0xf8_u8 => ColumnType::MYSQL_TYPE_SET,
            0xf9_u8 => ColumnType::MYSQL_TYPE_TINY_BLOB,
            0xfa_u8 => ColumnType::MYSQL_TYPE_MEDIUM_BLOB,
            0xfb_u8 => ColumnType::MYSQL_TYPE_LONG_BLOB,
            0xfc_u8 => ColumnType::MYSQL_TYPE_BLOB,
            0xfd_u8 => ColumnType::MYSQL_TYPE_VAR_STRING,
            0xfe_u8 => ColumnType::MYSQL_TYPE_STRING,
            0xff_u8 => ColumnType::MYSQL_TYPE_GEOMETRY,
            _ => panic!("Unknown column type {}", x),
        }
    }
}

!bitflags
pub struct ColumnFlags: u16 {
    /// Field can't be NULL.
    const NOT_NULL_FLAG         = 1u16;

    /// Field is part of a primary key.
    const PRI_KEY_FLAG          = 2u16;

    /// Field is part of a unique key.
    const UNIQUE_KEY_FLAG       = 4u16;

    /// Field is part of a key.
    const MULTIPLE_KEY_FLAG     = 8u16;

    /// Field is a blob.
    const BLOB_FLAG             = 16u16;

    /// Field is unsigned.
    const UNSIGNED_FLAG         = 32u16;

    /// Field is zerofill.
    const ZEROFILL_FLAG         = 64u16;

    /// Field is binary.
    const BINARY_FLAG           = 128u16;

    /// Field is an enum.
    const ENUM_FLAG             = 256u16;

    /// Field is a autoincrement field.
    const AUTO_INCREMENT_FLAG   = 512u16;

    /// Field is a timestamp.
    const TIMESTAMP_FLAG        = 1024u16;

    /// Field is a set.
    const SET_FLAG              = 2048u16;

    /// Field doesn't have default value.
    const NO_DEFAULT_VALUE_FLAG = 4096u16;

    /// Field is set to NOW on UPDATE.
    const ON_UPDATE_NOW_FLAG    = 8192u16;

    /// Intern; Part of some key.
    const PART_KEY_FLAG         = 16384u16;

    /// Field is num (for clients).
    const NUM_FLAG              = 32768u16;
}

!bitflags
pub struct StatusFlags: u16 {
    /// Is raised when a multi-statement transaction has been started, either explicitly,
    /// by means of BEGIN or COMMIT AND CHAIN, or implicitly, by the first transactional
    /// statement, when autocommit=off.
    const SERVER_STATUS_IN_TRANS             = 0x0001;

    /// Server in auto_commit mode.
    const SERVER_STATUS_AUTOCOMMIT           = 0x0002;

    /// Multi query - next query exists.
    const SERVER_MORE_RESULTS_EXISTS         = 0x0008;

    const SERVER_STATUS_NO_GOOD_INDEX_USED   = 0x0010;

    const SERVER_STATUS_NO_INDEX_USED        = 0x0020;

    /// The server was able to fulfill the clients request and opened a read-only
    /// non-scrollable cursor for a query. This flag comes in reply to COM_STMT_EXECUTE
    /// and COM_STMT_FETCH commands. Used by Binary Protocol Resultset to signal that
    /// COM_STMT_FETCH must be used to fetch the row-data.
    const SERVER_STATUS_CURSOR_EXISTS        = 0x0040;

    /// This flag is sent when a read-only cursor is exhausted, in reply to
    /// COM_STMT_FETCH command.
    const SERVER_STATUS_LAST_ROW_SENT        = 0x0080;

    /// A database was dropped.
    const SERVER_STATUS_DB_DROPPED           = 0x0100;

    const SERVER_STATUS_NO_BACKSLASH_ESCAPES = 0x0200;

    /// Sent to the client if after a prepared statement reprepare we discovered
    /// that the new statement returns a different number of result set columns.
    const SERVER_STATUS_METADATA_CHANGED     = 0x0400;

    const SERVER_QUERY_WAS_SLOW              = 0x0800;

    /// To mark ResultSet containing output parameter values.
    const SERVER_PS_OUT_PARAMS               = 0x1000;

    /// Set at the same time as SERVER_STATUS_IN_TRANS if the started multi-statement
    /// transaction is a read-only transaction. Cleared when the transaction commits
    /// or aborts. Since this flag is sent to clients in OK and EOF packets, the flag
    /// indicates the transaction status at the end of command execution.
    const SERVER_STATUS_IN_TRANS_READONLY    = 0x2000;

    /// This status flag, when on, implies that one of the state information has
    /// changed on the server because of the execution of the last statement.
    const SERVER_SESSION_STATE_CHANGED       = 0x4000;
}
*/
