use crate::compile::CommandCompletion;
use bitflags::bitflags;
use datafusion::arrow::datatypes::{DataType, Field, IntervalUnit, TimeUnit};
use pg_srv::{protocol::CommandComplete, PgTypeId};
use serde::Serialize;

#[derive(Clone, PartialEq, Debug, Serialize)]
pub enum ColumnType {
    String,
    VarStr,
    Double,
    Boolean,
    Int8,
    Int32,
    Int64,
    Blob,
    // true = Date32
    // false = Date64
    Date(bool),
    Interval(IntervalUnit),
    Timestamp,
    Decimal(usize, usize),
    List(Box<Field>),
}

impl ColumnType {
    pub fn to_pg_tid(&self) -> PgTypeId {
        match self {
            ColumnType::Blob => PgTypeId::BYTEA,
            ColumnType::Boolean => PgTypeId::BOOL,
            ColumnType::Int64 => PgTypeId::INT8,
            ColumnType::Int8 => PgTypeId::INT2,
            ColumnType::Int32 => PgTypeId::INT4,
            ColumnType::String | ColumnType::VarStr => PgTypeId::TEXT,
            ColumnType::Interval(_) => PgTypeId::INTERVAL,
            ColumnType::Date(_) => PgTypeId::DATE,
            ColumnType::Timestamp => PgTypeId::TIMESTAMP,
            ColumnType::Double => PgTypeId::NUMERIC,
            ColumnType::Decimal(_, _) => PgTypeId::NUMERIC,
            ColumnType::List(field) => match field.data_type() {
                DataType::Binary => PgTypeId::ARRAYBYTEA,
                DataType::Boolean => PgTypeId::ARRAYBOOL,
                DataType::Utf8 => PgTypeId::ARRAYTEXT,
                DataType::Int16 => PgTypeId::ARRAYINT2,
                DataType::Int32 => PgTypeId::ARRAYINT4,
                DataType::Int64 => PgTypeId::ARRAYINT8,
                DataType::UInt16 => PgTypeId::ARRAYINT2,
                DataType::UInt32 => PgTypeId::ARRAYINT4,
                DataType::UInt64 => PgTypeId::ARRAYINT8,
                dt => unimplemented!("Unsupported data type for List: {}", dt),
            },
        }
    }

    pub fn avg_size(&self) -> usize {
        match self {
            ColumnType::Boolean | ColumnType::Int8 => 1,
            ColumnType::Int32
            | ColumnType::Date(true)
            | ColumnType::Interval(IntervalUnit::YearMonth) => 4,
            ColumnType::Double
            | ColumnType::Int64
            | ColumnType::Date(false)
            | ColumnType::Interval(IntervalUnit::DayTime)
            | ColumnType::Timestamp => 8,
            ColumnType::Interval(IntervalUnit::MonthDayNano) | ColumnType::Decimal(_, _) => 16,
            ColumnType::String | ColumnType::VarStr => 64,
            ColumnType::Blob | ColumnType::List(_) => 128,
        }
    }

    pub fn to_arrow(&self) -> DataType {
        match self {
            ColumnType::Date(large) => {
                if *large {
                    DataType::Date64
                } else {
                    DataType::Date32
                }
            }
            ColumnType::Interval(unit) => DataType::Interval(unit.clone()),
            ColumnType::String => DataType::Utf8,
            ColumnType::VarStr => DataType::Utf8,
            ColumnType::Boolean => DataType::Boolean,
            ColumnType::Double => DataType::Float64,
            ColumnType::Int8 => DataType::Int64,
            ColumnType::Int32 => DataType::Int64,
            ColumnType::Int64 => DataType::Int64,
            ColumnType::Blob => DataType::Utf8,
            ColumnType::Decimal(p, s) => DataType::Decimal(*p, *s),
            ColumnType::List(field) => DataType::List(field.clone()),
            ColumnType::Timestamp => DataType::Timestamp(TimeUnit::Nanosecond, None),
        }
    }
}

bitflags! {
    pub struct ColumnFlags: u8 {
        const NOT_NULL  = 0b00000001;
        const UNSIGNED  = 0b00000010;
    }
}

impl CommandCompletion {
    pub fn to_pg_command(self) -> CommandComplete {
        match self {
            // IDENTIFIER ONLY
            CommandCompletion::Begin => CommandComplete::Plain("BEGIN".to_string()),
            CommandCompletion::Prepare => CommandComplete::Plain("PREPARE".to_string()),
            CommandCompletion::Commit => CommandComplete::Plain("COMMIT".to_string()),
            CommandCompletion::Rollback => CommandComplete::Plain("ROLLBACK".to_string()),
            CommandCompletion::Set => CommandComplete::Plain("SET".to_string()),
            CommandCompletion::Use => CommandComplete::Plain("USE".to_string()),
            CommandCompletion::DeclareCursor => {
                CommandComplete::Plain("DECLARE CURSOR".to_string())
            }
            CommandCompletion::CloseCursor => CommandComplete::Plain("CLOSE CURSOR".to_string()),
            CommandCompletion::CloseCursorAll => {
                CommandComplete::Plain("CLOSE CURSOR ALL".to_string())
            }
            CommandCompletion::Deallocate => CommandComplete::Plain("DEALLOCATE".to_string()),
            CommandCompletion::DeallocateAll => {
                CommandComplete::Plain("DEALLOCATE ALL".to_string())
            }
            CommandCompletion::Discard(tp) => CommandComplete::Plain(format!("DISCARD {}", tp)),
            // ROWS COUNT
            CommandCompletion::Select(rows) => CommandComplete::Select(rows),
            CommandCompletion::DropTable => CommandComplete::Plain("DROP TABLE".to_string()),
        }
    }
}
