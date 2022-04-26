use datafusion::arrow::datatypes::DataType;
use pg_srv::PgTypeId;
use std::io;

pub fn df_type_to_pg_tid(dt: &DataType) -> Result<PgTypeId, io::Error> {
    match dt {
        DataType::Boolean => Ok(PgTypeId::BOOL),
        DataType::Int16 => Ok(PgTypeId::INT2),
        DataType::Int32 => Ok(PgTypeId::INT4),
        DataType::Int64 => Ok(PgTypeId::INT8),
        DataType::UInt16 => Ok(PgTypeId::INT8),
        DataType::UInt32 => Ok(PgTypeId::INT8),
        DataType::UInt64 => Ok(PgTypeId::INT8),
        DataType::Float32 => Ok(PgTypeId::FLOAT4),
        DataType::Float64 => Ok(PgTypeId::FLOAT8),
        DataType::Utf8 | DataType::LargeUtf8 => Ok(PgTypeId::TEXT),
        DataType::Timestamp(_, tz) => match tz {
            None => Ok(PgTypeId::TIMESTAMP),
            Some(_) => Ok(PgTypeId::TIMESTAMPTZ),
        },
        DataType::Null => Ok(PgTypeId::BOOL),
        DataType::List(field) => match field.data_type() {
            DataType::Boolean => Ok(PgTypeId::ArrayBool),
            DataType::Int8 => Ok(PgTypeId::ArrayInt2),
            DataType::Int16 => Ok(PgTypeId::ArrayInt2),
            DataType::Int32 => Ok(PgTypeId::ArrayInt4),
            DataType::Int64 => Ok(PgTypeId::ArrayInt8),
            DataType::UInt8 => Ok(PgTypeId::ArrayInt2),
            DataType::UInt16 => Ok(PgTypeId::ArrayInt2),
            DataType::UInt32 => Ok(PgTypeId::ArrayInt4),
            DataType::UInt64 => Ok(PgTypeId::ArrayInt8),
            DataType::Float16 => Ok(PgTypeId::ArrayFloat4),
            DataType::Float32 => Ok(PgTypeId::ArrayFloat4),
            DataType::Float64 => Ok(PgTypeId::ArrayFloat8),
            DataType::Binary => Ok(PgTypeId::ArrayBytea),
            DataType::Utf8 => Ok(PgTypeId::ArrayText),
            dt => Err(io::Error::new(
                io::ErrorKind::Other,
                format!("Unsupported data type in List for pg-wire: {:?}", dt),
            )),
        },
        dt => Err(io::Error::new(
            io::ErrorKind::Other,
            format!("Unsupported data type for pg-wire: {:?}", dt),
        )),
    }
}
