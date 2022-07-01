use datafusion::arrow::datatypes::DataType;
use pg_srv::{protocol, PgTypeId, ProtocolError};

pub fn df_type_to_pg_tid(dt: &DataType) -> Result<PgTypeId, ProtocolError> {
    match dt {
        DataType::Boolean => Ok(PgTypeId::BOOL),
        DataType::Int16 => Ok(PgTypeId::INT2),
        DataType::UInt16 => Ok(PgTypeId::INT2),
        DataType::Int32 => Ok(PgTypeId::INT4),
        DataType::UInt32 => Ok(PgTypeId::INT4),
        DataType::Int64 => Ok(PgTypeId::INT8),
        DataType::UInt64 => Ok(PgTypeId::INT8),
        DataType::Float32 => Ok(PgTypeId::FLOAT4),
        DataType::Float64 => Ok(PgTypeId::FLOAT8),
        DataType::Decimal(_, _) => Ok(PgTypeId::NUMERIC),
        DataType::Utf8 | DataType::LargeUtf8 => Ok(PgTypeId::TEXT),
        DataType::Date32 | DataType::Date64 => Ok(PgTypeId::DATE),
        DataType::Interval(_) => Ok(PgTypeId::INTERVAL),
        DataType::Timestamp(_, tz) => match tz {
            None => Ok(PgTypeId::TIMESTAMP),
            Some(_) => Ok(PgTypeId::TIMESTAMPTZ),
        },
        DataType::Null => Ok(PgTypeId::BOOL),
        DataType::List(field) => match field.data_type() {
            DataType::Boolean => Ok(PgTypeId::ARRAYBOOL),
            DataType::Int8 => Ok(PgTypeId::ARRAYINT2),
            DataType::Int16 => Ok(PgTypeId::ARRAYINT2),
            DataType::Int32 => Ok(PgTypeId::ARRAYINT4),
            DataType::Int64 => Ok(PgTypeId::ARRAYINT8),
            DataType::UInt8 => Ok(PgTypeId::ARRAYINT2),
            DataType::UInt16 => Ok(PgTypeId::ARRAYINT2),
            DataType::UInt32 => Ok(PgTypeId::ARRAYINT4),
            DataType::UInt64 => Ok(PgTypeId::ARRAYINT8),
            DataType::Float16 => Ok(PgTypeId::ARRAYFLOAT4),
            DataType::Float32 => Ok(PgTypeId::ARRAYFLOAT4),
            DataType::Float64 => Ok(PgTypeId::ARRAYFLOAT8),
            DataType::Binary => Ok(PgTypeId::ARRAYBYTEA),
            DataType::Utf8 => Ok(PgTypeId::ARRAYTEXT),
            dt => Err(protocol::ErrorResponse::error(
                protocol::ErrorCode::FeatureNotSupported,
                format!("Unsupported data type in List for pg-wire: {:?}", dt),
            )
            .into()),
        },
        dt => Err(protocol::ErrorResponse::error(
            protocol::ErrorCode::FeatureNotSupported,
            format!("Unsupported data type for pg-wire: {:?}", dt),
        )
        .into()),
    }
}
