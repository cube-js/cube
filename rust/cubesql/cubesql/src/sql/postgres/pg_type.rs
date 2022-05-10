use datafusion::arrow::datatypes::DataType;
use pg_srv::{protocol, PgTypeId, ProtocolError};

pub fn df_type_to_pg_tid(dt: &DataType) -> Result<PgTypeId, ProtocolError> {
    match dt {
        DataType::Boolean => Ok(PgTypeId::BOOL),
        // TODO: @ovr — table values <int8 are not supported at the moment
        DataType::Int16 => Ok(PgTypeId::INT8),
        DataType::Int32 => Ok(PgTypeId::INT8),
        DataType::Int64 => Ok(PgTypeId::INT8),
        DataType::UInt16 => Ok(PgTypeId::INT8),
        DataType::UInt32 => Ok(PgTypeId::INT8),
        DataType::UInt64 => Ok(PgTypeId::INT8),
        // TODO: @ovr — table values <float8 are not supported at the moment
        DataType::Float32 => Ok(PgTypeId::FLOAT8),
        DataType::Float64 => Ok(PgTypeId::FLOAT8),
        DataType::Utf8 | DataType::LargeUtf8 => Ok(PgTypeId::TEXT),
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
