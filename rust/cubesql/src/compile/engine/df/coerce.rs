use datafusion::arrow::datatypes::DataType;

pub fn is_signed_numeric(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
    )
}

pub fn is_numeric(dt: &DataType) -> bool {
    is_signed_numeric(dt)
        || match dt {
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => true,
            _ => false,
        }
}

pub fn numerical_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    // error on any non-numeric type
    if !is_numeric(lhs_type) || !is_numeric(rhs_type) {
        return None;
    };

    // same type => all good
    if lhs_type == rhs_type {
        return Some(lhs_type.clone());
    }

    match (lhs_type, rhs_type) {
        (_, DataType::UInt64) => Some(DataType::UInt64),
        (DataType::UInt64, _) => Some(DataType::UInt64),
        //
        (_, DataType::Int64) => Some(DataType::Int64),
        (DataType::Int64, _) => Some(DataType::Int64),
        //
        _ => None,
    }
}

pub fn if_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    // same type => all good
    if lhs_type == rhs_type {
        return Some(lhs_type.clone());
    }

    numerical_coercion(lhs_type, rhs_type)
}
