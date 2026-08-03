macro_rules! downcast_string_arg {
    ($ARG:expr, $NAME:expr, $T:ident) => {{
        $ARG.as_any()
            .downcast_ref::<datafusion::arrow::array::GenericStringArray<$T>>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "could not cast {} from {} to {}",
                    $NAME,
                    $ARG.data_type(),
                    type_name::<datafusion::arrow::array::GenericStringArray<$T>>()
                ))
            })?
    }};
}

macro_rules! downcast_boolean_arr {
    ($ARG:expr, $NAME:expr) => {{
        $ARG.as_any()
            .downcast_ref::<datafusion::arrow::array::BooleanArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "could not cast {} from {} to {}",
                    $NAME,
                    $ARG.data_type(),
                    type_name::<datafusion::arrow::array::BooleanArray>()
                ))
            })?
    }};
}

macro_rules! downcast_primitive_arg {
    ($ARG:expr, $NAME:expr, $T:ident) => {{
        $ARG.as_any()
            .downcast_ref::<datafusion::arrow::array::PrimitiveArray<$T>>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "could not cast {} from {} to {}",
                    $NAME,
                    $ARG.data_type(),
                    type_name::<$T>()
                ))
            })?
    }};
}

pub(crate) use downcast_boolean_arr;
pub(crate) use downcast_primitive_arg;
pub(crate) use downcast_string_arg;
