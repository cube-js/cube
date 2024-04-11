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

pub(crate) use downcast_string_arg;
