use crate::compile::engine::{
    df::scan::{ArrayRef, DataFusionError, DataType, StringBuilder},
    udf::{common::ReturnTypeFunction, utils::*},
};
use datafusion::{
    arrow::{
        array::{Array, PrimitiveArray},
        compute::{cast_with_options, CastOptions},
        datatypes::{Int64Type, TimeUnit, TimestampNanosecondType},
    },
    logical_expr::{ScalarUDF, Signature, TypeSignature, Volatility},
    physical_plan::functions::make_scalar_function,
};
use itertools::izip;
use regex::Regex;
use std::{any::type_name, collections::HashMap, sync::Arc};

// https://docs.aws.amazon.com/redshift/latest/dg/r_DATEDIFF_function.html
pub fn create_datediff_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        assert!(args.len() == 3);

        let datepart_array = downcast_string_arg!(args[0], "datepart", i32);

        let left_date_array = match args[1].data_type() {
            DataType::Timestamp(TimeUnit::Nanosecond, None) => Arc::clone(&args[1]),
            DataType::Timestamp(TimeUnit::Microsecond, None)
            | DataType::Timestamp(TimeUnit::Millisecond, None)
            | DataType::Timestamp(TimeUnit::Second, None) => cast_with_options(
                &args[1],
                &DataType::Timestamp(TimeUnit::Nanosecond, None),
                &CastOptions { safe: false },
            )?,
            t => {
                return Err(DataFusionError::Execution(format!(
                    "second datediff argument must be of type Timestamp actual: {}",
                    t
                )))
            }
        };

        let right_date_array = match args[2].data_type() {
            DataType::Timestamp(TimeUnit::Nanosecond, None) => Arc::clone(&args[2]),
            DataType::Timestamp(TimeUnit::Microsecond, None)
            | DataType::Timestamp(TimeUnit::Millisecond, None)
            | DataType::Timestamp(TimeUnit::Second, None) => cast_with_options(
                &args[2],
                &DataType::Timestamp(TimeUnit::Nanosecond, None),
                &CastOptions { safe: false },
            )?,
            t => {
                return Err(DataFusionError::Execution(format!(
                    "third datediff argument must be of type Timestamp actual: {}",
                    t
                )))
            }
        };

        let left_date_array =
            downcast_primitive_arg!(left_date_array, "left_date", TimestampNanosecondType);
        let right_date_array =
            downcast_primitive_arg!(right_date_array, "right_date", TimestampNanosecondType);

        let result = izip!(datepart_array, left_date_array, right_date_array)
            .map(|args| {
                match args {
                    (Some(datepart), Some(left_date), Some(right_date)) => {
                        match datepart.to_lowercase().as_str() {
                            // TODO: support more dateparts as needed
                            "day" | "days" | "d" => {
                                let nanoseconds_in_day = 86_400_000_000_000_i64;
                                let left_days = left_date / nanoseconds_in_day;
                                let right_days = right_date / nanoseconds_in_day;
                                let day_difference = right_days - left_days;
                                Ok(Some(day_difference))
                            }
                            _ => Err(DataFusionError::Execution(format!(
                                "unsupported DATEDIFF datepart: {}",
                                datepart,
                            ))),
                        }
                    }
                    _ => Ok(None),
                }
            })
            .collect::<crate::compile::engine::df::scan::Result<PrimitiveArray<Int64Type>>>()?;

        Ok(Arc::new(result) as ArrayRef)
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Int64)));

    ScalarUDF::new(
        "datediff",
        &Signature::any(3, Volatility::Immutable),
        &return_type,
        &fun,
    )
}

// https://docs.aws.amazon.com/redshift/latest/dg/r_DATEADD_function.html
pub fn create_dateadd_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        assert!(args.len() == 3);

        return Err(DataFusionError::NotImplemented(format!(
            "dateadd is not implemented, it's stub"
        )));
    });

    let return_type: ReturnTypeFunction =
        Arc::new(move |_| Ok(Arc::new(DataType::Timestamp(TimeUnit::Nanosecond, None))));

    ScalarUDF::new(
        "dateadd",
        &Signature::any(3, Volatility::Immutable),
        &return_type,
        &fun,
    )
}

/// https://docs.aws.amazon.com/redshift/latest/dg/REGEXP_SUBSTR.html
pub fn create_regexp_substr_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        let source_arr = downcast_string_arg!(args[0], "source_string", i32);
        let pattern_arr = downcast_string_arg!(args[1], "pattern", i32);
        let position_arr = if args.len() > 2 {
            Some(downcast_primitive_arg!(args[2], "position", Int64Type))
        } else {
            None
        };

        if args.len() > 3 {
            return Err(DataFusionError::NotImplemented(
                "regexp_substr does not support occurrence and parameters (flags)".to_string(),
            ));
        }

        let mut patterns: HashMap<String, Regex> = HashMap::new();
        let mut builder = StringBuilder::new(source_arr.len());

        for ((idx, source), pattern) in source_arr.iter().enumerate().zip(pattern_arr.iter()) {
            match (source, pattern) {
                (None, _) => builder.append_null()?,
                (_, None) => builder.append_null()?,
                (Some(s), Some(p)) => {
                    let input = if let Some(position) = position_arr {
                        if position.is_null(idx) {
                            builder.append_null()?;

                            continue;
                        } else {
                            let pos = position.value(idx);
                            if pos <= 1 {
                                s
                            } else if (pos as usize) > s.len() {
                                builder.append_value(&"")?;

                                continue;
                            } else {
                                &s[((pos as usize) - 1)..]
                            }
                        }
                    } else {
                        s
                    };

                    let re_pattern = if let Some(re) = patterns.get(p) {
                        re.clone()
                    } else {
                        let re = Regex::new(p).map_err(|e| {
                            DataFusionError::Execution(format!(
                                "Regular expression did not compile: {:?}",
                                e
                            ))
                        })?;
                        patterns.insert(p.to_string(), re.clone());

                        re
                    };

                    match re_pattern.captures(input) {
                        Some(caps) => {
                            if let Some(m) = caps.get(0) {
                                builder.append_value(m.as_str())?;
                            } else {
                                builder.append_value("")?
                            }
                        }
                        None => builder.append_value("")?,
                    }
                }
            };
        }

        Ok(Arc::new(builder.finish()))
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Utf8)));

    ScalarUDF::new(
        "regexp_substr",
        &Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Int64]),
                TypeSignature::Exact(vec![
                    DataType::Utf8,
                    DataType::Utf8,
                    DataType::Int64,
                    DataType::Int64,
                ]),
                TypeSignature::Exact(vec![
                    DataType::Utf8,
                    DataType::Utf8,
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Utf8,
                ]),
            ],
            Volatility::Immutable,
        ),
        &return_type,
        &fun,
    )
}

// https://docs.aws.amazon.com/redshift/latest/dg/r_CHARINDEX.html
pub fn create_charindex_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        assert!(args.len() == 2);

        return Err(DataFusionError::NotImplemented(format!(
            "charindex is not implemented, it's stub"
        )));
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Int32)));

    ScalarUDF::new(
        "charindex",
        &Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable),
        &return_type,
        &fun,
    )
}
