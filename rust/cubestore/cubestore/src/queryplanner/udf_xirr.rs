use std::{any::Any, sync::Arc};

use datafusion::{
    arrow::{
        array::{ArrayRef, ArrowPrimitiveType, Date32Array, Float64Array, ListArray},
        compute::cast,
        datatypes::{DataType, Date32Type, Field, Float64Type, TimeUnit},
    },
    common::utils::proxy::VecAllocExt,
    error::{DataFusionError, Result},
    logical_expr::{
        function::{AccumulatorArgs, StateFieldsArgs},
        utils::format_state_name,
        AggregateUDFImpl, Signature, TypeSignature, Volatility,
    },
    physical_plan::Accumulator,
    scalar::ScalarValue,
};

// This is copy/pasted and edited from cubesql in a file xirr.rs -- you might need to update both.

pub const XIRR_UDAF_NAME: &str = "xirr";

/// An XIRR Aggregate UDF.
///
/// Syntax:
/// ```sql
/// XIRR(<payment>, <date> [, <initial_guess> [, <on_error>]])
/// ```
///
/// This function calculates internal rate of return for a series of cash flows (payments)
/// that occur at irregular intervals.
///
/// The function takes two arguments:
/// - `payment` (numeric): The cash flow amount. NULL values are considered 0.
/// - `date` (datetime): The date of the payment. Time is ignored. Must never be NULL.
/// - (optional) `initial_guess` (numeric): An initial guess for the rate of return. Must be
///   greater than -1.0 and consistent across all rows. If NULL or omitted, a default value
///   of 0.1 is used.
/// - (optional) `on_error` (numeric): A value to return if the function cannot find a solution.
///   If omitted, the function will yield an error when it cannot find a solution. Must be
///   consistent across all rows.
///
/// The function always yields an error if:
/// - There are no rows.
/// - The `date` argument contains a NULL value.
/// - The `initial_guess` argument is less than or equal to -1.0, or inconsistent across all rows.
/// - The `on_error` argument is inconsistent across all rows.
///
/// The function returns `on_error` value (or yields an error if omitted) if:
/// - The function cannot find a solution after a set number of iterations.
/// - The calculation failed due to internal division by 0.

#[derive(Debug)]
pub(crate) struct XirrUDF {
    signature: Signature,
}

impl XirrUDF {
    pub fn new() -> XirrUDF {
        let type_signatures = {
            // Only types actually used by cubesql are included
            const NUMERIC_TYPES: &[DataType] =
                &[DataType::Float64, DataType::Int64, DataType::Int32];
            const DATETIME_TYPES: &[DataType] = &[
                DataType::Date32,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                DataType::Timestamp(TimeUnit::Millisecond, None),
            ];
            let mut type_signatures = Vec::with_capacity(45);
            for payment_type in NUMERIC_TYPES {
                for date_type in DATETIME_TYPES {
                    // Base signatures without `initial_guess` and `on_error` arguments
                    type_signatures.push(TypeSignature::Exact(vec![
                        payment_type.clone(),
                        date_type.clone(),
                    ]));
                    // Signatures with `initial_guess` argument; only [`DataType::Float64`] is accepted
                    const INITIAL_GUESS_TYPE: DataType = DataType::Float64;
                    type_signatures.push(TypeSignature::Exact(vec![
                        payment_type.clone(),
                        date_type.clone(),
                        INITIAL_GUESS_TYPE,
                    ]));
                    // Signatures with `initial_guess` and `on_error` arguments
                    for on_error_type in NUMERIC_TYPES {
                        type_signatures.push(TypeSignature::Exact(vec![
                            payment_type.clone(),
                            date_type.clone(),
                            INITIAL_GUESS_TYPE,
                            on_error_type.clone(),
                        ]));
                    }
                }
            }
            type_signatures
        };
        let type_signature = TypeSignature::OneOf(type_signatures);
        XirrUDF {
            signature: Signature {
                type_signature,
                volatility: Volatility::Immutable,
            },
        }
    }
}

impl AggregateUDFImpl for XirrUDF {
    fn name(&self) -> &str {
        XIRR_UDAF_NAME
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::Float64)
    }
    fn accumulator(
        &self,
        _acc_args: AccumulatorArgs,
    ) -> datafusion::common::Result<Box<dyn Accumulator>> {
        Ok(Box::new(XirrAccumulator::new()))
    }
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {
        Ok(vec![
            Field::new(
                format_state_name(args.name, "payments"),
                DataType::List(Arc::new(Field::new_list_field(DataType::Float64, true))),
                false,
            ),
            Field::new(
                format_state_name(args.name, "dates"),
                DataType::List(Arc::new(Field::new_list_field(DataType::Date32, true))),
                false,
            ),
            Field::new(
                format_state_name(args.name, "initial_guess"),
                DataType::List(Arc::new(Field::new_list_field(DataType::Float64, true))),
                false,
            ),
            Field::new(
                format_state_name(args.name, "on_error"),
                DataType::List(Arc::new(Field::new_list_field(DataType::Float64, true))),
                false,
            ),
        ])
    }
}

#[derive(Debug)]
pub struct XirrAccumulator {
    /// Pairs of (payment, date).
    pairs: Vec<(f64, i32)>,
    initial_guess: ValueState<f64>,
    on_error: ValueState<f64>,
}

impl XirrAccumulator {
    pub fn new() -> Self {
        XirrAccumulator {
            pairs: vec![],
            initial_guess: ValueState::Unset,
            on_error: ValueState::Unset,
        }
    }

    fn add_pair(&mut self, payment: Option<f64>, date: Option<i32>) -> Result<()> {
        let Some(date) = date else {
            return Err(DataFusionError::Execution(
                "One or more values for the `date` argument passed to XIRR is null".to_string(),
            ));
        };
        // NULL payment value is treated as 0
        let payment = payment.unwrap_or(0.0);
        self.pairs.push((payment, date));
        Ok(())
    }

    fn set_initial_guess(&mut self, initial_guess: Option<f64>) -> Result<()> {
        let ValueState::Set(current_initial_guess) = self.initial_guess else {
            self.initial_guess = ValueState::Set(initial_guess);
            return Ok(());
        };
        if current_initial_guess != initial_guess {
            return Err(DataFusionError::Execution(
                "The `initial_guess` argument passed to XIRR is inconsistent".to_string(),
            ));
        }
        Ok(())
    }

    fn set_on_error(&mut self, on_error: Option<f64>) -> Result<()> {
        let ValueState::Set(current_on_error) = self.on_error else {
            self.on_error = ValueState::Set(on_error);
            return Ok(());
        };
        if current_on_error != on_error {
            return Err(DataFusionError::Execution(
                "The `on_error` argument passed to XIRR is inconsistent".to_string(),
            ));
        }
        Ok(())
    }

    fn yield_no_solution(&self) -> Result<ScalarValue> {
        match self.on_error {
            ValueState::Unset => Err(DataFusionError::Execution(
                "The XIRR function couldn't find a solution".to_string(),
            )),
            ValueState::Set(on_error) => Ok(ScalarValue::Float64(on_error)),
        }
    }

    fn allocated_size(&self) -> usize {
        let XirrAccumulator {
            pairs,
            initial_guess,
            on_error,
        } = self;
        pairs.allocated_size() + initial_guess.allocated_size() + on_error.allocated_size()
    }
}

// TODO upgrade DF: Remove these, say, once we've confirmed we are not porting Cube's inplace
// aggregate implementation.  These would be used by update or merge functions in the Accumulator
// trait -- functions which no longer exist.

// fn cast_scalar_to_float64(scalar: &ScalarValue) -> Result<Option<f64>> {
//     fn err(from_type: &str) -> Result<Option<f64>> {
//         Err(DataFusionError::Internal(format!(
//             "cannot cast {} to Float64",
//             from_type
//         )))
//     }
//     match scalar {
//         ScalarValue::Null => err("Null"),
//         ScalarValue::Boolean(_) => err("Boolean"),
//         ScalarValue::Float16(o) => Ok(o.map(f64::from)),
//         ScalarValue::Float32(o) => Ok(o.map(f64::from)),
//         ScalarValue::Float64(o) => Ok(*o),
//         ScalarValue::Int8(o) => Ok(o.map(f64::from)),
//         ScalarValue::Int16(o) => Ok(o.map(f64::from)),
//         ScalarValue::Int32(o) => Ok(o.map(f64::from)),
//         ScalarValue::Int64(o) => Ok(o.map(|x| x as f64)),
//         ScalarValue::Decimal128(o, precision, scale) => {
//             Ok(o.map(|x| (x as f64) / 10f64.powi(*scale as i32)))
//         }
//         ScalarValue::Decimal256(o, precision, scale) => err("Decimal256"),  // TODO?
//         ScalarValue::UInt8(o) => Ok(o.map(f64::from)),
//         ScalarValue::UInt16(o) => Ok(o.map(f64::from)),
//         ScalarValue::UInt32(o) => Ok(o.map(f64::from)),
//         ScalarValue::UInt64(o) => Ok(o.map(|x| x as f64)),
//         ScalarValue::Utf8(_) => err("Utf8"),
//         ScalarValue::Utf8View(_) => err("Utf8View"),
//         ScalarValue::LargeUtf8(_) => err("LargeUtf8"),
//         ScalarValue::Binary(_) => err("Binary"),
//         ScalarValue::BinaryView(_) => err("BinaryView"),
//         ScalarValue::FixedSizeBinary(_, _) => err("FixedSizeBinary"),
//         ScalarValue::LargeBinary(_) => err("LargeBinary"),
//         ScalarValue::FixedSizeList(_) => err("FixedSizeList"),
//         ScalarValue::List(_) => err("List"),
//         ScalarValue::LargeList(_) => err("LargeList"),
//         ScalarValue::Struct(_) => err("Struct"),
//         ScalarValue::Map(_) => err("Map"),
//         ScalarValue::Date32(_) => err("Date32"),
//         ScalarValue::Date64(_) => err("Date64"),
//         ScalarValue::Time32Second(_) => err("Time32Second"),
//         ScalarValue::Time32Millisecond(_) => err("Time32Millisecond"),
//         ScalarValue::Time64Microsecond(_) => err("Time64Microsecond"),
//         ScalarValue::Time64Nanosecond(_) => err("Time64Nanosecond"),
//         ScalarValue::TimestampSecond(_, _) => err("TimestampSecond"),
//         ScalarValue::TimestampMillisecond(_, _) => err("TimestampMillisecond"),
//         ScalarValue::TimestampMicrosecond(_, _) => err("TimestampMicrosecond"),
//         ScalarValue::TimestampNanosecond(_, _) => err("TimestampNanosecond"),
//         ScalarValue::IntervalYearMonth(_) => err("IntervalYearMonth"),
//         ScalarValue::IntervalDayTime(_) => err("IntervalDayTime"),
//         ScalarValue::IntervalMonthDayNano(_) => err("IntervalMonthDayNano"),
//         ScalarValue::DurationSecond(_) => err("DurationSecond"),
//         ScalarValue::DurationMillisecond(_) => err("DurationMillisecond"),
//         ScalarValue::DurationMicrosecond(_) => err("DurationMicrosecond"),
//         ScalarValue::DurationNanosecond(_) => err("DurationNanosecond"),
//         ScalarValue::Union(_, _, _) => err("Union"),
//         ScalarValue::Dictionary(_, _) => err("Dictionary"),
//     }
// }

// fn cast_scalar_to_date32(scalar: &ScalarValue) -> Result<Option<i32>> {
//     fn err(from_type: &str) -> Result<Option<i32>> {
//         Err(DataFusionError::Internal(format!(
//             "cannot cast {} to Date32",
//             from_type
//         )))
//     }
//     fn string_to_date32(o: &Option<String>) -> Result<Option<i32>> {
//         if let Some(x) = o {
//             // Consistent with cast() in update_batch being configured with the "safe" option true, so we return None (null value) if there is a cast error.
//             Ok(x.parse::<chrono::NaiveDate>()
//                 .map(|date| date.num_days_from_ce() - EPOCH_DAYS_FROM_CE)
//                 .ok())
//         } else {
//             Ok(None)
//         }
//     }

//     // Number of days between 0001-01-01 and 1970-01-01
//     const EPOCH_DAYS_FROM_CE: i32 = 719_163;

//     const SECONDS_IN_DAY: i64 = 86_400;
//     const MILLISECONDS_IN_DAY: i64 = SECONDS_IN_DAY * 1_000;

//     match scalar {
//         ScalarValue::Null => err("Null"),
//         ScalarValue::Boolean(_) => err("Boolean"),
//         ScalarValue::Float16(_) => err("Float16"),
//         ScalarValue::Float32(_) => err("Float32"),
//         ScalarValue::Float64(_) => err("Float64"),
//         ScalarValue::Int8(_) => err("Int8"),
//         ScalarValue::Int16(_) => err("Int16"),
//         ScalarValue::Int32(o) => Ok(*o),
//         ScalarValue::Int64(o) => Ok(o.and_then(|x| num::NumCast::from(x))),
//         ScalarValue::Decimal128(_, _, _) => err("Decimal128"),
//         ScalarValue::Decimal256(_, _, _) => err("Decimal256"),
//         ScalarValue::UInt8(_) => err("UInt8"),
//         ScalarValue::UInt16(_) => err("UInt16"),
//         ScalarValue::UInt32(_) => err("UInt32"),
//         ScalarValue::UInt64(_) => err("UInt64"),
//         ScalarValue::Utf8(o) => string_to_date32(o),
//         ScalarValue::Utf8View(o) => string_to_date32(o),
//         ScalarValue::LargeUtf8(o) => string_to_date32(o),
//         ScalarValue::Binary(_) => err("Binary"),
//         ScalarValue::BinaryView(_) => err("BinaryView"),
//         ScalarValue::FixedSizeBinary(_, _) => err("FixedSizeBinary"),
//         ScalarValue::LargeBinary(_) => err("LargeBinary"),
//         ScalarValue::FixedSizeList(_) => err("FixedSizeList"),
//         ScalarValue::List(_) => err("List"),
//         ScalarValue::LargeList(_) => err("LargeList"),
//         ScalarValue::Struct(_) => err("Struct"),
//         ScalarValue::Map(_) => err("Map"),
//         ScalarValue::Date32(o) => Ok(*o),
//         ScalarValue::Date64(o) => Ok(o.map(|x| (x / MILLISECONDS_IN_DAY) as i32)),
//         ScalarValue::Time32Second(_) => err("Time32Second"),
//         ScalarValue::Time32Millisecond(_) => err("Time32Millisecond"),
//         ScalarValue::Time64Microsecond(_) => err("Time64Microsecond"),
//         ScalarValue::Time64Nanosecond(_) => err("Time64Nanosecond"),

//         ScalarValue::TimestampSecond(o, _tz) => Ok(o.map(|x| (x / SECONDS_IN_DAY) as i32)),
//         ScalarValue::TimestampMillisecond(o, _tz) => Ok(o.map(|x| (x / MILLISECONDS_IN_DAY) as i32)),
//         ScalarValue::TimestampMicrosecond(o, _tz) => {
//             Ok(o.map(|x| (x / (1_000_000 * SECONDS_IN_DAY)) as i32))
//         }
//         ScalarValue::TimestampNanosecond(o, _tz) => {
//             Ok(o.map(|x| (x / (1_000_000_000 * SECONDS_IN_DAY)) as i32))
//         }
//         ScalarValue::IntervalYearMonth(_) => err("IntervalYearMonth"),
//         ScalarValue::IntervalDayTime(_) => err("IntervalDayTime"),
//         ScalarValue::IntervalMonthDayNano(_) => err("IntervalMonthDayNano"),
//         ScalarValue::DurationSecond(_) => err("DurationSecond"),
//         ScalarValue::DurationMillisecond(_) => err("DurationMillisecond"),
//         ScalarValue::DurationMicrosecond(_) => err("DurationMicrosecond"),
//         ScalarValue::DurationNanosecond(_) => err("DurationNanosecond"),
//         ScalarValue::Union(_, _, _) => err("Union"),
//         ScalarValue::Dictionary(_, _) => err("Dictionary"),
//     }
// }

fn single_element_listarray<T, P>(iter: P) -> ListArray
where
    T: ArrowPrimitiveType,
    P: IntoIterator<Item = Option<<T as ArrowPrimitiveType>::Native>>,
{
    ListArray::from_iter_primitive::<T, P, _>(vec![Some(iter)])
}

impl Accumulator for XirrAccumulator {
    // Note that we don't have a GroupsAccumulator implementation for Xirr.

    // We keep implementations of the Cube extension functions (reset and peek_... patched into DF)
    // because our state and evaluate implementations would be immutable anyway, to avoid
    // differences between branches before and after the upgrade to DF >= 42.

    fn reset(&mut self) -> Result<()> {
        self.pairs.clear();
        self.initial_guess = ValueState::Unset;
        self.on_error = ValueState::Unset;
        Ok(())
    }

    fn peek_state(&self) -> Result<Vec<ScalarValue>> {
        let (payments_vec, dates_vec): (Vec<_>, Vec<_>) =
            self.pairs.iter().copied::<(f64, i32)>().unzip();

        let payments_list =
            single_element_listarray::<Float64Type, _>(payments_vec.into_iter().map(|p| Some(p)));
        let dates_list =
            single_element_listarray::<Date32Type, _>(dates_vec.into_iter().map(|p| Some(p)));

        let initial_guess_list = match self.initial_guess {
            ValueState::Unset => {
                single_element_listarray::<Float64Type, _>(([] as [Option<f64>; 0]).into_iter())
            }
            ValueState::Set(initial_guess) => single_element_listarray::<Float64Type, _>(
                ([initial_guess] as [Option<f64>; 1]).into_iter(),
            ),
        };
        let on_error_list = match self.on_error {
            ValueState::Unset => {
                single_element_listarray::<Float64Type, _>(([] as [Option<f64>; 0]).into_iter())
            }
            ValueState::Set(on_error) => single_element_listarray::<Float64Type, _>(
                ([on_error] as [Option<f64>; 1]).into_iter(),
            ),
        };
        Ok(vec![
            ScalarValue::List(Arc::new(payments_list)),
            ScalarValue::List(Arc::new(dates_list)),
            ScalarValue::List(Arc::new(initial_guess_list)),
            ScalarValue::List(Arc::new(on_error_list)),
        ])
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.peek_state()
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let payments = cast(&values[0], &DataType::Float64)?;
        let payments = payments.as_any().downcast_ref::<Float64Array>().unwrap();
        let dates = cast(&values[1], &DataType::Date32)?;
        let dates = dates.as_any().downcast_ref::<Date32Array>().unwrap();
        for (payment, date) in payments.into_iter().zip(dates) {
            self.add_pair(payment, date)?;
        }
        let values_len = values.len();
        if values_len < 3 {
            return Ok(());
        }
        let initial_guesses = values[2].as_any().downcast_ref::<Float64Array>().unwrap();
        for initial_guess in initial_guesses {
            self.set_initial_guess(initial_guess)?;
        }
        if values_len < 4 {
            return Ok(());
        }
        let on_errors = cast(&values[3], &DataType::Float64)?;
        let on_errors = on_errors.as_any().downcast_ref::<Float64Array>().unwrap();
        for on_error in on_errors {
            self.set_on_error(on_error)?;
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.len() != 4 {
            return Err(DataFusionError::Internal(format!(
                "Merging XIRR states list with {} columns instead of 4",
                states.len()
            )));
        }
        let payments = states[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .values();
        let payments = payments.as_any().downcast_ref::<Float64Array>().unwrap();
        let dates = states[1]
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .values();
        let dates = dates.as_any().downcast_ref::<Date32Array>().unwrap();
        for (payment, date) in payments.into_iter().zip(dates) {
            self.add_pair(payment, date)?;
        }

        let initial_guesses = states[2]
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .values();
        let initial_guesses = initial_guesses
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for initial_guess in initial_guesses {
            self.set_initial_guess(initial_guess)?;
        }

        let on_errors = states[3]
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .values();
        let on_errors = on_errors.as_any().downcast_ref::<Float64Array>().unwrap();
        for on_error in on_errors {
            self.set_on_error(on_error)?;
        }
        Ok(())
    }

    fn peek_evaluate(&self) -> Result<ScalarValue> {
        const MAX_ITERATIONS: usize = 100;
        const TOLERANCE: f64 = 1e-6;
        const DEFAULT_INITIAL_GUESS: f64 = 0.1;
        let Some(min_date) = self.pairs.iter().map(|(_, date)| *date).min() else {
            return Err(DataFusionError::Execution(
                "A result for XIRR couldn't be determined because the arguments are empty"
                    .to_string(),
            ));
        };
        let pairs = self
            .pairs
            .iter()
            .map(|(payment, date)| {
                let year_difference = (*date - min_date) as f64 / 365.0;
                (*payment, year_difference)
            })
            .collect::<Vec<_>>();
        let mut rate_of_return = self
            .initial_guess
            .to_value()
            .unwrap_or(DEFAULT_INITIAL_GUESS);
        if rate_of_return <= -1.0 {
            return Err(DataFusionError::Execution(
                "The `initial_guess` argument passed to the XIRR function must be greater than -1"
                    .to_string(),
            ));
        }
        for _ in 0..MAX_ITERATIONS {
            let mut net_present_value = 0.0;
            let mut derivative_value = 0.0;
            for (payment, year_difference) in &pairs {
                if *payment == 0.0 {
                    continue;
                }
                let rate_positive = 1.0 + rate_of_return;
                let denominator = rate_positive.powf(*year_difference);
                net_present_value += *payment / denominator;
                derivative_value -= *year_difference * *payment / denominator / rate_positive;
            }
            if net_present_value.abs() < TOLERANCE {
                return Ok(ScalarValue::Float64(Some(rate_of_return)));
            }
            let rate_reduction = net_present_value / derivative_value;
            if rate_reduction.is_nan() {
                return self.yield_no_solution();
            }
            rate_of_return -= rate_reduction;
        }
        self.yield_no_solution()
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.peek_evaluate()
    }

    fn size(&self) -> usize {
        size_of::<Self>() + self.allocated_size()
    }
}

#[derive(Debug)]
enum ValueState<T: Copy> {
    Unset,
    Set(Option<T>),
}

impl<T: Copy> ValueState<T> {
    fn to_value(&self) -> Option<T> {
        match self {
            ValueState::Unset => None,
            ValueState::Set(value) => *value,
        }
    }

    #[inline(always)]
    /// Zero.  Note that T: Copy.
    fn allocated_size(&self) -> usize {
        0
    }
}
