use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{ArrayRef, Date32Array, Float64Array, ListArray},
        compute::cast,
        datatypes::{DataType, Field, TimeUnit},
    },
    error::{DataFusionError, Result},
    logical_expr::{
        Accumulator, AccumulatorFunctionImplementation, AggregateUDF, ReturnTypeFunction,
        Signature, StateTypeFunction, TypeSignature, Volatility,
    },
    scalar::ScalarValue,
};

pub const XIRR_UDAF_NAME: &str = "xirr";

/// Creates a XIRR Aggregate UDF.
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
pub fn create_xirr_udaf() -> AggregateUDF {
    let name = XIRR_UDAF_NAME;
    let type_signatures = {
        // Only types actually used by cubesql are included
        const NUMERIC_TYPES: &[DataType] = &[DataType::Float64, DataType::Int64, DataType::Int32];
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
    let signature = Signature::one_of(
        type_signatures,
        Volatility::Volatile, // due to the usage of [`f64::powf`]
    );
    let return_type: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Float64)));
    let accumulator: AccumulatorFunctionImplementation =
        Arc::new(|| Ok(Box::new(XirrAccumulator::new())));
    let state_type: StateTypeFunction = Arc::new(|_| {
        Ok(Arc::new(vec![
            DataType::List(Box::new(Field::new("item", DataType::Float64, true))),
            DataType::List(Box::new(Field::new("item", DataType::Date32, true))),
            DataType::List(Box::new(Field::new("item", DataType::Float64, true))),
            DataType::List(Box::new(Field::new("item", DataType::Float64, true))),
        ]))
    });
    AggregateUDF::new(name, &signature, &return_type, &accumulator, &state_type)
}

#[derive(Debug)]
struct XirrAccumulator {
    /// Pairs of (payment, date).
    pairs: Vec<(f64, i32)>,
    initial_guess: ValueState<f64>,
    on_error: ValueState<f64>,
}

impl XirrAccumulator {
    fn new() -> Self {
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
}

impl Accumulator for XirrAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        let (payments, dates): (Vec<_>, Vec<_>) = self
            .pairs
            .iter()
            .map(|(payment, date)| {
                let payment = ScalarValue::Float64(Some(*payment));
                let date = ScalarValue::Date32(Some(*date));
                (payment, date)
            })
            .unzip();
        let initial_guess = match self.initial_guess {
            ValueState::Unset => vec![],
            ValueState::Set(initial_guess) => vec![ScalarValue::Float64(initial_guess)],
        };
        let on_error = match self.on_error {
            ValueState::Unset => vec![],
            ValueState::Set(on_error) => vec![ScalarValue::Float64(on_error)],
        };
        Ok(vec![
            ScalarValue::List(Some(Box::new(payments)), Box::new(DataType::Float64)),
            ScalarValue::List(Some(Box::new(dates)), Box::new(DataType::Date32)),
            ScalarValue::List(Some(Box::new(initial_guess)), Box::new(DataType::Float64)),
            ScalarValue::List(Some(Box::new(on_error)), Box::new(DataType::Float64)),
        ])
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
        let states_len = states.len();
        if states_len < 3 {
            return Ok(());
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
        if states_len < 4 {
            return Ok(());
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

    fn evaluate(&self) -> Result<ScalarValue> {
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
}
