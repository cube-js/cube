use crate::queryplanner::coalesce::SUPPORTED_COALESCE_TYPES;
use crate::queryplanner::hll::{Hll, HllUnion};
use crate::CubeError;
use chrono::{Datelike, Duration, Months, NaiveDateTime, TimeZone, Utc};
use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, TimestampNanosecondArray, UInt64Builder,
};
use datafusion::arrow::datatypes::{DataType, IntervalDayTime, IntervalUnit, TimeUnit};
use std::any::Any;
use tokio_tungstenite::tungstenite::protocol::frame::coding::Data;
// use datafusion::cube_ext::datetime::{date_addsub_array, date_addsub_scalar};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion::logical_expr::{
    AggregateUDF, AggregateUDFImpl, Expr, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion::physical_plan::{Accumulator, ColumnarValue};
use datafusion::scalar::ScalarValue;
use serde_derive::{Deserialize, Serialize};
use smallvec::smallvec;
use smallvec::SmallVec;
use std::sync::Arc;

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub enum CubeScalarUDFKind {
    HllCardinality, // cardinality(), accepting the HyperLogLog sketches.
    // Coalesce,
    // Now,
    UnixTimestamp,
    DateAdd,
    DateSub,
    DateBin,
}

pub fn scalar_udf_by_kind(k: CubeScalarUDFKind) -> Arc<ScalarUDF> {
    match k {
        CubeScalarUDFKind::HllCardinality => Arc::new(HllCardinality::descriptor()),
        // CubeScalarUDFKind::Coalesce => Box::new(Coalesce {}),
        // CubeScalarUDFKind::Now => Box::new(Now {}),
        CubeScalarUDFKind::UnixTimestamp => {
            Arc::new(ScalarUDF::new_from_impl(UnixTimestamp::new()))
        }
        CubeScalarUDFKind::DateAdd => Arc::new(ScalarUDF::new_from_impl(DateAddSub::new_add())),
        CubeScalarUDFKind::DateSub => Arc::new(ScalarUDF::new_from_impl(DateAddSub::new_sub())),
        CubeScalarUDFKind::DateBin => Arc::new(ScalarUDF::new_from_impl(DateBin::new())),
    }
}

pub fn registerable_scalar_udfs() -> Vec<ScalarUDF> {
    vec![
        HllCardinality::descriptor(),
        ScalarUDF::new_from_impl(DateBin::new()),
        ScalarUDF::new_from_impl(DateAddSub::new_add()),
        ScalarUDF::new_from_impl(DateAddSub::new_sub()),
    ]
}

pub fn registerable_arc_scalar_udfs() -> Vec<Arc<ScalarUDF>> {
    registerable_scalar_udfs()
        .into_iter()
        .map(Arc::new)
        .collect()
}

/// Note that only full match counts. Pass capitalized names.
pub fn scalar_kind_by_name(n: &str) -> Option<CubeScalarUDFKind> {
    if n == "CARDINALITY" {
        return Some(CubeScalarUDFKind::HllCardinality);
    }
    // if n == "COALESCE" {
    //     return Some(CubeScalarUDFKind::Coalesce);
    // }
    // if n == "NOW" {
    //     return Some(CubeScalarUDFKind::Now);
    // }
    if n == "UNIX_TIMESTAMP" {
        return Some(CubeScalarUDFKind::UnixTimestamp);
    }
    if n == "DATE_ADD" {
        return Some(CubeScalarUDFKind::DateAdd);
    }
    if n == "DATE_SUB" {
        return Some(CubeScalarUDFKind::DateSub);
    }
    if n == "DATE_BIN" {
        return Some(CubeScalarUDFKind::DateBin);
    }
    // TODO upgrade DF: Remove this (once we are no longer in flux about naming casing of UDFs and UDAFs).
    if [
        "CARDINALITY",
        /* "COALESCE", "NOW", */ "UNIX_TIMESTAMP",
        "DATE_ADD",
        "DATE_SUB",
        "DATE_BIN",
    ]
    .contains(&(&n.to_ascii_uppercase() as &str))
    {
        panic!(
            "scalar_kind_by_name failing on '{}' due to uppercase/lowercase mixup",
            n
        );
    }
    return None;
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub enum CubeAggregateUDFKind {
    MergeHll, // merge(), accepting the HyperLogLog sketches.
              // Xirr,
}

pub trait CubeAggregateUDF {
    fn kind(&self) -> CubeAggregateUDFKind;
    fn name(&self) -> &str;
    fn descriptor(&self) -> AggregateUDF;
    fn accumulator(&self) -> Box<dyn Accumulator>;
}

pub fn registerable_aggregate_udfs() -> Vec<AggregateUDF> {
    vec![AggregateUDF::new_from_impl(HllMergeUDF::new())]
}

pub fn registerable_arc_aggregate_udfs() -> Vec<Arc<AggregateUDF>> {
    registerable_aggregate_udfs()
        .into_iter()
        .map(Arc::new)
        .collect()
}

pub fn aggregate_udf_by_kind(k: CubeAggregateUDFKind) -> AggregateUDF {
    match k {
        CubeAggregateUDFKind::MergeHll => AggregateUDF::new_from_impl(HllMergeUDF::new()),
    }
}

/// Note that only full match counts. Pass capitalized names.
pub fn aggregate_kind_by_name(n: &str) -> Option<CubeAggregateUDFKind> {
    if n == "merge" {
        return Some(CubeAggregateUDFKind::MergeHll);
    }
    // if n == "XIRR" {
    //     return Some(CubeAggregateUDFKind::Xirr);
    // }
    return None;
}

// The rest of the file are implementations of the various functions that we have.
// TODO: add custom type and use it instead of `Binary` for HLL columns.

// TODO upgrade DF - remove?
// struct Coalesce {}
// impl Coalesce {
//     fn signature() -> Signature {
//         Signature::Variadic(SUPPORTED_COALESCE_TYPES.to_vec())
//     }
// }
// impl CubeScalarUDF for Coalesce {
//     fn kind(&self) -> CubeScalarUDFKind {
//         CubeScalarUDFKind::Coalesce
//     }
//
//     fn name(&self) -> &str {
//         "COALESCE"
//     }
//
//     fn descriptor(&self) -> ScalarUDF {
//         return ScalarUDF {
//             name: self.name().to_string(),
//             signature: Self::signature(),
//             return_type: Arc::new(|inputs| {
//                 if inputs.is_empty() {
//                     return Err(DataFusionError::Plan(
//                         "COALESCE requires at least 1 argument".to_string(),
//                     ));
//                 }
//                 let ts = type_coercion::data_types(inputs, &Self::signature())?;
//                 Ok(Arc::new(ts[0].clone()))
//             }),
//             fun: Arc::new(coalesce),
//         };
//     }
// }

// TODO upgrade DF - remove?
// struct Now {}
// impl Now {
//     fn signature() -> Signature {
//         Signature::Exact(Vec::new())
//     }
// }
// impl CubeScalarUDF for Now {
//     fn kind(&self) -> CubeScalarUDFKind {
//         CubeScalarUDFKind::Now
//     }
//
//     fn name(&self) -> &str {
//         "NOW"
//     }
//
//     fn descriptor(&self) -> ScalarUDF {
//         return ScalarUDF {
//             name: self.name().to_string(),
//             signature: Self::signature(),
//             return_type: Arc::new(|inputs| {
//                 assert!(inputs.is_empty());
//                 Ok(Arc::new(DataType::Timestamp(TimeUnit::Nanosecond, None)))
//             }),
//             fun: Arc::new(|_| {
//                 Err(DataFusionError::Internal(
//                     "NOW() was not optimized away".to_string(),
//                 ))
//             }),
//         };
//     }
// }

#[derive(Debug)]
struct UnixTimestamp {
    signature: Signature,
}

impl UnixTimestamp {
    pub fn new() -> Self {
        UnixTimestamp {
            signature: Self::signature(),
        }
    }
    fn signature() -> Signature {
        Signature::exact(Vec::new(), Volatility::Stable)
    }
}

impl ScalarUDFImpl for UnixTimestamp {
    fn name(&self) -> &str {
        "UNIX_TIMESTAMP"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke(&self, _args: &[ColumnarValue]) -> datafusion::common::Result<ColumnarValue> {
        Err(DataFusionError::Internal(
            "UNIX_TIMESTAMP() was not optimized away".to_string(),
        ))
    }

    fn invoke_no_args(&self, _number_rows: usize) -> datafusion::common::Result<ColumnarValue> {
        Err(DataFusionError::Internal(
            "UNIX_TIMESTAMP() was not optimized away".to_string(),
        ))
    }

    fn simplify(
        &self,
        _args: Vec<Expr>,
        info: &dyn SimplifyInfo,
    ) -> datafusion::common::Result<ExprSimplifyResult> {
        let unix_time = info
            .execution_props()
            .query_execution_start_time
            .timestamp();
        Ok(ExprSimplifyResult::Simplified(Expr::Literal(
            ScalarValue::Int64(Some(unix_time)),
        )))
    }
}

fn interval_dt_duration(i: &IntervalDayTime) -> Duration {
    // TODO upgrade DF: Check we're handling, or check that we _were_ handling, interval values
    // correctly. It seems plausible there was a bug here with millis: if the representation hasn't
    // changed, then it should have been doing `(i & ((1 << 32) - 1))`.

    // let days: i64 = i.signum() * (i.abs() >> 32);
    // let millis: i64 = i.signum() * ((i.abs() << 32) >> 32);

    let duration = Duration::days(i.days as i64) + Duration::milliseconds(i.milliseconds as i64);

    duration
}

fn calc_intervals(start: NaiveDateTime, end: NaiveDateTime, interval: i32) -> i32 {
    let years_diff = end.year() - start.year();
    let months_diff = end.month() as i32 - start.month() as i32;
    let mut total_months = years_diff * 12 + months_diff;

    if total_months > 0 && end.day() < start.day() {
        total_months -= 1; // If the day in the final date is less, reduce by 1 month
    }

    let rem = months_diff % interval;
    let mut num_intervals = total_months / interval;

    if num_intervals < 0 && rem == 0 && end.day() < start.day() {
        num_intervals -= 1;
    }

    num_intervals
}

// TODO upgrade DF: Use DateTime::from_timestamp because NaiveDateTime::from_timestamp is
// deprecated?  Or does that break behavior?

/// Calculate date_bin timestamp for source date for year-month interval
fn calc_bin_timestamp_ym(origin: NaiveDateTime, source: &i64, interval: i32) -> NaiveDateTime {
    let timestamp =
        NaiveDateTime::from_timestamp(*source / 1_000_000_000, (*source % 1_000_000_000) as u32);
    let num_intervals = calc_intervals(origin, timestamp, interval);
    let nearest_date = if num_intervals >= 0 {
        origin
            .date()
            .checked_add_months(Months::new((num_intervals * interval) as u32))
            .unwrap_or(origin.date())
    } else {
        origin
            .date()
            .checked_sub_months(Months::new((-num_intervals * interval) as u32))
            .unwrap_or(origin.date())
    };

    NaiveDateTime::new(nearest_date, origin.time())
}

/// Calculate date_bin timestamp for source date for date-time interval
fn calc_bin_timestamp_dt(
    origin: NaiveDateTime,
    source: &i64,
    interval: &IntervalDayTime,
) -> NaiveDateTime {
    let timestamp =
        NaiveDateTime::from_timestamp(*source / 1_000_000_000, (*source % 1_000_000_000) as u32);
    let diff = timestamp - origin;
    let interval_duration = interval_dt_duration(&interval);
    let num_intervals =
        diff.num_nanoseconds().unwrap_or(0) / interval_duration.num_nanoseconds().unwrap_or(1);
    let mut nearest_timestamp = origin
        .checked_add_signed(interval_duration * num_intervals as i32)
        .unwrap_or(origin);

    if diff.num_nanoseconds().unwrap_or(0) < 0 {
        nearest_timestamp = nearest_timestamp
            .checked_sub_signed(interval_duration)
            .unwrap_or(origin);
    }

    nearest_timestamp
}

#[derive(Debug)]
struct DateBin {
    signature: Signature,
}
impl DateBin {
    fn new() -> DateBin {
        DateBin {
            signature: Signature {
                type_signature: TypeSignature::OneOf(vec![
                    TypeSignature::Exact(vec![
                        DataType::Interval(IntervalUnit::YearMonth),
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Interval(IntervalUnit::DayTime),
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Interval(IntervalUnit::MonthDayNano),
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                    ]),
                ]),
                volatility: Volatility::Immutable,
            },
        }
    }
}

impl ScalarUDFImpl for DateBin {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "DATE_BIN"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
    }
    fn invoke(&self, inputs: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
        assert_eq!(inputs.len(), 3);
        let interval = match &inputs[0] {
            ColumnarValue::Scalar(i) => i.clone(),
            _ => {
                // We leave this case out for simplicity.
                // CubeStore does not allow intervals inside tables, so this is super rare.
                return Err(DataFusionError::Execution(format!(
                    "Only scalar intervals are supported in DATE_BIN"
                )));
            }
        };

        let origin = match &inputs[2] {
            // TODO upgrade DF: We ignore timezone field
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(o), _tz)) => {
                NaiveDateTime::from_timestamp(*o / 1_000_000_000, (*o % 1_000_000_000) as u32)
            }
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(None, _)) => {
                return Err(DataFusionError::Execution(format!(
                    "Third argument (origin) of DATE_BIN must be a non-null timestamp"
                )));
            }
            _ => {
                // Leaving out other rare cases.
                // The initial need for the date_bin comes from custom granularities support
                // and there will always be a scalar origin point
                return Err(DataFusionError::Execution(format!(
                    "Only scalar origins are supported in DATE_BIN"
                )));
            }
        };

        fn handle_year_month(
            inputs: &[ColumnarValue],
            origin: NaiveDateTime,
            interval: i32,
        ) -> Result<ColumnarValue, DataFusionError> {
            match &inputs[1] {
                ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(None, _)) => Ok(
                    ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(None, None)),
                ),
                ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(t), _tz)) => {
                    // TODO upgrade DF: Handle _tz?
                    let nearest_timestamp = calc_bin_timestamp_ym(origin, t, interval);

                    Ok(ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                        Some(nearest_timestamp.timestamp_nanos()),
                        None, // TODO upgrade DF: handle _tz?
                    )))
                }
                ColumnarValue::Array(arr) if arr.as_any().is::<TimestampNanosecondArray>() => {
                    let ts_array = arr
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .unwrap();

                    let mut builder = TimestampNanosecondArray::builder(ts_array.len());

                    for i in 0..ts_array.len() {
                        if ts_array.is_null(i) {
                            builder.append_null();
                        } else {
                            let ts = ts_array.value(i);
                            let nearest_timestamp = calc_bin_timestamp_ym(origin, &ts, interval);
                            builder.append_value(nearest_timestamp.timestamp_nanos());
                        }
                    }

                    Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
                }
                _ => {
                    return Err(DataFusionError::Execution(format!(
                        "Second argument of DATE_BIN must be a non-null timestamp"
                    )));
                }
            }
        }

        fn handle_day_time(
            inputs: &[ColumnarValue],
            origin: NaiveDateTime,
            interval: IntervalDayTime,
        ) -> Result<ColumnarValue, DataFusionError> {
            match &inputs[1] {
                ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(None, _)) => Ok(
                    ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(None, None)),
                ),
                ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(t), _tz)) => {
                    let nearest_timestamp = calc_bin_timestamp_dt(origin, t, &interval);

                    Ok(ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                        Some(nearest_timestamp.timestamp_nanos()),
                        None, // TODO upgrade DF: Handle _tz?
                    )))
                }
                ColumnarValue::Array(arr) if arr.as_any().is::<TimestampNanosecondArray>() => {
                    let ts_array = arr
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .unwrap();

                    let mut builder = TimestampNanosecondArray::builder(ts_array.len());

                    for i in 0..ts_array.len() {
                        if ts_array.is_null(i) {
                            builder.append_null();
                        } else {
                            let ts = ts_array.value(i);
                            let nearest_timestamp = calc_bin_timestamp_dt(origin, &ts, &interval);
                            builder.append_value(nearest_timestamp.timestamp_nanos());
                        }
                    }

                    Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
                }
                _ => {
                    return Err(DataFusionError::Execution(format!(
                        "Second argument of DATE_BIN must be a non-null timestamp"
                    )));
                }
            }
        }

        match interval {
            ScalarValue::IntervalYearMonth(Some(interval)) => {
                handle_year_month(inputs, origin, interval)
            }
            ScalarValue::IntervalDayTime(Some(interval)) => {
                handle_day_time(inputs, origin, interval)
            }
            ScalarValue::IntervalMonthDayNano(Some(month_day_nano)) => {
                // We handle months or day/time but not combinations of month with day/time.
                // Potential reasons:  Before the upgrade to DF 42.2.0, there was no
                // IntervalMonthDayNano.  Also, custom granularities support doesn't need it.
                // (Also, how would it behave?)
                if month_day_nano.months != 0 {
                    if month_day_nano.days == 0 && month_day_nano.nanoseconds == 0 {
                        handle_year_month(inputs, origin, month_day_nano.months)
                    } else {
                        Err(DataFusionError::Execution(format!(
                            "Unsupported interval type (mixed month with day/time interval): {:?}",
                            interval
                        )))
                    }
                } else {
                    let milliseconds64 = month_day_nano.nanoseconds / 1_000_000;
                    let milliseconds32 = i32::try_from(milliseconds64).map_err(|_| {
                        DataFusionError::Execution(format!(
                        "Unsupported interval time value ({} nanoseconds is out of range): {:?}",
                        month_day_nano.nanoseconds,
                        interval
                    ))
                    })?;
                    // TODO upgrade DF: Pass nanoseconds to handle_day_time?
                    handle_day_time(
                        inputs,
                        origin,
                        IntervalDayTime::new(month_day_nano.days, milliseconds32),
                    )
                }
            }
            _ => Err(DataFusionError::Execution(format!(
                "Unsupported interval type: {:?}",
                interval
            ))),
        }
    }
}

#[derive(Debug)]
struct DateAddSub {
    is_add: bool,
    signature: Signature,
}

impl DateAddSub {
    pub fn new(is_add: bool) -> DateAddSub {
        DateAddSub {
            is_add,
            signature: Signature {
                type_signature: TypeSignature::OneOf(vec![
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        DataType::Interval(IntervalUnit::YearMonth),
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        DataType::Interval(IntervalUnit::DayTime),
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        DataType::Interval(IntervalUnit::MonthDayNano),
                    ]),
                ]),
                volatility: Volatility::Immutable,
            },
        }
    }
    pub fn new_add() -> DateAddSub {
        Self::new(true)
    }
    pub fn new_sub() -> DateAddSub {
        Self::new(false)
    }
}

impl DateAddSub {
    fn name_static(&self) -> &'static str {
        match self.is_add {
            true => "DATE_ADD",
            false => "DATE_SUB",
        }
    }
}

impl ScalarUDFImpl for DateAddSub {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        self.name_static()
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
    }
    fn invoke(&self, inputs: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
        use datafusion::arrow::compute::kernels::numeric::add;
        use datafusion::arrow::compute::kernels::numeric::sub;
        assert_eq!(inputs.len(), 2);
        // DF 42.2.0 already has date + interval or date - interval.  Note that `add` and `sub` are
        // public (defined in arrow_arith), while timestamp-specific functions they invoke,
        // `arithmetic_op` and then `timestamp_op::<TimestampNanosecondType>`, are not.
        //
        // TODO upgrade DF: Double-check that the TypeSignature is actually enforced.
        datafusion::physical_expr_common::datum::apply(
            &inputs[0],
            &inputs[1],
            if self.is_add { add } else { sub },
        )
    }
}

#[derive(Debug)]
struct HllCardinality {
    signature: Signature,
}
impl HllCardinality {
    pub fn new() -> HllCardinality {
        // TODO upgrade DF: Is it Volatile or Immutable?
        let signature = Signature::new(
            TypeSignature::Exact(vec![DataType::Binary]),
            Volatility::Volatile,
        );

        HllCardinality { signature }
    }
    fn descriptor() -> ScalarUDF {
        return ScalarUDF::new_from_impl(HllCardinality::new());
    }
}

impl ScalarUDFImpl for HllCardinality {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "CARDINALITY"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(DataType::UInt64)
    }
    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
        assert_eq!(args.len(), 1);
        let sketches = args[0].clone().into_array(1)?;
        let sketches = sketches
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("expected binary data");

        let mut r = UInt64Builder::with_capacity(sketches.len());
        for s in sketches {
            match s {
                None => r.append_null(),
                Some(d) => {
                    if d.len() == 0 {
                        r.append_value(0)
                    } else {
                        r.append_value(read_sketch(d)?.cardinality())
                    }
                }
            }
        }
        return Ok(ColumnarValue::Array(Arc::new(r.finish())));
    }
    fn aliases(&self) -> &[String] {
        &[]
    }
}

#[derive(Debug)]
struct HllMergeUDF {
    signature: Signature,
}
impl HllMergeUDF {
    fn new() -> HllMergeUDF {
        HllMergeUDF {
            signature: Signature::exact(vec![DataType::Binary], Volatility::Stable),
        }
    }
}

impl AggregateUDFImpl for HllMergeUDF {
    fn name(&self) -> &str {
        return "merge";
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::Binary)
    }

    fn accumulator(
        &self,
        acc_args: AccumulatorArgs,
    ) -> datafusion::common::Result<Box<dyn Accumulator>> {
        Ok(Box::new(HllMergeAccumulator { acc: None }))
    }
}

#[derive(Debug)]
struct HllMergeAccumulator {
    // TODO: store sketch for empty set from the start.
    //       this requires storing index_bit_len in the type.
    acc: Option<HllUnion>,
}

impl Accumulator for HllMergeAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<(), DataFusionError> {
        assert_eq!(values.len(), 1);

        if let Some(value_rows) = values[0].as_any().downcast_ref::<BinaryArray>() {
            for opt_datum in value_rows {
                if let Some(data) = opt_datum {
                    if data.len() != 0 {
                        self.merge_sketch(read_sketch(&data)?)?;
                    } else {
                        // empty state is ok, this means an empty sketch.
                    }
                } else {
                    // ignore NULL.
                }
            }
            return Ok(());
        } else {
            return Err(CubeError::internal(
                "invalid array type passed to update_batch, expecting HLL sketches".to_string(),
            )
            .into());
        }
    }

    fn evaluate(&mut self) -> Result<ScalarValue, DataFusionError> {
        let v;
        match &self.acc {
            None => v = Vec::new(),
            Some(s) => v = s.write(),
        }
        return Ok(ScalarValue::Binary(Some(v)));
    }

    fn size(&self) -> usize {
        let hllu_allocated_size = if let Some(hllu) = &self.acc {
            hllu.allocated_size()
        } else {
            0
        };
        size_of::<Self>() + hllu_allocated_size
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>, DataFusionError> {
        return Ok(vec![self.evaluate()?]);
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<(), DataFusionError> {
        assert_eq!(states.len(), 1);

        if let Some(value_rows) = states[0].as_any().downcast_ref::<BinaryArray>() {
            for opt_datum in value_rows {
                if let Some(data) = opt_datum {
                    if data.len() != 0 {
                        self.merge_sketch(read_sketch(&data)?)?;
                    } else {
                        // empty state is ok, this means an empty sketch.
                    }
                } else {
                    // ignore NULL.
                }
            }
            return Ok(());
        } else {
            return Err(CubeError::internal("invalid state in MERGE".to_string()).into());
        }
    }
}

impl HllMergeAccumulator {
    fn merge_sketch(&mut self, s: Hll) -> Result<(), DataFusionError> {
        if self.acc.is_none() {
            self.acc = Some(HllUnion::new(s)?);
            return Ok(());
        } else if let Some(acc_s) = &mut self.acc {
            if !acc_s.is_compatible(&s) {
                return Err(CubeError::internal(
                    "cannot merge two incompatible HLL sketches".to_string(),
                )
                .into());
            }
            acc_s.merge_with(s)?;
        } else {
            unreachable!("impossible");
        }
        return Ok(());
    }
}

pub fn read_sketch(data: &[u8]) -> Result<Hll, DataFusionError> {
    return Hll::read(&data).map_err(|e| DataFusionError::Execution(e.message));
}
