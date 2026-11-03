use crate::queryplanner::hll::{Hll, HllUnion};
use crate::queryplanner::udf_xirr::{XirrUDF, XIRR_UDAF_NAME};
use crate::CubeError;
use chrono::{DateTime, Datelike, Duration, Months, NaiveDateTime, Timelike, Utc};
use datafusion::arrow::array::temporal_conversions::{
    as_datetime, MICROSECONDS, MILLISECONDS, NANOSECONDS,
};
use datafusion::arrow::array::{
    new_null_array, Array, ArrayRef, AsArray, BinaryArray, PrimitiveArray, StringArray,
    UInt64Builder,
};
use datafusion::arrow::compute::kernels::cast_utils::string_to_datetime;
use datafusion::arrow::datatypes::{
    ArrowTimestampType, DataType, IntervalUnit, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
};
use datafusion::error::DataFusionError;
use datafusion::functions::datetime::date_trunc::DateTruncFunc;
use datafusion::functions::datetime::to_timestamp::ToTimestampMicrosFunc;
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion::logical_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion::logical_expr::type_coercion::functions::data_types;
use datafusion::logical_expr::{
    AggregateUDF, AggregateUDFImpl, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility, TIMEZONE_WILDCARD,
};
use datafusion::physical_plan::{Accumulator, ColumnarValue};
use datafusion::scalar::ScalarValue;
use serde_derive::{Deserialize, Serialize};
use std::any::Any;
use std::sync::Arc;
use std::time::SystemTime;

/// `timestamp_micros` picks the unit date function arguments are coerced to while planning (see
/// `ConfigObj::timestamp_micros`). The functions themselves accept either unit, so a node that only
/// executes received plans can pass any value.
pub fn registerable_scalar_udfs_iter(timestamp_micros: bool) -> impl Iterator<Item = ScalarUDF> {
    let unit = planning_time_unit(timestamp_micros);
    [
        ScalarUDF::new_from_impl(HllCardinality::new()),
        ScalarUDF::new_from_impl(DateBin::new(unit)),
        ScalarUDF::new_from_impl(DateAddSub::new_add(unit)),
        ScalarUDF::new_from_impl(DateAddSub::new_sub(unit)),
        ScalarUDF::new_from_impl(UnixTimestamp::new()),
        ScalarUDF::new_from_impl(ConvertTz::new(unit)),
        ScalarUDF::new_from_impl(Now::new()),
        ScalarUDF::new_from_impl(CubeDateTrunc::new(unit)),
        ScalarUDF::new_from_impl(CubeToTimestampMicros::new()),
    ]
    .into_iter()
}

fn planning_time_unit(timestamp_micros: bool) -> TimeUnit {
    if timestamp_micros {
        TimeUnit::Microsecond
    } else {
        TimeUnit::Nanosecond
    }
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub enum CubeAggregateUDFKind {
    MergeHll, // merge(), accepting the HyperLogLog sketches.
    Xirr,
}

pub fn registerable_aggregate_udfs_iter() -> impl Iterator<Item = AggregateUDF> {
    [
        AggregateUDF::new_from_impl(HllMergeUDF::new()),
        AggregateUDF::new_from_impl(XirrUDF::new()),
    ]
    .into_iter()
}

pub fn aggregate_udf_by_kind(k: CubeAggregateUDFKind) -> AggregateUDF {
    match k {
        CubeAggregateUDFKind::MergeHll => AggregateUDF::new_from_impl(HllMergeUDF::new()),
        CubeAggregateUDFKind::Xirr => AggregateUDF::new_from_impl(XirrUDF::new()),
    }
}

/// Note that only full match counts. Pass lowercase names.
pub fn aggregate_kind_by_name(n: &str) -> Option<CubeAggregateUDFKind> {
    if n == "merge" {
        return Some(CubeAggregateUDFKind::MergeHll);
    }
    if n == XIRR_UDAF_NAME {
        return Some(CubeAggregateUDFKind::Xirr);
    }
    return None;
}

// The rest of the file are implementations of the various functions that we have.
// TODO: add custom type and use it instead of `Binary` for HLL columns.

#[derive(Debug)]
struct Now {
    signature: Signature,
}

impl Now {
    fn new() -> Self {
        Now {
            signature: Self::signature(),
        }
    }

    fn signature() -> Signature {
        Signature::exact(Vec::new(), Volatility::Stable)
    }
}

impl ScalarUDFImpl for Now {
    fn name(&self) -> &str {
        "NOW"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
    }

    fn invoke_with_args(
        &self,
        _args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        let t = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
            Ok(t) => t,
            Err(e) => {
                return Err(DataFusionError::Internal(format!(
                    "Failed to get current timestamp: {}",
                    e
                )))
            }
        };

        let nanos = match i64::try_from(t.as_nanos()) {
            Ok(t) => t,
            Err(e) => {
                return Err(DataFusionError::Internal(format!(
                    "Failed to convert timestamp to i64: {}",
                    e
                )))
            }
        };

        Ok(ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
            Some(nanos),
            None,
        )))
    }
}

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
        "unix_timestamp"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(
        &self,
        _args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        let t = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
            Ok(t) => t,
            Err(e) => {
                return Err(DataFusionError::Internal(format!(
                    "Failed to get current timestamp: {}",
                    e
                )))
            }
        };

        let seconds = match i64::try_from(t.as_secs()) {
            Ok(t) => t,
            Err(e) => {
                return Err(DataFusionError::Internal(format!(
                    "Failed to convert timestamp to i64: {}",
                    e
                )))
            }
        };

        Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(seconds))))
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

fn interval_dt_duration(interval_days: i32, interval_nanos: i64) -> Duration {
    Duration::days(interval_days as i64) + Duration::nanoseconds(interval_nanos)
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

/// Calculate date_bin timestamp for source date for year-month interval
fn calc_bin_timestamp_ym(
    origin: NaiveDateTime,
    timestamp: NaiveDateTime,
    interval: i32,
) -> NaiveDateTime {
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
    timestamp: NaiveDateTime,
    interval_days: i32,
    interval_nanos: i64,
) -> NaiveDateTime {
    // In i128 nanoseconds: i64 ones only span 292 years between the origin and the timestamp.
    let diff = duration_nanos(timestamp - origin);
    let interval = match duration_nanos(interval_dt_duration(interval_days, interval_nanos)) {
        0 => 1,
        interval => interval,
    };
    let mut num_intervals = diff / interval;
    if diff < 0 {
        num_intervals -= 1;
    }
    let offset = num_intervals * interval;
    let offset = Duration::seconds((offset / 1_000_000_000) as i64)
        + Duration::nanoseconds((offset % 1_000_000_000) as i64);
    origin.checked_add_signed(offset).unwrap_or(origin)
}

fn duration_nanos(d: Duration) -> i128 {
    d.num_seconds() as i128 * 1_000_000_000 + d.subsec_nanos() as i128
}

// Evaluates `$body` with `$T` bound to the Arrow timestamp type of `$unit`.
macro_rules! with_timestamp_type {
    ($unit:expr, $T:ident => $body:expr) => {
        match $unit {
            TimeUnit::Second => {
                type $T = TimestampSecondType;
                $body
            }
            TimeUnit::Millisecond => {
                type $T = TimestampMillisecondType;
                $body
            }
            TimeUnit::Microsecond => {
                type $T = TimestampMicrosecondType;
                $body
            }
            TimeUnit::Nanosecond => {
                type $T = TimestampNanosecondType;
                $body
            }
        }
    };
}

fn timestamp_to_naive(value: i64, unit: &TimeUnit) -> Result<NaiveDateTime, DataFusionError> {
    with_timestamp_type!(unit, T => as_datetime::<T>(value)).ok_or_else(|| {
        DataFusionError::Execution(format!("Timestamp {} {:?} is out of range", value, unit))
    })
}

fn naive_to_timestamp(value: NaiveDateTime, unit: &TimeUnit) -> Result<i64, DataFusionError> {
    with_timestamp_type!(unit, T => T::make_value(value)).ok_or_else(|| {
        DataFusionError::Execution(format!(
            "Timestamp {} can't be represented in {:?}",
            value, unit
        ))
    })
}

// The result keeps the unit and drops the time zone.
fn map_timestamp_array(
    array: &dyn Array,
    f: impl Fn(i64) -> Result<i64, DataFusionError>,
) -> Result<ArrayRef, DataFusionError> {
    match array.data_type() {
        DataType::Timestamp(unit, _) => with_timestamp_type!(unit, T => {
            let result: PrimitiveArray<T> = array.as_primitive::<T>().try_unary(f)?;
            Ok(Arc::new(result) as ArrayRef)
        }),
        other => Err(DataFusionError::Execution(format!(
            "Expected a timestamp array, got {}",
            other
        ))),
    }
}

// The result keeps the unit and has no time zone.
fn map_timestamp_value(
    value: &ColumnarValue,
    f: impl Fn(i64, &TimeUnit) -> Result<i64, DataFusionError>,
) -> Result<ColumnarValue, DataFusionError> {
    let unit = match value.data_type() {
        DataType::Timestamp(unit, _) => unit,
        other => {
            return Err(DataFusionError::Execution(format!(
                "Expected a timestamp, got {}",
                other
            )))
        }
    };
    match value {
        ColumnarValue::Array(array) => Ok(ColumnarValue::Array(map_timestamp_array(
            array.as_ref(),
            |v| f(v, &unit),
        )?)),
        ColumnarValue::Scalar(scalar) => {
            let array = scalar.to_array_of_size(1)?;
            let result = map_timestamp_array(array.as_ref(), |v| f(v, &unit))?;
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                result.as_ref(),
                0,
            )?))
        }
    }
}

fn scalar_timestamp(value: &ScalarValue) -> Option<(i64, TimeUnit)> {
    match value {
        ScalarValue::TimestampSecond(Some(v), _) => Some((*v, TimeUnit::Second)),
        ScalarValue::TimestampMillisecond(Some(v), _) => Some((*v, TimeUnit::Millisecond)),
        ScalarValue::TimestampMicrosecond(Some(v), _) => Some((*v, TimeUnit::Microsecond)),
        ScalarValue::TimestampNanosecond(Some(v), _) => Some((*v, TimeUnit::Nanosecond)),
        _ => None,
    }
}

// DataFusion coerces strings only to nanosecond timestamps, so for microseconds the arguments at
// `positions` are retyped before coercing by `signature`.
fn coerce_timestamp_args(
    name: &str,
    arg_types: &[DataType],
    positions: &[usize],
    unit: &TimeUnit,
    signature: &Signature,
) -> Result<Vec<DataType>, DataFusionError> {
    let mut arg_types = arg_types.to_vec();
    if *unit == TimeUnit::Microsecond {
        for i in positions {
            if let Some(arg) = arg_types.get_mut(*i) {
                match arg {
                    DataType::Utf8
                    | DataType::LargeUtf8
                    | DataType::Utf8View
                    | DataType::Date32
                    | DataType::Null => *arg = timestamp(unit),
                    DataType::Timestamp(arg_unit, _) => *arg_unit = unit.clone(),
                    _ => {}
                }
            }
        }
    }
    data_types(name, &arg_types, signature)
}

fn timestamp(unit: &TimeUnit) -> DataType {
    DataType::Timestamp(unit.clone(), None)
}

fn timestamp_arg_unit(arg_types: &[DataType], i: usize) -> Result<TimeUnit, DataFusionError> {
    match arg_types.get(i) {
        Some(DataType::Timestamp(unit, _)) => Ok(unit.clone()),
        other => Err(DataFusionError::Internal(format!(
            "Expected a timestamp argument at {}, got {:?}",
            i, other
        ))),
    }
}

#[derive(Debug)]
struct DateBin {
    signature: Signature,
    coercion: Signature,
    unit: TimeUnit,
}
impl DateBin {
    fn new(unit: TimeUnit) -> DateBin {
        let exact = |interval| {
            TypeSignature::Exact(vec![
                DataType::Interval(interval),
                timestamp(&unit),
                timestamp(&unit),
            ])
        };
        DateBin {
            signature: Signature::user_defined(Volatility::Immutable),
            coercion: Signature::one_of(
                vec![
                    exact(IntervalUnit::YearMonth),
                    exact(IntervalUnit::DayTime),
                    exact(IntervalUnit::MonthDayNano),
                ],
                Volatility::Immutable,
            ),
            unit,
        }
    }
}

impl ScalarUDFImpl for DateBin {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "date_bin"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>, DataFusionError> {
        coerce_timestamp_args(self.name(), arg_types, &[1, 2], &self.unit, &self.coercion)
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(timestamp(&timestamp_arg_unit(arg_types, 1)?))
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

        // Time zones are ignored: bins are computed in UTC and the result has no time zone.
        let origin = match &inputs[2] {
            ColumnarValue::Scalar(origin) => match scalar_timestamp(origin) {
                Some((origin, unit)) => timestamp_to_naive(origin, &unit)?,
                None => {
                    return Err(DataFusionError::Execution(format!(
                        "Third argument (origin) of DATE_BIN must be a non-null timestamp"
                    )));
                }
            },
            _ => {
                // Leaving out other rare cases.
                // The initial need for the date_bin comes from custom granularities support
                // and there will always be a scalar origin point
                return Err(DataFusionError::Execution(format!(
                    "Only scalar origins are supported in DATE_BIN"
                )));
            }
        };

        if !matches!(inputs[1].data_type(), DataType::Timestamp(_, _)) {
            return Err(DataFusionError::Execution(format!(
                "Second argument of DATE_BIN must be a non-null timestamp"
            )));
        }

        let bin = |calc: &dyn Fn(NaiveDateTime) -> NaiveDateTime| {
            map_timestamp_value(&inputs[1], |v, unit| {
                naive_to_timestamp(calc(timestamp_to_naive(v, unit)?), unit)
            })
        };
        let year_month = |interval: i32| bin(&|t| calc_bin_timestamp_ym(origin, t, interval));
        let day_time =
            |days: i32, nanos: i64| bin(&|t| calc_bin_timestamp_dt(origin, t, days, nanos));

        match interval {
            ScalarValue::IntervalYearMonth(Some(interval)) => year_month(interval),
            ScalarValue::IntervalDayTime(Some(interval)) => {
                day_time(interval.days, (interval.milliseconds as i64) * 1_000_000)
            }
            ScalarValue::IntervalMonthDayNano(Some(month_day_nano)) => {
                // We handle months or day/time but not combinations of month with day/time.
                // Potential reasons:  Before the upgrade to DF 42.2.0, there was no
                // IntervalMonthDayNano.  Also, custom granularities support doesn't need it.
                // (Also, how would it behave?)
                if month_day_nano.months != 0 {
                    if month_day_nano.days == 0 && month_day_nano.nanoseconds == 0 {
                        year_month(month_day_nano.months)
                    } else {
                        Err(DataFusionError::Execution(format!(
                            "Unsupported interval type (mixed month with day/time interval): {:?}",
                            interval
                        )))
                    }
                } else {
                    day_time(month_day_nano.days, month_day_nano.nanoseconds)
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
    coercion: Signature,
    unit: TimeUnit,
}

impl DateAddSub {
    pub fn new(is_add: bool, unit: TimeUnit) -> DateAddSub {
        let tz_wildcard: Arc<str> = Arc::from(TIMEZONE_WILDCARD);
        let exact = |tz: Option<Arc<str>>, interval| {
            TypeSignature::Exact(vec![
                DataType::Timestamp(unit.clone(), tz),
                DataType::Interval(interval),
            ])
        };
        DateAddSub {
            is_add,
            signature: Signature::user_defined(Volatility::Immutable),
            coercion: Signature::one_of(
                vec![
                    exact(None, IntervalUnit::YearMonth),
                    exact(None, IntervalUnit::DayTime),
                    exact(None, IntervalUnit::MonthDayNano),
                    // We wanted this for NOW(), which has "+00:00" time zone.  Using
                    // TIMEZONE_WILDCARD to favor DST-related questions over "UTC" == "+00:00"
                    // questions.  MySQL doesn't have a timezone as this function is applied, and
                    // we simply invoke DF's date + interval behavior.
                    exact(Some(tz_wildcard.clone()), IntervalUnit::YearMonth),
                    exact(Some(tz_wildcard.clone()), IntervalUnit::DayTime),
                    exact(Some(tz_wildcard), IntervalUnit::MonthDayNano),
                ],
                Volatility::Immutable,
            ),
            unit,
        }
    }
    pub fn new_add(unit: TimeUnit) -> DateAddSub {
        Self::new(true, unit)
    }
    pub fn new_sub(unit: TimeUnit) -> DateAddSub {
        Self::new(false, unit)
    }
}

impl DateAddSub {
    fn name_static(&self) -> &'static str {
        match self.is_add {
            true => "date_add",
            false => "date_sub",
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
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>, DataFusionError> {
        coerce_timestamp_args(self.name(), arg_types, &[0], &self.unit, &self.coercion)
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        if arg_types.len() != 2 {
            return Err(DataFusionError::Internal(format!(
                "DateAddSub return_type expects 2 arguments, got {:?}",
                arg_types
            )));
        }
        match (&arg_types[0], &arg_types[1]) {
            (ts @ DataType::Timestamp(_, _), DataType::Interval(_)) => Ok(ts.clone()),
            _ => Err(DataFusionError::Internal(format!(
                "DateAddSub return_type expects Timestamp and Interval arguments, got {:?}",
                arg_types
            ))),
        }
    }
    fn invoke(&self, inputs: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
        use datafusion::arrow::compute::kernels::numeric::add;
        use datafusion::arrow::compute::kernels::numeric::sub;
        assert_eq!(inputs.len(), 2);
        // DF 42.2.0 already has date + interval or date - interval.  Note that `add` and `sub` are
        // public (defined in arrow_arith), while timestamp-specific functions they invoke,
        // Arrow's `arithmetic_op` and then `timestamp_op::<TimestampNanosecondType>`, are not.
        datafusion::physical_expr_common::datum::apply(
            &inputs[0],
            &inputs[1],
            if self.is_add { add } else { sub },
        )
    }
}

#[derive(Debug)]
pub(crate) struct HllCardinality {
    signature: Signature,
}
impl HllCardinality {
    pub fn new() -> HllCardinality {
        let signature = Signature::new(
            TypeSignature::Exact(vec![DataType::Binary]),
            Volatility::Immutable,
        );

        HllCardinality { signature }
    }

    /// Lets us call [`ScalarFunctionExpr::new`] in some cases without elaborately computing return
    /// type or using [`ScalarFunctionExpr::try_new`].
    pub fn static_return_type() -> DataType {
        DataType::UInt64
    }

    pub fn static_name() -> &'static str {
        "cardinality"
    }
}

impl ScalarUDFImpl for HllCardinality {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        Self::static_name()
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(Self::static_return_type())
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
pub(crate) struct HllMergeUDF {
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

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::Binary)
    }

    fn accumulator(
        &self,
        _acc_args: AccumulatorArgs,
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
        self.peek_evaluate()
    }

    // Cube ext:
    fn peek_evaluate(&self) -> Result<ScalarValue, DataFusionError> {
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

    fn reset(&mut self) -> Result<(), DataFusionError> {
        self.acc = None;
        Ok(())
    }
    fn peek_state(&self) -> Result<Vec<ScalarValue>, DataFusionError> {
        Ok(vec![self.peek_evaluate()?])
    }
    fn supports_cube_ext(&self) -> bool {
        true
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

#[derive(Debug)]
struct ConvertTz {
    signature: Signature,
    coercion: Signature,
    unit: TimeUnit,
}

impl ConvertTz {
    fn new(unit: TimeUnit) -> ConvertTz {
        ConvertTz {
            signature: Signature::user_defined(Volatility::Immutable),
            coercion: Signature::exact(
                vec![timestamp(&unit), DataType::Utf8],
                Volatility::Immutable,
            ),
            unit,
        }
    }
}

impl ScalarUDFImpl for ConvertTz {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "convert_tz"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>, DataFusionError> {
        coerce_timestamp_args(self.name(), arg_types, &[0], &self.unit, &self.coercion)
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(timestamp(&timestamp_arg_unit(arg_types, 0)?))
    }
    fn invoke(&self, inputs: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
        match (&inputs[0], &inputs[1]) {
            (t, ColumnarValue::Scalar(ScalarValue::Utf8(None))) => {
                let null_type = timestamp(&timestamp_arg_unit(&[t.data_type()], 0)?);
                Ok(match t {
                    ColumnarValue::Array(t) => {
                        ColumnarValue::Array(new_null_array(&null_type, t.len()))
                    }
                    ColumnarValue::Scalar(_) => {
                        ColumnarValue::Scalar(ScalarValue::try_from(&null_type)?)
                    }
                })
            }
            (t, ColumnarValue::Scalar(ScalarValue::Utf8(Some(shift)))) => {
                let shift = convert_tz_compute_shift_seconds(shift)?;
                map_timestamp_value(t, |v, unit| apply_tz_shift(v, shift, unit))
            }
            (t, ColumnarValue::Array(shift)) => {
                let t = t.to_array(shift.len())?;
                convert_tz(&t, shift).map(ColumnarValue::Array)
            }
            _ => Err(DataFusionError::Internal(
                "Unsupported input type in convert_tz".to_string(),
            )),
        }
    }
}

fn convert_tz_compute_shift_seconds(shift: &str) -> Result<i64, DataFusionError> {
    let hour_min = shift.split(':').collect::<Vec<_>>();
    if hour_min.len() != 2 {
        return Err(DataFusionError::Execution(format!(
            "Can't parse timezone shift '{}'",
            shift
        )));
    }
    let hour = hour_min[0].parse::<i64>().map_err(|e| {
        DataFusionError::Execution(format!(
            "Can't parse hours of timezone shift '{}': {}",
            hour_min[0], e
        ))
    })?;
    let minute = hour_min[1].parse::<i64>().map_err(|e| {
        DataFusionError::Execution(format!(
            "Can't parse minutes of timezone shift '{}': {}",
            hour_min[1], e
        ))
    })?;
    Ok((hour * 60 + hour.signum() * minute) * 60)
}

fn apply_tz_shift(value: i64, shift_seconds: i64, unit: &TimeUnit) -> Result<i64, DataFusionError> {
    let per_second = match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => MILLISECONDS,
        TimeUnit::Microsecond => MICROSECONDS,
        TimeUnit::Nanosecond => NANOSECONDS,
    };
    shift_seconds
        .checked_mul(per_second)
        .and_then(|shift| value.checked_add(shift))
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "Timestamp {} {:?} is out of range after convert_tz",
                value, unit
            ))
        })
}

/// convert_tz SQL function. A NULL timestamp or shift gives NULL.
pub fn convert_tz(args_0: &ArrayRef, args_1: &ArrayRef) -> Result<ArrayRef, DataFusionError> {
    let unit = timestamp_arg_unit(&[args_0.data_type().clone()], 0)?;
    let shift = args_1
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            DataFusionError::Execution(
                "Could not cast convert_tz shift input to StringArray".to_string(),
            )
        })?;
    with_timestamp_type!(&unit, T => {
        let result = args_0
            .as_primitive::<T>()
            .iter()
            .zip(shift.iter())
            .map(|(t, shift)| match (t, shift) {
                (Some(t), Some(shift)) => {
                    let shift = convert_tz_compute_shift_seconds(shift)?;
                    apply_tz_shift(t, shift, &unit).map(Some)
                }
                _ => Ok(None),
            })
            .collect::<Result<PrimitiveArray<T>, DataFusionError>>()?;
        Ok(Arc::new(result) as ArrayRef)
    })
}

/// DataFusion's `date_trunc` computes in nanoseconds whatever unit it is given, so a microsecond
/// timestamp past 2262-04-11 overflows. This one truncates microseconds through chrono and leaves
/// every other input to DataFusion's implementation.
#[derive(Debug)]
pub struct CubeDateTrunc {
    inner: DateTruncFunc,
    signature: Signature,
    unit: TimeUnit,
}

impl CubeDateTrunc {
    pub fn new(unit: TimeUnit) -> CubeDateTrunc {
        CubeDateTrunc {
            inner: DateTruncFunc::new(),
            signature: Signature::user_defined(Volatility::Immutable),
            unit,
        }
    }
}

impl ScalarUDFImpl for CubeDateTrunc {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>, DataFusionError> {
        coerce_timestamp_args(
            self.name(),
            arg_types,
            &[1],
            &self.unit,
            self.inner.signature(),
        )
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        self.inner.return_type(arg_types)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue, DataFusionError> {
        let granularity = match &args.args[0] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(v)))
            | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(v))) => v.to_lowercase(),
            _ => return self.inner.invoke_with_args(args),
        };
        if args.args[1].data_type() != timestamp(&TimeUnit::Microsecond) {
            return self.inner.invoke_with_args(args);
        }
        map_timestamp_value(&args.args[1], |v, _| date_trunc_micros(&granularity, v))
    }
    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties, DataFusionError> {
        self.inner.output_ordering(input)
    }
    fn documentation(&self) -> Option<&datafusion::logical_expr::Documentation> {
        self.inner.documentation()
    }
}

/// DataFusion's `to_timestamp_micros` parses strings to nanoseconds and divides, so it fails past
/// 2262-04-11. This one parses a string straight to microseconds and leaves every other input
/// (numbers, timestamps, explicit formats) to DataFusion's implementation.
#[derive(Debug)]
pub struct CubeToTimestampMicros {
    inner: ToTimestampMicrosFunc,
}

impl CubeToTimestampMicros {
    pub fn new() -> CubeToTimestampMicros {
        CubeToTimestampMicros {
            inner: ToTimestampMicrosFunc::new(),
        }
    }
}

impl ScalarUDFImpl for CubeToTimestampMicros {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn signature(&self) -> &Signature {
        self.inner.signature()
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        self.inner.return_type(arg_types)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue, DataFusionError> {
        let is_string = matches!(
            args.args[0].data_type(),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        );
        if args.args.len() != 1 || !is_string {
            return self.inner.invoke_with_args(args);
        }
        let parse = |array: &dyn Array| -> Result<ArrayRef, DataFusionError> {
            let array = datafusion::arrow::compute::cast(array, &DataType::Utf8)?;
            let result = array
                .as_string::<i32>()
                .iter()
                .map(|s| {
                    s.map(|s| {
                        string_to_datetime(&Utc, s)
                            .map(|ts| ts.timestamp_micros())
                            .map_err(|e| {
                                DataFusionError::Execution(format!(
                                    "Error parsing timestamp from '{}': {}",
                                    s, e
                                ))
                            })
                    })
                    .transpose()
                })
                .collect::<Result<PrimitiveArray<TimestampMicrosecondType>, DataFusionError>>()?;
            Ok(Arc::new(result))
        };
        match &args.args[0] {
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(parse(array.as_ref())?)),
            ColumnarValue::Scalar(scalar) => {
                let result = parse(scalar.to_array_of_size(1)?.as_ref())?;
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    result.as_ref(),
                    0,
                )?))
            }
        }
    }
    fn documentation(&self) -> Option<&datafusion::logical_expr::Documentation> {
        self.inner.documentation()
    }
}

/// Truncates a microsecond timestamp the way DataFusion's `date_trunc` does for a timestamp without
/// a time zone, including its rounding toward zero for sub-second granularities.
pub fn date_trunc_micros(granularity: &str, micros: i64) -> Result<i64, DataFusionError> {
    let out_of_range = || {
        DataFusionError::Execution(format!(
            "Timestamp {} is out of range for date_trunc",
            micros
        ))
    };
    let start_of_day = |d: NaiveDateTime| d.date().and_hms_opt(0, 0, 0).unwrap();
    let value = DateTime::from_timestamp_micros(micros)
        .ok_or_else(out_of_range)?
        .naive_utc();
    let truncated = match granularity {
        "microsecond" => return Ok(micros),
        "millisecond" => return Ok(micros / 1_000 * 1_000),
        "second" => value.with_nanosecond(0),
        "minute" => value.with_nanosecond(0).and_then(|d| d.with_second(0)),
        "hour" => value
            .with_nanosecond(0)
            .and_then(|d| d.with_second(0))
            .and_then(|d| d.with_minute(0)),
        "day" => Some(start_of_day(value)),
        "week" => start_of_day(value)
            .checked_sub_signed(Duration::days(value.weekday().num_days_from_monday() as i64)),
        "month" => start_of_day(value).with_day0(0),
        "quarter" => start_of_day(value)
            .with_day0(0)
            .and_then(|d| d.with_month(1 + 3 * ((d.month() - 1) / 3))),
        "year" => start_of_day(value)
            .with_day0(0)
            .and_then(|d| d.with_month0(0)),
        unsupported => {
            return Err(DataFusionError::Execution(format!(
                "Unsupported date_trunc granularity: {}",
                unsupported
            )))
        }
    };
    Ok(truncated
        .ok_or_else(out_of_range)?
        .and_utc()
        .timestamp_micros())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::timestamp_from_string;
    use datafusion::arrow::array::{TimestampMicrosecondArray, TimestampNanosecondArray};
    use itertools::Itertools;

    const GRANULARITIES: [&str; 10] = [
        "microsecond",
        "millisecond",
        "second",
        "minute",
        "hour",
        "day",
        "week",
        "month",
        "quarter",
        "year",
    ];

    fn micros(s: &str) -> i64 {
        timestamp_from_string(s).unwrap().micros()
    }

    fn micros_array(values: Vec<Option<i64>>) -> ColumnarValue {
        ColumnarValue::Array(Arc::new(TimestampMicrosecondArray::from(values)))
    }

    fn call(udf: &dyn ScalarUDFImpl, args: Vec<ColumnarValue>) -> ColumnarValue {
        let arg_types = args.iter().map(|a| a.data_type()).collect_vec();
        let number_rows = args
            .iter()
            .find_map(|a| match a {
                ColumnarValue::Array(a) => Some(a.len()),
                ColumnarValue::Scalar(_) => None,
            })
            .unwrap_or(1);
        udf.invoke_with_args(ScalarFunctionArgs {
            args,
            number_rows,
            return_type: &udf.return_type(&arg_types).unwrap(),
        })
        .unwrap()
    }

    fn micros_values(value: ColumnarValue) -> Vec<Option<i64>> {
        let array = value.into_array(1).unwrap();
        array
            .as_primitive::<TimestampMicrosecondType>()
            .iter()
            .collect_vec()
    }

    fn datafusion_date_trunc(granularity: &str, values: &[i64]) -> Vec<i64> {
        let result = DateTruncFunc::new()
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some(granularity.to_string()))),
                    micros_array(values.iter().map(|v| Some(*v)).collect()),
                ],
                number_rows: values.len(),
                return_type: &timestamp(&TimeUnit::Microsecond),
            })
            .unwrap();
        micros_values(result)
            .into_iter()
            .map(|v| v.unwrap())
            .collect()
    }

    #[test]
    fn date_trunc_micros_matches_datafusion_in_nanosecond_range() {
        let values = [
            0,
            1,
            -1,
            999,
            -999,
            1_000,
            -1_001,
            1_584_272_707_123_456,  // 2020-03-15 11:45:07.123456
            -8_506_049_394_749_999, // 1700-06-15 08:30:05.250001
            9_214_646_400_000_000,  // 2262-01-01
        ];
        for granularity in GRANULARITIES {
            let actual = values
                .iter()
                .map(|v| date_trunc_micros(granularity, *v).unwrap())
                .collect_vec();
            assert_eq!(
                actual,
                datafusion_date_trunc(granularity, &values),
                "{}",
                granularity
            );
        }
    }

    #[test]
    fn date_trunc_micros_past_nanosecond_range() {
        let value = micros("2432-03-17T13:45:07.123456Z");
        for (granularity, expected) in [
            ("microsecond", "2432-03-17T13:45:07.123456Z"),
            ("millisecond", "2432-03-17T13:45:07.123Z"),
            ("second", "2432-03-17T13:45:07Z"),
            ("minute", "2432-03-17T13:45:00Z"),
            ("hour", "2432-03-17T13:00:00Z"),
            ("day", "2432-03-17T00:00:00Z"),
            ("week", "2432-03-15T00:00:00Z"),
            ("month", "2432-03-01T00:00:00Z"),
            ("quarter", "2432-01-01T00:00:00Z"),
            ("year", "2432-01-01T00:00:00Z"),
        ] {
            assert_eq!(
                date_trunc_micros(granularity, value).unwrap(),
                micros(expected),
                "{}",
                granularity
            );
        }
        assert_eq!(
            date_trunc_micros("year", micros("9999-12-31T23:59:59Z")).unwrap(),
            micros("9999-01-01T00:00:00Z")
        );
        assert!(date_trunc_micros("decade", value).is_err());
    }

    #[test]
    fn date_trunc_keeps_microseconds() {
        let date_trunc = CubeDateTrunc::new(TimeUnit::Microsecond);
        let value = micros("2432-03-17T13:45:07Z");
        let result = date_trunc
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some("month".to_string()))),
                    micros_array(vec![Some(value), None]),
                ],
                number_rows: 2,
                return_type: &timestamp(&TimeUnit::Microsecond),
            })
            .unwrap();
        assert_eq!(
            micros_values(result),
            vec![Some(micros("2432-03-01T00:00:00Z")), None]
        );
    }

    #[test]
    fn coercion_follows_the_planning_unit() {
        let ns = timestamp(&TimeUnit::Nanosecond);
        let us = timestamp(&TimeUnit::Microsecond);
        let utf8 = DataType::Utf8;

        let convert_tz = ConvertTz::new(TimeUnit::Nanosecond);
        assert_eq!(
            convert_tz
                .coerce_types(&[us.clone(), utf8.clone()])
                .unwrap(),
            vec![ns.clone(), utf8.clone()]
        );
        let convert_tz = ConvertTz::new(TimeUnit::Microsecond);
        assert_eq!(
            convert_tz
                .coerce_types(&[utf8.clone(), utf8.clone()])
                .unwrap(),
            vec![us.clone(), utf8.clone()]
        );
        assert_eq!(
            convert_tz.return_type(&[us.clone(), utf8.clone()]).unwrap(),
            us
        );
        assert_eq!(
            convert_tz.return_type(&[ns.clone(), utf8.clone()]).unwrap(),
            ns
        );

        let date_trunc = CubeDateTrunc::new(TimeUnit::Nanosecond);
        assert_eq!(
            date_trunc
                .coerce_types(&[utf8.clone(), utf8.clone()])
                .unwrap(),
            data_types(
                "date_trunc",
                &[utf8.clone(), utf8.clone()],
                DateTruncFunc::new().signature()
            )
            .unwrap()
        );
        let date_trunc = CubeDateTrunc::new(TimeUnit::Microsecond);
        for arg in [utf8.clone(), ns.clone(), us.clone()] {
            assert_eq!(
                date_trunc.coerce_types(&[utf8.clone(), arg]).unwrap(),
                vec![utf8.clone(), us.clone()]
            );
        }

        let date_add = DateAddSub::new_add(TimeUnit::Microsecond);
        let interval = DataType::Interval(IntervalUnit::MonthDayNano);
        assert_eq!(
            date_add
                .coerce_types(&[ns.clone(), interval.clone()])
                .unwrap(),
            vec![us.clone(), interval.clone()]
        );
    }

    #[test]
    fn convert_tz_keeps_unit() {
        let value = micros("2432-03-17T13:45:07Z");
        let shift = ColumnarValue::Scalar(ScalarValue::Utf8(Some("-02:30".to_string())));
        let udf = ConvertTz::new(TimeUnit::Microsecond);

        let result = call(
            &udf,
            vec![micros_array(vec![Some(value), None]), shift.clone()],
        );
        assert_eq!(
            micros_values(result),
            vec![Some(micros("2432-03-17T11:15:07Z")), None]
        );

        let result = call(
            &udf,
            vec![
                ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(Some(value), None)),
                shift,
            ],
        );
        assert_eq!(
            micros_values(result),
            vec![Some(micros("2432-03-17T11:15:07Z"))]
        );

        // A NULL shift gives NULL, and so does a NULL timestamp whatever its shift.
        let null_shift = ColumnarValue::Scalar(ScalarValue::Utf8(None));
        let result = call(&udf, vec![micros_array(vec![Some(value)]), null_shift]);
        assert_eq!(micros_values(result), vec![None]);

        let shifts: ArrayRef = Arc::new(StringArray::from(vec![
            Some("+01:00"),
            Some("-01:00"),
            None,
            Some("not a shift"),
        ]));
        let ns = Arc::new(TimestampNanosecondArray::from(vec![
            Some(0),
            Some(0),
            Some(0),
            None,
        ])) as ArrayRef;
        let result = convert_tz(&ns, &shifts).unwrap();
        assert_eq!(
            result
                .as_primitive::<TimestampNanosecondType>()
                .iter()
                .collect_vec(),
            vec![
                Some(3_600_000_000_000),
                Some(-3_600_000_000_000),
                None,
                None
            ]
        );
    }

    #[test]
    fn date_bin_keeps_unit() {
        let date_bin = DateBin::new(TimeUnit::Microsecond);
        let origin = micros("2020-01-01T00:00:00Z");
        let values = vec![
            Some(micros("2020-03-15T11:45:07Z")),
            Some(micros("2432-03-17T13:45:07Z")),
            None,
        ];
        let bin = |interval: ScalarValue, values: ColumnarValue| {
            call(
                &date_bin,
                vec![
                    ColumnarValue::Scalar(interval),
                    values,
                    ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(Some(origin), None)),
                ],
            )
        };

        let months = bin(
            ScalarValue::new_interval_mdn(1, 0, 0),
            micros_array(values.clone()),
        );
        assert_eq!(
            micros_values(months),
            vec![
                Some(micros("2020-03-01T00:00:00Z")),
                Some(micros("2432-03-01T00:00:00Z")),
                None
            ]
        );

        let days = bin(
            ScalarValue::new_interval_dt(1, 0),
            micros_array(values.clone()),
        );
        assert_eq!(
            micros_values(days),
            vec![
                Some(micros("2020-03-15T00:00:00Z")),
                Some(micros("2432-03-17T00:00:00Z")),
                None
            ]
        );

        // Nanosecond input keeps producing nanoseconds.
        let ns = ColumnarValue::Array(Arc::new(TimestampNanosecondArray::from(vec![Some(
            micros("2020-03-15T11:45:07Z") * 1000,
        )])));
        let result = bin(ScalarValue::new_interval_mdn(1, 0, 0), ns)
            .into_array(1)
            .unwrap();
        assert_eq!(
            result
                .as_primitive::<TimestampNanosecondType>()
                .values()
                .to_vec(),
            vec![micros("2020-03-01T00:00:00Z") * 1000]
        );
    }
}
