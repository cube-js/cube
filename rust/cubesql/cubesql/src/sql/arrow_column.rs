//! JSON serialization of Arrow arrays column by column, for `responseFormat: "columnar"`
//! of `/v1/cubesql`.

use chrono::prelude::*;
use datafusion::arrow::{
    array::{
        Array, ArrayRef, BooleanArray, Date32Array, Date64Array, DecimalArray, Float32Array,
        Float64Array, Int16Array, Int32Array, Int64Array, IntervalDayTimeArray,
        IntervalMonthDayNanoArray, IntervalYearMonthArray, ListArray, PrimitiveArray, StringArray,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        UInt16Array, UInt32Array, UInt64Array,
    },
    datatypes::{ArrowPrimitiveType, DataType, IntervalUnit, TimeUnit},
};
use serde::{Serialize, Serializer};
use std::fmt;

use super::dataframe::{
    interval_day_time_value, interval_month_day_nano_value, interval_year_month_value,
    Decimal128Value, ListValue, TableValue,
};

/// Serializes an Arrow array as a JSON array straight from its buffers, with the
/// same value for every cell as `batches_to_dataframe` + `TableValue::serialize`
/// would produce, but without materializing rows.
pub struct ArrowColumn<'a>(pub &'a ArrayRef);

macro_rules! serialize_column {
    ($SERIALIZER:expr, $ARRAY:expr, $ARRAY_TYPE:ident, |$A:ident, $I:ident| $VALUE:expr) => {{
        let $A = $ARRAY.as_any().downcast_ref::<$ARRAY_TYPE>().unwrap();
        if $A.null_count() == 0 {
            $SERIALIZER.collect_seq((0..$A.len()).map(|$I| $VALUE))
        } else {
            $SERIALIZER.collect_seq((0..$A.len()).map(|$I| {
                if $A.is_null($I) {
                    None
                } else {
                    Some($VALUE)
                }
            }))
        }
    }};
}

/// An integer as a JSON string, like `TableValue::Int*`, formatted with `itoa`
/// rather than through `collect_str`'s `fmt::Write` adapter.
struct IntAsStr(i64);

impl Serialize for IntAsStr {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(itoa::Buffer::new().format(self.0))
    }
}

/// A float as a JSON string holding exactly its `Display` text, like `TableValue`.
/// Whole numbers (ids, counts) and floats nearest to `k / 1000` (money) skip float
/// formatting; everything else, `-0.0` included, goes through `Display`.
struct FloatAsStr<T>(T);

trait FastFloat: fmt::Display + Copy {
    /// `Some(k)` when the value is the float nearest to `k / 10^scale` for one of the
    /// shapes above (`scale` 0 or 3).
    fn as_decimal(self) -> Option<(i64, u32)>;
}

macro_rules! impl_fast_float {
    ($T:ty, $EXACT_INT:expr, $MILLI_LIMIT:expr) => {
        impl FastFloat for $T {
            fn as_decimal(self) -> Option<(i64, u32)> {
                if self == 0.0 && self.is_sign_negative() {
                    return None;
                }
                let abs = self.abs();
                if abs < $EXACT_INT && self.fract() == 0.0 {
                    return Some((self as i64, 0));
                }
                if abs < $MILLI_LIMIT {
                    let k = (self * 1000.0).round();
                    if k / 1000.0 == self {
                        return Some((k as i64, 3));
                    }
                }
                None
            }
        }
    };
}

// Below 2^53 (2^24) a whole float is exact as an integer, and `Display` prints it
// without a fraction. `k / 1000` round-trips to the float, and while the float spacing
// stays below 1e-3 (under 2^52 * 1e-3, 2^23 * 1e-3) no other decimal with as few
// digits is as close, so trimmed of trailing zeros it is the shortest form `Display`
// picks.
impl_fast_float!(f64, 9_007_199_254_740_992.0, 1e12);
impl_fast_float!(f32, 16_777_216.0, 8000.0);

impl<T: FastFloat> Serialize for FloatAsStr<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.0.as_decimal() {
            Some((int, 0)) => IntAsStr(int).serialize(serializer),
            Some((milli, _)) => {
                let mut out = [0u8; 24];
                let mut w = 0;
                if milli < 0 {
                    out[0] = b'-';
                    w = 1;
                }
                let milli = milli.unsigned_abs();
                let mut int = itoa::Buffer::new();
                let int = int.format(milli / 1000).as_bytes();
                out[w..w + int.len()].copy_from_slice(int);
                w += int.len();
                let mut frac = milli % 1000;
                if frac != 0 {
                    let mut digits = 3;
                    while frac % 10 == 0 {
                        frac /= 10;
                        digits -= 1;
                    }
                    out[w] = b'.';
                    for i in (0..digits).rev() {
                        out[w + 1 + i] = b'0' + (frac % 10) as u8;
                        frac /= 10;
                    }
                    w += 1 + digits;
                }
                // Only ASCII digits, `-` and `.`.
                serializer.serialize_str(std::str::from_utf8(&out[..w]).unwrap())
            }
            None => serializer.collect_str(&self.0),
        }
    }
}

/// A timestamp column as `TableValue` writes timestamps (`TimestampValue`'s `Display`:
/// UTC, truncated to microseconds toward zero, milliseconds shown, zone not printed).
/// Neighbouring rows usually share a day, so the date is formatted once per day
/// rather than per cell, and the time of day is plain integer math.
struct TimestampColumn<'a, T: ArrowPrimitiveType<Native = i64>> {
    array: &'a PrimitiveArray<T>,
    nanos_per_unit: i64,
}

const NANOS_PER_DAY: i64 = 86_400_000_000_000;

impl<T: ArrowPrimitiveType<Native = i64>> Serialize for TimestampColumn<'_, T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let date = std::cell::Cell::new((i64::MIN, [0u8; 10]));
        let cell = |i: usize| TimestampCell {
            nanos: self.array.value(i) * self.nanos_per_unit,
            date: &date,
        };
        if self.array.null_count() == 0 {
            serializer.collect_seq((0..self.array.len()).map(cell))
        } else {
            serializer.collect_seq(
                (0..self.array.len()).map(|i| (!self.array.is_null(i)).then(|| cell(i))),
            )
        }
    }
}

struct TimestampCell<'a> {
    nanos: i64,
    /// The last formatted day and its `YYYY-MM-DD`.
    date: &'a std::cell::Cell<(i64, [u8; 10])>,
}

impl Serialize for TimestampCell<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        fn put(buf: &mut [u8], mut value: u64) {
            for digit in buf.iter_mut().rev() {
                *digit = b'0' + (value % 10) as u8;
                value /= 10;
            }
        }

        let nanos = self.nanos - self.nanos % 1000;
        let day = nanos.div_euclid(NANOS_PER_DAY);
        let of_day = nanos.rem_euclid(NANOS_PER_DAY) as u64;

        let (cached_day, mut date) = self.date.get();
        if cached_day != day {
            // Not from `day * NANOS_PER_DAY`: that overflows for the earliest day.
            let ndt = Utc.timestamp_nanos(nanos);
            // i64 nanoseconds span 1677..=2262, so the year is always 4 digits.
            put(&mut date[0..4], ndt.year() as u64);
            date[4] = b'-';
            put(&mut date[5..7], ndt.month() as u64);
            date[7] = b'-';
            put(&mut date[8..10], ndt.day() as u64);
            self.date.set((day, date));
        }

        let secs = of_day / 1_000_000_000;
        let mut out = *b"0000-00-00T00:00:00.000";
        out[..10].copy_from_slice(&date);
        put(&mut out[11..13], secs / 3600);
        put(&mut out[14..16], secs / 60 % 60);
        put(&mut out[17..19], secs % 60);
        put(&mut out[20..23], of_day % 1_000_000_000 / 1_000_000);

        // Only ASCII digits and separators.
        serializer.serialize_str(std::str::from_utf8(&out).unwrap())
    }
}

impl Serialize for ArrowColumn<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let array = self.0;
        match array.data_type() {
            DataType::UInt16 => serialize_column!(serializer, array, UInt16Array, |a, i| {
                IntAsStr((a.value(i) as i16).into())
            }),
            DataType::Int16 => serialize_column!(serializer, array, Int16Array, |a, i| {
                IntAsStr(a.value(i).into())
            }),
            DataType::UInt32 => serialize_column!(serializer, array, UInt32Array, |a, i| {
                IntAsStr((a.value(i) as i32).into())
            }),
            DataType::Int32 => serialize_column!(serializer, array, Int32Array, |a, i| {
                IntAsStr(a.value(i).into())
            }),
            DataType::UInt64 => serialize_column!(serializer, array, UInt64Array, |a, i| {
                IntAsStr(a.value(i) as i64)
            }),
            DataType::Int64 => serialize_column!(serializer, array, Int64Array, |a, i| {
                IntAsStr(a.value(i))
            }),
            DataType::Float32 => serialize_column!(serializer, array, Float32Array, |a, i| {
                FloatAsStr(a.value(i))
            }),
            DataType::Float64 => serialize_column!(serializer, array, Float64Array, |a, i| {
                FloatAsStr(a.value(i))
            }),
            // `Option<bool>` / `Option<&str>` serialize exactly like `TableValue::Boolean`
            // / `TableValue::String`, without the per-cell `String` copy the latter needs.
            DataType::Boolean => serializer.collect_seq(
                array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .iter(),
            ),
            DataType::Utf8 => {
                serializer.collect_seq(array.as_any().downcast_ref::<StringArray>().unwrap().iter())
            }
            DataType::Date32 => serialize_column!(serializer, array, Date32Array, |a, i| {
                TableValue::Date(
                    a.value_as_date(i)
                        .expect("value_as_date must return Option with NaiveDate for Date32Array"),
                )
            }),
            DataType::Date64 => serialize_column!(serializer, array, Date64Array, |a, i| {
                TableValue::Date(
                    a.value_as_date(i)
                        .expect("value_as_date must return Option with NaiveDate for Date64Array"),
                )
            }),
            DataType::Timestamp(TimeUnit::Millisecond, _) => TimestampColumn {
                array: array
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap(),
                nanos_per_unit: 1_000_000,
            }
            .serialize(serializer),
            DataType::Timestamp(TimeUnit::Microsecond, _) => TimestampColumn {
                array: array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap(),
                nanos_per_unit: 1000,
            }
            .serialize(serializer),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => TimestampColumn {
                array: array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap(),
                nanos_per_unit: 1,
            }
            .serialize(serializer),
            DataType::Interval(IntervalUnit::DayTime) => {
                serialize_column!(serializer, array, IntervalDayTimeArray, |a, i| {
                    TableValue::Interval(interval_day_time_value(a.value(i)))
                })
            }
            DataType::Interval(IntervalUnit::YearMonth) => {
                serialize_column!(serializer, array, IntervalYearMonthArray, |a, i| {
                    TableValue::Interval(interval_year_month_value(a.value(i)))
                })
            }
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                serialize_column!(serializer, array, IntervalMonthDayNanoArray, |a, i| {
                    TableValue::Interval(interval_month_day_nano_value(a.value(i)))
                })
            }
            DataType::Decimal(_, s) => {
                serialize_column!(serializer, array, DecimalArray, |a, i| {
                    TableValue::Decimal128(Decimal128Value::new(a.value(i), *s))
                })
            }
            DataType::List(_) => serialize_column!(serializer, array, ListArray, |a, i| {
                TableValue::List(ListValue::new(a.value(i)))
            }),
            DataType::Null => serializer.collect_seq((0..array.len()).map(|_| ())),
            x => Err(serde::ser::Error::custom(format!(
                "Unsupported data type: {:?}",
                x
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::dataframe::{batches_to_dataframe, TimestampValue};
    use datafusion::arrow::{datatypes::Schema, record_batch::RecordBatch};

    #[test]
    fn test_arrow_column_matches_dataframe_rows() {
        use datafusion::arrow::{
            array::DecimalBuilder,
            datatypes::{Field, Int64Type},
        };
        use std::sync::Arc;

        let mut decimal = DecimalBuilder::new(2, 10, 2);
        decimal.append_value(-12345).unwrap();
        decimal.append_null().unwrap();

        let columns: Vec<ArrayRef> = vec![
            Arc::new(UInt16Array::from(vec![Some(65535), None])),
            Arc::new(Int16Array::from(vec![Some(-7), None])),
            Arc::new(UInt32Array::from(vec![Some(7), None])),
            Arc::new(Int32Array::from(vec![Some(-70000), None])),
            Arc::new(UInt64Array::from(vec![Some(u64::MAX), None])),
            Arc::new(Int64Array::from(vec![Some(i64::MIN), None])),
            Arc::new(Float32Array::from(vec![Some(1.25), None])),
            Arc::new(Float64Array::from(vec![Some(0.1 + 0.2), None])),
            Arc::new(BooleanArray::from(vec![Some(false), None])),
            Arc::new(StringArray::from(vec![Some("a\"b\n"), None])),
            Arc::new(Date32Array::from(vec![Some(8541), None])),
            Arc::new(Date64Array::from(vec![Some(737942400000), None])),
            Arc::new(TimestampMillisecondArray::from_opt_vec(
                vec![Some(737942400123), None],
                None,
            )),
            Arc::new(TimestampMicrosecondArray::from_opt_vec(
                vec![Some(737942400123456), None],
                None,
            )),
            Arc::new(TimestampNanosecondArray::from_opt_vec(
                vec![Some(737942400123456789), None],
                Some("UTC".to_string()),
            )),
            Arc::new(IntervalYearMonthArray::from(vec![Some(14), None])),
            Arc::new(IntervalDayTimeArray::from(vec![
                Some((3_i64 << 32) | 3_723_004),
                None,
            ])),
            Arc::new(IntervalMonthDayNanoArray::from(vec![
                Some((1_i128 << 96) | (2_i128 << 64) | 3_723_000_000_500),
                None,
            ])),
            Arc::new(decimal.finish()),
            Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
                Some(vec![Some(1), None]),
                None,
            ])),
            Arc::new(datafusion::arrow::array::NullArray::new(2)),
        ];
        // Edge values of the types `ArrowColumn` formats by hand rather than through
        // `TableValue`.
        let edge_columns: Vec<ArrayRef> = vec![
            Arc::new(Float32Array::from(vec![
                Some(f32::NAN),
                Some(f32::NEG_INFINITY),
                Some(-0.0),
                Some(f32::MAX),
                Some(1e-7),
            ])),
            Arc::new(Float64Array::from(vec![
                Some(1e300),
                Some(-1e-300),
                Some(f64::INFINITY),
                Some(f64::MIN_POSITIVE),
                Some(123456789.0),
            ])),
            Arc::new(TimestampNanosecondArray::from_opt_vec(
                vec![
                    Some(-1),
                    Some(-1_500_000_000),
                    Some(i64::MIN),
                    Some(i64::MAX),
                    Some(0),
                ],
                None,
            )),
            Arc::new(TimestampMillisecondArray::from_opt_vec(
                vec![
                    Some(-1),
                    Some(-86_400_001),
                    Some(9_223_372_036_853),
                    Some(-9_223_372_036_854),
                    None,
                ],
                Some("+05:00".to_string()),
            )),
            Arc::new(Int64Array::from(vec![
                Some(i64::MIN),
                Some(i64::MAX),
                Some(0),
                Some(-1),
                None,
            ])),
        ];

        for columns in [columns, edge_columns] {
            let schema = Arc::new(Schema::new(
                columns
                    .iter()
                    .enumerate()
                    .map(|(i, c)| Field::new(&format!("c{}", i), c.data_type().clone(), true))
                    .collect(),
            ));
            let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();

            let frame = batches_to_dataframe(&schema, vec![batch.clone()]).unwrap();
            for (idx, column) in batch.columns().iter().enumerate() {
                let expected: Vec<&TableValue> =
                    frame.get_rows().iter().map(|r| &r.values()[idx]).collect();

                assert_eq!(
                    serde_json::to_string(&ArrowColumn(column)).unwrap(),
                    serde_json::to_string(&expected).unwrap(),
                    "column {} ({:?})",
                    idx,
                    column.data_type()
                );
            }
        }
    }

    #[test]
    // The ties below are written in full on purpose: their shortest forms are the bug.
    #[allow(clippy::excessive_precision)]
    fn test_float_as_str_matches_display() {
        fn check<T: FastFloat + fmt::Debug>(v: T) {
            check_float(v);
        }

        for v in [
            0.0,
            -0.0,
            1.0,
            -1.0,
            0.1,
            0.3,
            0.1 + 0.2,
            1e-7,
            1e15,
            1e16,
            1.5e16,
            123456789.0,
            1e21,
            1e22,
            1e23,
            1e-300,
            1e300,
            f64::MAX,
            f64::MIN,
            f64::MIN_POSITIVE,
            f64::EPSILON,
            5e-324,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            562949953421312.25,
            9007199254740991.0,
            9007199254740992.0,
            -9007199254740992.0,
            1e16,
        ] {
            check(v);
        }
        for v in [
            0.0f32,
            -0.0,
            1.0,
            0.1,
            1e-7,
            1e10,
            f32::MAX,
            f32::MIN_POSITIVE,
            1e-45,
            1557507.25,
            16777215.0,
            16777216.0,
            -16777216.0,
        ] {
            check(v);
        }

        float_sweep(20_000, 10_000);
    }

    /// The same check over 100x more values than `test_float_as_str_matches_display`;
    /// too slow for every run, so it runs on demand.
    #[test]
    #[ignore]
    fn test_float_as_str_matches_display_sweep() {
        float_sweep(2_000_000, 1_000_000);
    }

    fn check_float<T: FastFloat + fmt::Debug>(v: T) {
        assert_eq!(
            serde_json::to_string(&FloatAsStr(v)).unwrap(),
            serde_json::to_string(&v.to_string()).unwrap(),
            "{:?}",
            v
        );
    }

    fn float_sweep(random: usize, structured: i64) {
        use rand::{Rng, SeedableRng};

        let check = |v: f64| check_float(v);
        let check32 = |v: f32| check_float(v);
        let mut rng = rand::rngs::StdRng::seed_from_u64(12380);
        for _ in 0..random {
            check(f64::from_bits(rng.gen()));
            check32(f32::from_bits(rng.gen()));
        }
        // Money- and count-like values, the common case, and values around the limits
        // of both fast paths.
        for i in 0..structured {
            let around = i - structured / 2;
            check(i as f64 / 100.0);
            check(-(i as f64) / 1000.0);
            check(i as f64 / 10.0 + 1e9);
            check((i * 104729) as f64 / 1000.0);
            check(around as f64 / 1000.0 + 1e12);
            check32(i as f32 / 1000.0);
            check32(-(i as f32) / 100.0);
            check32(around as f32 / 1000.0 + 8000.0);
            check((i * 7919) as f64);
            check(-(i as f64));
            check32(i as f32);
            check((1i64 << 53) as f64 + around as f64 * 2.0);
            check32((1i64 << 24) as f32 + (around % 1000) as f32 * 2.0);
        }
    }

    #[test]
    fn test_timestamp_column_matches_timestamp_value() {
        use rand::{Rng, SeedableRng};

        let mut rng = rand::rngs::StdRng::seed_from_u64(12503);
        // Runs of nearby values reuse the cached date; jumps and nulls break the runs.
        let mut nanos = Vec::new();
        let mut t: i64 = rng.gen_range(-5_000_000_000_000_000_000..5_000_000_000_000_000_000);
        for _ in 0..200_000 {
            match rng.gen_range(0..10) {
                0 => nanos.push(None),
                1 => {
                    t = rng.gen_range(-9_000_000_000_000_000_000..9_000_000_000_000_000_000);
                    nanos.push(Some(t));
                }
                _ => {
                    t = t.saturating_add(rng.gen_range(-3_600_000_000_000..3_600_000_000_000));
                    nanos.push(Some(t));
                }
            }
        }

        let expected = |scale: i64| -> String {
            let values: Vec<TableValue> = nanos
                .iter()
                .map(|v| match v {
                    Some(v) => TableValue::Timestamp(TimestampValue::new(v / scale * scale, None)),
                    None => TableValue::Null,
                })
                .collect();
            serde_json::to_string(&values).unwrap()
        };
        let scaled = |scale: i64| -> Vec<Option<i64>> {
            nanos.iter().map(|v| v.map(|v| v / scale)).collect()
        };

        let ns: ArrayRef =
            std::sync::Arc::new(TimestampNanosecondArray::from_opt_vec(scaled(1), None));
        let us: ArrayRef =
            std::sync::Arc::new(TimestampMicrosecondArray::from_opt_vec(scaled(1000), None));
        let ms: ArrayRef = std::sync::Arc::new(TimestampMillisecondArray::from_opt_vec(
            scaled(1_000_000),
            None,
        ));
        assert_eq!(
            serde_json::to_string(&ArrowColumn(&ns)).unwrap(),
            expected(1)
        );
        assert_eq!(
            serde_json::to_string(&ArrowColumn(&us)).unwrap(),
            expected(1000)
        );
        assert_eq!(
            serde_json::to_string(&ArrowColumn(&ms)).unwrap(),
            expected(1_000_000)
        );
    }
}
