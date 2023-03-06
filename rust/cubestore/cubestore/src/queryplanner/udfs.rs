use crate::queryplanner::coalesce::{coalesce, SUPPORTED_COALESCE_TYPES};
use crate::queryplanner::hll::Hll;
use crate::CubeError;
use arrow::array::{Array, BinaryArray, TimestampNanosecondArray, UInt64Builder};
use arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use chrono::{TimeZone, Utc};
use datafusion::cube_ext::datetime::{date_addsub_array, date_addsub_scalar};
use datafusion::error::DataFusionError;
use datafusion::physical_plan::functions::Signature;
use datafusion::physical_plan::udaf::AggregateUDF;
use datafusion::physical_plan::udf::ScalarUDF;
use datafusion::physical_plan::{type_coercion, Accumulator, ColumnarValue};
use datafusion::scalar::ScalarValue;
use serde_derive::{Deserialize, Serialize};
use smallvec::smallvec;
use smallvec::SmallVec;
use std::sync::Arc;

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub enum CubeScalarUDFKind {
    HllCardinality, // cardinality(), accepting the HyperLogLog sketches.
    Coalesce,
    Now,
    UnixTimestamp,
    DateAdd,
    DateSub,
}

pub trait CubeScalarUDF {
    fn kind(&self) -> CubeScalarUDFKind;
    fn name(&self) -> &str;
    fn descriptor(&self) -> ScalarUDF;
}

pub fn scalar_udf_by_kind(k: CubeScalarUDFKind) -> Box<dyn CubeScalarUDF> {
    match k {
        CubeScalarUDFKind::HllCardinality => Box::new(HllCardinality {}),
        CubeScalarUDFKind::Coalesce => Box::new(Coalesce {}),
        CubeScalarUDFKind::Now => Box::new(Now {}),
        CubeScalarUDFKind::UnixTimestamp => Box::new(UnixTimestamp {}),
        CubeScalarUDFKind::DateAdd => Box::new(DateAddSub { is_add: true }),
        CubeScalarUDFKind::DateSub => Box::new(DateAddSub { is_add: false }),
    }
}

/// Note that only full match counts. Pass capitalized names.
pub fn scalar_kind_by_name(n: &str) -> Option<CubeScalarUDFKind> {
    if n == "CARDINALITY" {
        return Some(CubeScalarUDFKind::HllCardinality);
    }
    if n == "COALESCE" {
        return Some(CubeScalarUDFKind::Coalesce);
    }
    if n == "NOW" {
        return Some(CubeScalarUDFKind::Now);
    }
    if n == "UNIX_TIMESTAMP" {
        return Some(CubeScalarUDFKind::UnixTimestamp);
    }
    if n == "DATE_ADD" {
        return Some(CubeScalarUDFKind::DateAdd);
    }
    if n == "DATE_SUB" {
        return Some(CubeScalarUDFKind::DateSub);
    }
    return None;
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub enum CubeAggregateUDFKind {
    MergeHll, // merge(), accepting the HyperLogLog sketches.
}

pub trait CubeAggregateUDF {
    fn kind(&self) -> CubeAggregateUDFKind;
    fn name(&self) -> &str;
    fn descriptor(&self) -> AggregateUDF;
    fn accumulator(&self) -> Box<dyn Accumulator>;
}

pub fn aggregate_udf_by_kind(k: CubeAggregateUDFKind) -> Box<dyn CubeAggregateUDF> {
    match k {
        CubeAggregateUDFKind::MergeHll => Box::new(HllMergeUDF {}),
    }
}

/// Note that only full match counts. Pass capitalized names.
pub fn aggregate_kind_by_name(n: &str) -> Option<CubeAggregateUDFKind> {
    if n == "MERGE" {
        return Some(CubeAggregateUDFKind::MergeHll);
    }
    return None;
}

// The rest of the file are implementations of the various functions that we have.
// TODO: add custom type and use it instead of `Binary` for HLL columns.

struct Coalesce {}
impl Coalesce {
    fn signature() -> Signature {
        Signature::Variadic(SUPPORTED_COALESCE_TYPES.to_vec())
    }
}
impl CubeScalarUDF for Coalesce {
    fn kind(&self) -> CubeScalarUDFKind {
        CubeScalarUDFKind::Coalesce
    }

    fn name(&self) -> &str {
        "COALESCE"
    }

    fn descriptor(&self) -> ScalarUDF {
        return ScalarUDF {
            name: self.name().to_string(),
            signature: Self::signature(),
            return_type: Arc::new(|inputs| {
                if inputs.is_empty() {
                    return Err(DataFusionError::Plan(
                        "COALESCE requires at least 1 argument".to_string(),
                    ));
                }
                let ts = type_coercion::data_types(inputs, &Self::signature())?;
                Ok(Arc::new(ts[0].clone()))
            }),
            fun: Arc::new(coalesce),
        };
    }
}

struct Now {}
impl Now {
    fn signature() -> Signature {
        Signature::Exact(Vec::new())
    }
}
impl CubeScalarUDF for Now {
    fn kind(&self) -> CubeScalarUDFKind {
        CubeScalarUDFKind::Now
    }

    fn name(&self) -> &str {
        "NOW"
    }

    fn descriptor(&self) -> ScalarUDF {
        return ScalarUDF {
            name: self.name().to_string(),
            signature: Self::signature(),
            return_type: Arc::new(|inputs| {
                assert!(inputs.is_empty());
                Ok(Arc::new(DataType::Timestamp(TimeUnit::Nanosecond, None)))
            }),
            fun: Arc::new(|_| {
                Err(DataFusionError::Internal(
                    "NOW() was not optimized away".to_string(),
                ))
            }),
        };
    }
}

struct UnixTimestamp {}
impl UnixTimestamp {
    fn signature() -> Signature {
        Signature::Exact(Vec::new())
    }
}
impl CubeScalarUDF for UnixTimestamp {
    fn kind(&self) -> CubeScalarUDFKind {
        CubeScalarUDFKind::UnixTimestamp
    }

    fn name(&self) -> &str {
        "UNIX_TIMESTAMP"
    }

    fn descriptor(&self) -> ScalarUDF {
        return ScalarUDF {
            name: self.name().to_string(),
            signature: Self::signature(),
            return_type: Arc::new(|inputs| {
                assert!(inputs.is_empty());
                Ok(Arc::new(DataType::Int64))
            }),
            fun: Arc::new(|_| {
                Err(DataFusionError::Internal(
                    "UNIX_TIMESTAMP() was not optimized away".to_string(),
                ))
            }),
        };
    }
}

struct DateAddSub {
    is_add: bool,
}

impl DateAddSub {
    fn signature() -> Signature {
        Signature::OneOf(vec![
            Signature::Exact(vec![
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                DataType::Interval(IntervalUnit::YearMonth),
            ]),
            Signature::Exact(vec![
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                DataType::Interval(IntervalUnit::DayTime),
            ]),
        ])
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

impl CubeScalarUDF for DateAddSub {
    fn kind(&self) -> CubeScalarUDFKind {
        match self.is_add {
            true => CubeScalarUDFKind::DateAdd,
            false => CubeScalarUDFKind::DateSub,
        }
    }

    fn name(&self) -> &str {
        self.name_static()
    }

    fn descriptor(&self) -> ScalarUDF {
        let name = self.name_static();
        let is_add = self.is_add;
        return ScalarUDF {
            name: self.name().to_string(),
            signature: Self::signature(),
            return_type: Arc::new(|_| {
                Ok(Arc::new(DataType::Timestamp(TimeUnit::Nanosecond, None)))
            }),
            fun: Arc::new(move |inputs| {
                assert_eq!(inputs.len(), 2);
                let interval = match &inputs[1] {
                    ColumnarValue::Scalar(i) => i.clone(),
                    _ => {
                        // We leave this case out for simplicity.
                        // CubeStore does not allow intervals inside tables, so this is super rare.
                        return Err(DataFusionError::Execution(format!(
                            "Only scalar intervals are supported in `{}`",
                            name
                        )));
                    }
                };
                match &inputs[0] {
                    ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(None)) => Ok(
                        ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(None)),
                    ),
                    ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(t))) => {
                        let r = date_addsub_scalar(Utc.timestamp_nanos(*t), interval, is_add)?;
                        Ok(ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                            Some(r.timestamp_nanos()),
                        )))
                    }
                    ColumnarValue::Array(t) if t.as_any().is::<TimestampNanosecondArray>() => {
                        let t = t
                            .as_any()
                            .downcast_ref::<TimestampNanosecondArray>()
                            .unwrap();
                        Ok(ColumnarValue::Array(Arc::new(date_addsub_array(
                            &t, interval, is_add,
                        )?)))
                    }
                    _ => {
                        return Err(DataFusionError::Execution(format!(
                            "First argument of `{}` must be a non-null timestamp",
                            name
                        )))
                    }
                }
            }),
        };
    }
}

struct HllCardinality {}
impl CubeScalarUDF for HllCardinality {
    fn kind(&self) -> CubeScalarUDFKind {
        return CubeScalarUDFKind::HllCardinality;
    }

    fn name(&self) -> &str {
        return "CARDINALITY";
    }

    fn descriptor(&self) -> ScalarUDF {
        return ScalarUDF {
            name: self.name().to_string(),
            signature: Signature::Exact(vec![DataType::Binary]),
            return_type: Arc::new(|_| Ok(Arc::new(DataType::UInt64))),
            fun: Arc::new(|a| {
                assert_eq!(a.len(), 1);
                let sketches = a[0].clone().into_array(1);
                let sketches = sketches
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .expect("expected binary data");

                let mut r = UInt64Builder::new(sketches.len());
                for s in sketches {
                    match s {
                        None => r.append_null()?,
                        Some(d) => {
                            if d.len() == 0 {
                                r.append_value(0)?
                            } else {
                                r.append_value(read_sketch(d)?.cardinality())?
                            }
                        }
                    }
                }
                return Ok(ColumnarValue::Array(Arc::new(r.finish())));
            }),
        };
    }
}

struct HllMergeUDF {}
impl CubeAggregateUDF for HllMergeUDF {
    fn kind(&self) -> CubeAggregateUDFKind {
        return CubeAggregateUDFKind::MergeHll;
    }
    fn name(&self) -> &str {
        return "MERGE";
    }
    fn descriptor(&self) -> AggregateUDF {
        return AggregateUDF {
            name: self.name().to_string(),
            signature: Signature::Exact(vec![DataType::Binary]),
            return_type: Arc::new(|_| Ok(Arc::new(DataType::Binary))),
            accumulator: Arc::new(|| Ok(Box::new(HllMergeAccumulator { acc: None }))),
            state_type: Arc::new(|_| Ok(Arc::new(vec![DataType::Binary]))),
        };
    }
    fn accumulator(&self) -> Box<dyn Accumulator> {
        return Box::new(HllMergeAccumulator { acc: None });
    }
}

#[derive(Debug)]
struct HllMergeAccumulator {
    // TODO: store sketch for empty set from the start.
    //       this requires storing index_bit_len in the type.
    acc: Option<Hll>,
}

impl Accumulator for HllMergeAccumulator {
    fn reset(&mut self) {
        self.acc = None;
    }

    fn state(&self) -> Result<SmallVec<[ScalarValue; 2]>, DataFusionError> {
        return Ok(smallvec![self.evaluate()?]);
    }

    fn update(&mut self, row: &[ScalarValue]) -> Result<(), DataFusionError> {
        assert_eq!(row.len(), 1);
        let data;
        if let ScalarValue::Binary(v) = &row[0] {
            if let Some(d) = v {
                data = d
            } else {
                return Ok(()); // ignore NULL.
            }
        } else {
            return Err(CubeError::internal(
                "invalid scalar value passed to MERGE, expecting HLL sketch".to_string(),
            )
            .into());
        }

        // empty state is ok, this means an empty sketch.
        if data.len() == 0 {
            return Ok(());
        }
        return self.merge_sketch(read_sketch(&data)?);
    }

    fn merge(&mut self, states: &[ScalarValue]) -> Result<(), DataFusionError> {
        assert_eq!(states.len(), 1);

        let data;
        if let ScalarValue::Binary(v) = &states[0] {
            if let Some(d) = v {
                data = d
            } else {
                return Ok(()); // ignore NULL.
            }
        } else {
            return Err(CubeError::internal("invalid state in MERGE".to_string()).into());
        }
        // empty state is ok, this means an empty sketch.
        if data.len() == 0 {
            return Ok(());
        }
        return self.merge_sketch(read_sketch(&data)?);
    }

    fn evaluate(&self) -> Result<ScalarValue, DataFusionError> {
        let v;
        match &self.acc {
            None => v = Vec::new(),
            Some(s) => v = s.write(),
        }
        return Ok(ScalarValue::Binary(Some(v)));
    }
}

impl HllMergeAccumulator {
    fn merge_sketch(&mut self, s: Hll) -> Result<(), DataFusionError> {
        if self.acc.is_none() {
            self.acc = Some(s);
            return Ok(());
        } else if let Some(acc_s) = &mut self.acc {
            if !acc_s.is_compatible(&s) {
                return Err(CubeError::internal(
                    "cannot merge two incompatible HLL sketches".to_string(),
                )
                .into());
            }
            acc_s.merge_with(&s)?;
        } else {
            unreachable!("impossible");
        }
        return Ok(());
    }
}

pub fn read_sketch(data: &[u8]) -> Result<Hll, DataFusionError> {
    return Hll::read(&data).map_err(|e| DataFusionError::Execution(e.message));
}
