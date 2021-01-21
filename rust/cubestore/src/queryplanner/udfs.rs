use crate::queryplanner::hll::Hll;
use crate::CubeError;
use arrow::array::{Array, BinaryArray, UInt64Builder};
use arrow::datatypes::DataType;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::functions::Signature;
use datafusion::physical_plan::udaf::AggregateUDF;
use datafusion::physical_plan::udf::ScalarUDF;
use datafusion::physical_plan::Accumulator;
use datafusion::scalar::ScalarValue;
use serde_derive::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub enum CubeScalarUDFKind {
    HllCardinality, // cardinality(), accepting the HyperLogLog sketches.
}

pub trait CubeScalarUDF {
    fn kind(&self) -> CubeScalarUDFKind;
    fn name(&self) -> &str;
    fn descriptor(&self) -> ScalarUDF;
}

pub fn scalar_udf_by_kind(k: CubeScalarUDFKind) -> Box<dyn CubeScalarUDF> {
    match k {
        CubeScalarUDFKind::HllCardinality => Box::new(HllCardinality {}),
    }
}

/// Note that only full match counts. Pass capitalized names.
pub fn scalar_kind_by_name(n: &str) -> Option<CubeScalarUDFKind> {
    if n == "CARDINALITY" {
        return Some(CubeScalarUDFKind::HllCardinality);
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
                let sketches = a[0]
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .expect("expected binary data");

                let mut r = UInt64Builder::new(sketches.len());
                for s in sketches {
                    match s {
                        None => r.append_null()?,
                        Some(d) => r.append_value(read_sketch(d)?.cardinality())?,
                    }
                }
                return Ok(Arc::new(r.finish()));
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
    fn state(&self) -> Result<Vec<ScalarValue>, DataFusionError> {
        return Ok(vec![self.evaluate()?]);
    }

    fn update(&mut self, row: &Vec<ScalarValue>) -> Result<(), DataFusionError> {
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
        return self.merge_sketch(read_sketch(&data)?);
    }

    fn merge(&mut self, states: &Vec<ScalarValue>) -> Result<(), DataFusionError> {
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

fn read_sketch(data: &[u8]) -> Result<Hll, DataFusionError> {
    return Hll::read(&data).map_err(|e| DataFusionError::Execution(e.message));
}
