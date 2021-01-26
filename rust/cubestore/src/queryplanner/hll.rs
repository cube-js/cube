use crate::CubeError;
use cubehll::HllSketch;
use cubezetasketch::HyperLogLogPlusPlus;

#[derive(Debug)]
pub enum Hll {
    Airlift(HllSketch),              // Compatible with Athena, Presto, etc.
    ZetaSketch(HyperLogLogPlusPlus), // Compatible with BigQuery.
}

impl Hll {
    pub fn read(data: &[u8]) -> Result<Hll, CubeError> {
        if data.is_empty() {
            return Err(CubeError::internal(
                "invalid serialized HLL (empty data)".to_string(),
            ));
        }
        // The first byte:
        //  - must larger than 3 due to how protos are encoded in ZetaSketch.
        //  - represents the data format version and is <= 3 in AirLift.
        if data[0] <= 3 {
            return Ok(Hll::Airlift(HllSketch::read(data)?));
        } else {
            return Ok(Hll::ZetaSketch(HyperLogLogPlusPlus::read(data)?));
        }
    }

    pub fn write(&self) -> Vec<u8> {
        match self {
            Hll::Airlift(h) => h.write(),
            Hll::ZetaSketch(h) => h.write(),
        }
    }

    pub fn is_compatible(&self, other: &Hll) -> bool {
        match (self, other) {
            (Hll::Airlift(l), Hll::Airlift(r)) => l.index_bit_len() == r.index_bit_len(),
            (Hll::ZetaSketch(l), Hll::ZetaSketch(r)) => l.is_compatible(r),
            _ => return false,
        }
    }

    pub fn cardinality(&self) -> u64 {
        match self {
            Hll::Airlift(h) => h.cardinality(),
            Hll::ZetaSketch(h) => h.cardinality(),
        }
    }

    /// Clients are responsible for calling `is_compatible` before running this function.
    /// On error, `self` may end up in inconsistent state and must be discarded.
    pub fn merge_with(&mut self, other: &Hll) -> Result<(), CubeError> {
        debug_assert!(self.is_compatible(other));
        match (self, other) {
            (Hll::Airlift(l), Hll::Airlift(r)) => l.merge_with(r),
            (Hll::ZetaSketch(l), Hll::ZetaSketch(r)) => l.merge_with(r)?,
            _ => panic!("incompatible HLL types"),
        }
        return Ok(());
    }
}
