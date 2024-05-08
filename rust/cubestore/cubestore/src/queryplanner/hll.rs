use crate::CubeError;
use cubedatasketches::{HLLDataSketch, HLLUnionDataSketch};
use cubehll::HllSketch;
use cubezetasketch::HyperLogLogPlusPlus;

#[derive(Debug)]
pub enum Hll {
    Airlift(HllSketch),              // Compatible with Athena, Presto, etc.
    ZetaSketch(HyperLogLogPlusPlus), // Compatible with BigQuery.
    DataSketches(HLLDataSketch),     // Compatible with DataBricks
}

const DS_LIST_PREINTS: u8 = 2;
const DS_HASH_SET_PREINTS: u8 = 3;
const DS_SER_VER: u8 = 1;
const DS_FAMILY_ID: u8 = 7;

impl Hll {
    pub fn read(data: &[u8]) -> Result<Hll, CubeError> {
        if data.is_empty() {
            return Err(CubeError::internal(
                "invalid serialized HLL (empty data)".to_string(),
            ));
        }

        //  - must larger than 3 due to how protos are encoded in ZetaSketch.
        //  - represents the data format version and is <= 3 in AirLift.
        // -  checking first 3 bytes for figure out HLL from Apache DataSketches
        if (data[0] == DS_LIST_PREINTS || data[0] == DS_HASH_SET_PREINTS)
            && data[1] == DS_SER_VER
            && data[2] == DS_FAMILY_ID
        {
            return Ok(Hll::DataSketches(HLLDataSketch::read(data)?));
        } else if data[0] <= 3 {
            return Ok(Hll::Airlift(HllSketch::read(data)?));
        } else {
            return Ok(Hll::ZetaSketch(HyperLogLogPlusPlus::read(data)?));
        }
    }

    pub fn write(&self) -> Vec<u8> {
        match self {
            Self::Airlift(h) => h.write(),
            Self::ZetaSketch(h) => h.write(),
            Self::DataSketches(h) => h.write(),
        }
    }

    pub fn cardinality(&mut self) -> u64 {
        match self {
            Hll::Airlift(h) => h.cardinality(),
            Hll::ZetaSketch(h) => h.cardinality(),
            Hll::DataSketches(h) => h.cardinality(),
        }
    }
}

#[derive(Debug)]
pub enum HllUnion {
    Airlift(HllSketch),
    ZetaSketch(HyperLogLogPlusPlus),
    DataSketches(HLLUnionDataSketch),
}

impl HllUnion {
    pub fn new(hll: Hll) -> Result<Self, CubeError> {
        match hll {
            Hll::Airlift(h) => Ok(Self::Airlift(h)),
            Hll::ZetaSketch(h) => Ok(Self::ZetaSketch(h)),
            Hll::DataSketches(h) => {
                let mut union = HLLUnionDataSketch::new(h.get_lg_config_k());
                union.merge_with(h)?;

                Ok(Self::DataSketches(union))
            }
        }
    }

    pub fn write(&self) -> Vec<u8> {
        match self {
            Self::Airlift(h) => h.write(),
            Self::ZetaSketch(h) => h.write(),
            Self::DataSketches(h) => h.write(),
        }
    }

    pub fn is_compatible(&self, other: &Hll) -> bool {
        match (self, other) {
            (Self::Airlift(l), Hll::Airlift(r)) => l.index_bit_len() == r.index_bit_len(),
            (Self::ZetaSketch(l), Hll::ZetaSketch(r)) => l.is_compatible(r),
            (Self::DataSketches(l), Hll::DataSketches(r)) => {
                l.get_lg_config_k() == r.get_lg_config_k()
            }
            _ => return false,
        }
    }

    /// Clients are responsible for calling `is_compatible` before running this function.
    /// On error, `self` may end up in inconsistent state and must be discarded.
    pub fn merge_with(&mut self, other: Hll) -> Result<(), CubeError> {
        debug_assert!(self.is_compatible(&other));

        match (self, other) {
            (Self::Airlift(l), Hll::Airlift(r)) => l.merge_with(&r),
            (Self::ZetaSketch(l), Hll::ZetaSketch(r)) => l.merge_with(&r)?,
            (Self::DataSketches(l), Hll::DataSketches(r)) => l.merge_with(r)?,
            _ => return Err(CubeError::internal("incompatible HLL types".to_string())),
        }

        return Ok(());
    }
}
