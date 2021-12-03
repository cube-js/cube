/*
 * Copyright 2019 Google LLC
 * Copyright 2021 Cube Dev, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/// HLL++ aggregator for estimating cardinalities of multisets.
///
/// The aggregator uses the standard format for storing the internal state of the cardinality
/// estimate as defined in hllplus-unique.proto, allowing users to merge aggregators with data
/// computed in C++ or Go and to load up the cardinalities in a variety of analysis tools.
///
/// The precision defines the accuracy of the HLL++ aggregator at the cost of the memory used. The
/// upper bound on the memory required is 2^precision bytes, but less memory is used for
/// smaller cardinalities (up to ~2^(precision - 2)). The relative error is 1.04 /
/// sqrt(2^precision). A typical value used at Google is 15, which gives an error of about
/// 0.6% while requiring an upper bound of 32&nbsp;KiB of memory.
///
/// Note that this aggregator is *not* designed to be thread safe.
use crate::error::Result;
use crate::normal::NormalRepresentation;
use crate::sparse::SparseRepresentation;
use crate::state::aggregator_state_proto::AGGREGATOR_TYPE_HYPERLOGLOG_PLUS_UNIQUE;
use crate::state::State;
use crate::ZetaError;
use protobuf::CodedInputStream;

#[derive(Debug, Clone)]
pub struct HyperLogLogPlusPlus {
    /** Backing state of the HyperLogLog++ representation. */
    state: State,
    representation: Representation,
}

#[derive(Debug, Clone)]
pub enum Representation {
    Sparse(SparseRepresentation),
    Normal(NormalRepresentation),
}

/**
 * HyperLogLog++ internally uses either a *sparse* or a *normal* representation,
 * depending on the current cardinality, in order to reduce the memory footprint. We encapsulate
 * these using a Strategy pattern in to `NormalRepresentation` and `SparseRepresentation`.
 *
 * Methods that modify the internal state return a Representation to be used for future calls.
 * This allows representations to undergo metamorphosis when they realize that they are no longer
 * applicable. Concretely, a sparse representation will upgrade itself to a normal representation
 * once it reaches a given size.
 */
impl Representation {
    fn from_state(state: &State) -> Result<Representation> {
        if state.has_data() {
            return Ok(Representation::Normal(NormalRepresentation::new(state)?));
        } else {
            return Ok(Representation::Sparse(SparseRepresentation::new(state)?));
        }
    }
}

impl HyperLogLogPlusPlus {
    // /** The smallest normal precision supported by this aggregator. */
    // pub const MINIMUM_PRECISION :i32 = NormalRepresentation::MINIMUM_PRECISION;
    //
    // /** The largest normal precision supported by this aggregator. */
    // pub const MAXIMUM_PRECISION : i32= NormalRepresentation::MAXIMUM_PRECISION;
    //
    // /** The default normal precision that is used if the user does not specify a normal precision. */
    // pub const DEFAULT_NORMAL_PRECISION :i32 = 15;
    //
    // /** The largest sparse precision supported by this aggregator. */
    // pub const MAXIMUM_SPARSE_PRECISION :i32 = SparseRepresentation::MAXIMUM_SPARSE_PRECISION;
    //
    // /** Value used to indicate that the sparse representation should not be used. */
    // pub const SPARSE_PRECISION_DISABLED :i32 = Representation::SPARSE_PRECISION_DISABLED;

    /**
     * If no sparse precision is specified, this value is added to the normal precision to obtain the
     * sparse precision, which optimizes the memory-precision trade-off.
     *
     */
    pub const DEFAULT_SPARSE_PRECISION_DELTA: i32 = 5;

    /** The encoding version of the `AggregatorStateProto`. We only support v2. */
    const ENCODING_VERSION: i32 = 2;

    /// Creates a new HyperLogLog++ aggregator from the serialized `proto`.
    ///
    /// `proto` is a valid aggregator state of type `AggregatorType::HYPERLOGLOG_PLUS_UNIQUE`.
    pub fn read(proto: &[u8]) -> Result<HyperLogLogPlusPlus> {
        return Self::for_coded_input(CodedInputStream::from_bytes(proto));
    }

    pub fn write(&self) -> Vec<u8> {
        if let Representation::Sparse(r) = &self.representation {
            if r.requires_compaction() {
                let mut state = self.state.clone();
                let mut r = r.clone();
                r.compact(&mut state).expect("HLL compaction failed");
                return state.to_byte_array();
            }
        }
        return self.state.to_byte_array();
    }

    pub fn cardinality(&mut self) -> u64 {
        match &mut self.representation {
            Representation::Sparse(r) => return r.cardinality(&mut self.state),
            Representation::Normal(r) => return r.cardinality(&self.state),
        }
    }

    pub fn is_compatible(&self, other: &HyperLogLogPlusPlus) -> bool {
        return self.state.precision == other.state.precision
            && self.state.sparse_precision == other.state.sparse_precision;
    }

    /// Will crash if `self.is_compatible(other)` returns false.
    pub fn merge_with(&mut self, other: &HyperLogLogPlusPlus) -> Result<()> {
        if self.state.precision != other.state.precision
            || self.state.sparse_precision != other.state.sparse_precision
        {
            return Err(ZetaError::new(format!("Expected sketches with the same precision. Our is (sp = {}, p = {}), their is (sp = {}, p = {}", self.state.sparse_precision, self.state.precision,
      other.state.sparse_precision, other
                                            .state.precision)));
        }
        self.state.num_values += other.state.num_values;

        let new_repr: Option<NormalRepresentation>;
        match (&mut self.representation, &other.representation) {
            (Representation::Sparse(l), Representation::Sparse(r)) => {
                new_repr = l.merge_with_sparse(&mut self.state, r, &other.state)?
            }
            (Representation::Sparse(l), Representation::Normal(r)) => {
                new_repr = l.merge_with_normal(&mut self.state, r, &other.state)?
            }
            (Representation::Normal(l), Representation::Sparse(r)) => {
                l.merge_with_sparse(&mut self.state, r, &other.state)?;
                return Ok(());
            }
            (Representation::Normal(l), Representation::Normal(r)) => {
                l.merge_with_normal(&mut self.state, r, &other.state);
                return Ok(());
            }
        }

        if let Some(n) = new_repr {
            self.representation = Representation::Normal(n)
        }
        return Ok(());
    }

    fn for_coded_input(proto: CodedInputStream) -> Result<HyperLogLogPlusPlus> {
        return Self::from_state(State::parse_stream(proto)?);
    }

    fn from_state(state: State) -> Result<HyperLogLogPlusPlus> {
        if !(state.type_ == AGGREGATOR_TYPE_HYPERLOGLOG_PLUS_UNIQUE) {
            return Err(ZetaError::new(format!(
                "Expected proto to be of type HYPERLOGLOG_PLUS_UNIQUE but was {:?}",
                state.type_
            )));
        }
        if !(state.encoding_version == Self::ENCODING_VERSION) {
            return Err(ZetaError::new(format!(
                "Expected encoding version to be {} but was {}",
                Self::ENCODING_VERSION,
                state.encoding_version
            )));
        }
        // TODO: implement or remove.
        // allowedTypes = Type.extractAndNormalize(state);
        let representation = Representation::from_state(&state)?;
        return Ok(HyperLogLogPlusPlus {
            state,
            representation,
        });
    }
}
