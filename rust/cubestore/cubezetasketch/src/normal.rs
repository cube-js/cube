/*
 * Copyright 2019 Google LLC
 * Copyright 2021 CubeDev, Inc.
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
use crate::data::{alpha, estimate_bias, linear_counting_threshold};
use crate::encoding::{NormalEncoding, SparseEncoding};
use crate::error::Result;
use crate::sparse::SparseRepresentation;
use crate::state::State;
use crate::ZetaError;
use std::cmp::max;

/// Implementation of the normal HLL++ representation.
#[derive(Debug, Clone)]
pub struct NormalRepresentation {
    /// Utility class that encapsulates the encoding / decoding of individual HyperLogLog++ registers.
    encoding: NormalEncoding,
}

impl NormalRepresentation {
    /// The smallest normal precision supported by this representation.
    const MINIMUM_PRECISION: i32 = 10;

    /// The largest normal precision supported by this representation.
    const MAXIMUM_PRECISION: i32 = 24;

    pub fn new(state: &State) -> Result<NormalRepresentation> {
        Self::check_precision(state.precision)?;

        if state.data.is_some() && state.data.as_ref().unwrap().len() != (1 << state.precision) {
            return Err(ZetaError::new(format!(
                "Expected data to consist of exactly {} bytes but got {}",
                1 << state.precision,
                state.data.as_ref().unwrap().len()
            )));
        }

        return Ok(NormalRepresentation {
            encoding: NormalEncoding::new(state.precision),
        });
    }
    /**
     * Checks that the precision is valid for a normal representation.
     */
    pub fn check_precision(precision: i32) -> Result<()> {
        if !(Self::MINIMUM_PRECISION <= precision && precision <= Self::MAXIMUM_PRECISION) {
            return Err(ZetaError::new(format!(
                "Expected normal precision to be >= {} and <= {} but was {}",
                Self::MINIMUM_PRECISION,
                Self::MAXIMUM_PRECISION,
                precision
            )));
        }
        return Ok(());
    }

    /// Computes the cardinality estimate according to the algorithm in Figure 6 of the HLL++ paper
    /// (https://goo.gl/pc916Z).
    pub fn cardinality(&self, state: &State) -> u64 {
        let data;
        if let Some(d) = state.data.as_ref() {
            data = d;
        } else {
            return 0;
        }

        // Compute the summation component of the harmonic mean for the HLL++ algorithm while also
        // keeping track of the number of zeros in case we need to apply LinearCounting instead.
        let mut num_zeros: u32 = 0;
        let mut sum: f64 = 0.;

        for &v in data {
            if v == 0 {
                num_zeros += 1;
            }

            // Compute sum += math.pow(2, -v) without actually performing a floating point exponent
            // computation (which is expensive). v can be at most 64 - precision + 1 and the minimum
            // precision is larger than 2 (see MINIMUM_PRECISION), so this left shift can not overflow.
            assert!(
                v <= 65 - state.precision as u8 && Self::MINIMUM_PRECISION <= state.precision,
                "invalid byte in normal encoding: {}",
                v
            );
            sum += 1.0 / ((1 as u64) << (v as u64)) as f64;
        }

        // Return the LinearCount for small cardinalities where, as explained in the HLL++ paper
        // (https://goo.gl/pc916Z), the results with LinearCount tend to be more accurate than with HLL.
        let m = (1 << state.precision) as f64;
        if 0 < num_zeros {
            let h = m * (m / num_zeros as f64).ln();
            if h <= linear_counting_threshold(state.precision) as f64 {
                return h.round() as u64;
            }
        }

        // The "raw" estimate, designated by E in the HLL++ paper (https://goo.gl/pc916Z).
        let estimate = alpha(state.precision) * m * m / sum;

        // Perform bias correction on small estimates. HyperLogLogPlusPlusData only contains bias
        // estimates for small cardinalities and returns 0 for anything else, so the "E < 5m" guard from
        // the HLL++ paper (https://goo.gl/pc916Z) is superfluous here.
        return (estimate - estimate_bias(estimate, state.precision)).round() as u64;
    }

    pub fn merge_with_sparse(
        &mut self,
        state: &mut State,
        other: &SparseRepresentation,
        other_state: &State,
    ) -> Result<()> {
        self.add_sparse_values(
            state,
            &other.encoding(),
            SparseRepresentation::sorted_iterator(other_state.sparse_data.as_deref()),
        )?;
        return Ok(());
    }

    /// Merges a HyperLogLog++ sourceData array into a state, downgrading the values from the source
    /// data if necessary. Note that this method requires the `targetEncoding` precision to be at
    /// most the `sourceEncoding` precision and that it will not attempt to downgrade the state.
    pub fn merge_with_normal(
        &mut self,
        state: &mut State,
        other: &NormalRepresentation,
        other_state: &State,
    ) {
        assert_eq!(
            &other.encoding.precision, &self.encoding.precision,
            "expected the same precision"
        );
        if !other_state.has_data() {
            return;
        }

        let source_data = other_state.data.as_ref().unwrap();

        Self::ensure_data(state);
        // TODO: check that the produced code uses SIMD instructions.
        let target_data = state.data.as_mut().unwrap();
        for i in 0..target_data.len() {
            target_data[i] = max(target_data[i], source_data[i])
        }
    }

    pub fn add_sparse_values<I: Iterator<Item = Result<u32>>>(
        &mut self,
        state: &mut State,
        source_encoding: &SparseEncoding,
        sparse_values: I,
    ) -> Result<()> {
        assert_eq!(
            source_encoding.normal_precision, self.encoding.precision,
            "expected the same precision"
        );

        Self::ensure_data(state);
        let data = state.data.as_mut().unwrap();

        for v in sparse_values {
            let sparse_value = v?;

            let idx = source_encoding.decode_normal_index(sparse_value as i32);
            let rho_w = source_encoding.decode_normal_rho_w(sparse_value as i32);
            if data[idx as usize] < rho_w {
                data[idx as usize] = rho_w;
            }
        }

        return Ok(());
    }

    fn ensure_data(state: &mut State) {
        if state.has_data() {
            return;
        }
        state.data = Some(vec![0; 1 << state.precision]);
    }
}
