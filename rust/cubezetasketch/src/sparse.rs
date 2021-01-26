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
use crate::difference_encoding::{DifferenceDecoder, DifferenceEncoder};
use crate::encoding::SparseEncoding;
use crate::normal::NormalRepresentation;
use crate::state::State;
use crate::Result;
use crate::ZetaError;
use itertools::Itertools;

#[derive(Debug, Clone)]
pub struct SparseRepresentation {
    /**
     * The maximum number of bytes that the `State::sparse_data` may contain before we upgrade to
     * normal. See `MAXIMUM_SPARSE_DATA_FRACTION` for more details.
     */
    max_sparse_data_bytes: u32,
    /** Helper object for encoding and decoding individual sparse values. */
    encoding: SparseEncoding,
}

impl SparseRepresentation {
    /** The largest sparse precision supported by this implementation. */
    const MAXIMUM_SPARSE_PRECISION: i32 = 25;
    /**
     * The maximum amount of encoded sparse data, relative to the normal representation size, before
     * we upgrade to normal.
     *
     * Note that, while some implementations also take into consideration the size of the temporary
     * (in-memory) `buffer`, we define this field only relative to the normal representation
     * size as the golden tests verify that representations are upgraded consistently (relative to the
     * on-disk size). This allows us to fine-tune the size of the temporary `buffer`
     * independently (e.g. improving runtime performance while trading off for peak memory usage).
     */
    const MAXIMUM_SPARSE_DATA_FRACTION: f32 = 0.75;

    pub fn new(state: &State) -> Result<SparseRepresentation> {
        Self::check_precision(state.precision, state.sparse_precision)?;

        let encoding = SparseEncoding::new(state.precision, state.sparse_precision);

        // Compute size limits for the encoded sparse data and temporary buffer relative to what the
        // normal representation would require (which is 2^p bytes).
        if !(state.precision < 31) {
            return Err(ZetaError::new(format!(
                "expected precision < 31, got {}",
                state.precision
            )));
        };
        let m = 1 << state.precision;
        let max_sparse_data_bytes = (m as f32 * Self::MAXIMUM_SPARSE_DATA_FRACTION) as u32;
        if max_sparse_data_bytes <= 0 {
            return Err(ZetaError::new(format!(
                "max_sparse_data_bytes must be > 0, got {}",
                max_sparse_data_bytes
            )));
        }
        // We have no good way of checking whether the data actually contains the given number of
        // elements without decoding the data, which would be inefficient here.
        return Ok(SparseRepresentation {
            max_sparse_data_bytes,
            encoding,
        });
    }

    pub fn encoding(&self) -> &SparseEncoding {
        return &self.encoding;
    }

    fn check_precision(normal_precision: i32, sparse_precision: i32) -> Result<()> {
        NormalRepresentation::check_precision(normal_precision)?;
        if !(normal_precision <= sparse_precision
            && sparse_precision <= Self::MAXIMUM_SPARSE_PRECISION)
        {
            return Err(ZetaError::new(format!(
                "Expected sparse precision to be >= normal precision ({}) and <= {}, but was {}.",
                normal_precision,
                Self::MAXIMUM_SPARSE_PRECISION,
                sparse_precision
            )));
        }
        return Ok(());
    }

    pub fn cardinality(&self, state: &State) -> u64 {
        // Linear counting over the number of empty sparse buckets.
        let buckets = 1 << state.sparse_precision;
        let num_zeros = buckets - state.sparse_size;
        let estimate = buckets as f64 * (buckets as f64 / num_zeros as f64).ln();

        return estimate.round() as u64;
    }

    /// `self` may end up be in the invalid state on error and must not be used further.
    pub fn merge_with_sparse(
        &mut self,
        state: &mut State,
        other: &SparseRepresentation,
        other_state: &State,
    ) -> Result<Option<NormalRepresentation>> {
        // TODO: Add special case when 'this' is empty and 'other' has only encoded data.
        // In that case, we can just copy over the sparse data without needing to decode and dedupe.
        return self.add_sparse_values(
            state,
            &other.encoding,
            Self::sorted_iterator(other_state.sparse_data.as_deref()),
        );
    }

    #[must_use]
    pub fn merge_with_normal(
        &mut self,
        state: &mut State,
        other: &NormalRepresentation,
        other_state: &State,
    ) -> Result<Option<NormalRepresentation>> {
        let mut normal = self.normalize(state)?;
        normal.merge_with_normal(state, other, other_state);
        return Ok(Some(normal));
    }

    fn add_sparse_values<Iter: Iterator<Item = Result<u32>> + Clone>(
        &mut self,
        state: &mut State,
        encoding: &SparseEncoding,
        sparse_values: Iter,
    ) -> Result<Option<NormalRepresentation>> {
        self.encoding.assert_compatible(encoding);

        // Special case when encodings are the same. Then we can profit from the fact that sparse_values
        // are sorted (as defined in the add_sparse_values contract) and do a merge-join.
        let self_data = state.sparse_data.take();
        let iter =
            Self::sorted_iterator(self_data.as_deref()).merge_by(sparse_values, |l, r| {
                match (l, r) {
                    (Err(_), _) => true,
                    (_, Err(_)) => false,
                    (Ok(l), Ok(r)) => l <= r,
                }
            });

        // TODO: Merge without risking to grow this representation above its maximum size.
        Self::set(state, self.encoding.dedupe(iter))?;
        return Ok(self.update_representation(state)?);
    }

    fn set<Iter: Iterator<Item = Result<u32>>>(state: &mut State, mut iter: Iter) -> Result<()> {
        let mut data = Vec::new();
        let mut encoder = DifferenceEncoder::new(&mut data);
        let mut size = 0;
        while let Some(x) = iter.next() {
            encoder.put_int(x?);
            size += 1;
        }

        state.sparse_data = Some(data);
        state.sparse_size = size;
        return Ok(());
    }

    pub(crate) fn sorted_iterator(sparse_data: Option<&[u8]>) -> DifferenceDecoder {
        return DifferenceDecoder::new(sparse_data.unwrap_or(&[]));
    }

    /// Updates the sparse representation:
    ///    - If the temporary list has become too large, serialize it into the sparse bytes
    ///      representation.
    ///    - If the sparse representation has become too large, converts to a `NormalRepresentation`.
    ///
    /// Returns a new normal representation if this sparse representation has outgrown itself or
    /// `None` if the sparse representation can continue to be be used.
    #[must_use]
    fn update_representation(&mut self, state: &mut State) -> Result<Option<NormalRepresentation>> {
        // Upgrade to normal if the sparse data exceeds the maximum allowed amount of memory.
        //
        // Note that sparse_data will allocate a larger buffer on the heap (of size
        // sparse_data.capacity()) than is actually occupied by the sparse encoding (of size
        // sparse_data.len()), since we cannot efficiently anticipate how many bytes will be
        // written when flushing the buffer. So in principle, we would need to compare
        // sparse_data.capacity() with max_sparse_data_bytes here if we wanted to make sure that we never
        // use too much memory at runtime. This would not be compatible with golden tests, though, which
        // ensure that the representation upgrades to normal just before the *serialized* sparse format
        // uses more memory than max_sparse_data_bytes. I.e., we would be upgrading to normal
        // representation earlier than the golden tests.
        if state.sparse_data.is_some()
            && state.sparse_data.as_ref().unwrap().len() > self.max_sparse_data_bytes as usize
        {
            return Ok(Some(self.normalize(state)?));
        }

        return Ok(None);
    }

    /// Convert to `NormalRepresentation`.
    #[must_use]
    fn normalize(&mut self, state: &mut State) -> Result<NormalRepresentation> {
        let mut representation = NormalRepresentation::new(state).expect("programming error");
        let sparse_data = state.sparse_data.take();
        state.sparse_size = 0;

        representation.add_sparse_values(
            state,
            self.encoding(),
            Self::sorted_iterator(sparse_data.as_deref()),
        )?;

        return Ok(representation);
    }
}
