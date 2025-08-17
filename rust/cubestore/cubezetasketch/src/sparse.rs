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
use std::cmp::min;
use std::collections::BTreeSet;

#[derive(Debug, Clone)]
pub struct SparseRepresentation {
    /**
     * The maximum number of bytes that the `State::sparse_data` may contain before we upgrade to
     * normal. See `MAXIMUM_SPARSE_DATA_FRACTION` for more details.
     */
    max_sparse_data_bytes: u32,

    /** Helper object for encoding and decoding individual sparse values. */
    encoding: SparseEncoding,

    /**
     * A buffer of integers which should be merged into the difference encoded [sparse_data]. The
     * sparse representation in [sparse_data] is more space efficient but also slower to read and
     * write to, so this buffer allows us to quickly return when adding new values.
     *
     * Original implementation uses vector here, we choose to use BTreeSet to improve merge times.
     * This involves a higher memory footprint, but allows to very efficiently buffer elements
     * on merges.
     */
    buffer: BTreeSet<u32>,
    /**
     * The maximum number of elements that the [buffer] may contain before it is flushed into the
     * sparse [sparseData] representation. See [MAXIMUM_BUFFER_ELEMENTS_FRACTION] for details on
     * how this is computed.
     */
    max_buffer_elements: u32,
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

    /**
     * The maximum amount of elements that the temporary `buffer` may contain before it is
     * flushed, relative to the number of bytes that the data in the normal representation would
     * require.
     *
     * The thinking for this is as follows: If the number of bytes that the normal representation
     * would occupy is `m`, then the maximum number of bytes that the encoded sparse data can
     * occupy is `0.75m` (see [MAXIMUM_SPARSE_DATA_FRACTION] above). This leaves `0.25m = m/4` bytes
     * of memory that the temporary buffer can use before the overall in-memory
     * footprint of the sparse representation exceeds that of the normal representation. Since each
     * element in the buffer requires 4 bytes (32-bit integers), we can at most keep `m/16` elements
     * before we exceed the in-memory footprint of the normal representation data.
     *
     * Now the problem is that writing and reading the difference encoded data is CPU expensive (it
     * is by far the limiting factor for sparse adds and merges) so there is a tradeoff between the
     * memory footprint and the CPU cost.
     * For this reason, we add a correction factor that allows the sparse representation to use a bit
     * more memory and thereby greatly increases the speed of adds and merges.
     *
     * A value of `4` was chosen in consistency with a legacy HLL++ implementation but this
     * is something to be evaluated critically.
     *
     * This results in a final elements to bytes ratio of `4 * m/16 = m/4`. This means that the
     * sparse representation can (in the worst case) use 1.75x the amount of RAM than the normal
     * representation would. It will always use less than [MAXIMUM_SPARSE_DATA_FRACTION] times the
     * amount of space on disk, however.
     */
    const MAXIMUM_BUFFER_ELEMENTS_FRACTION: f32 = 1. - Self::MAXIMUM_SPARSE_DATA_FRACTION;

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
        let max_buffer_elements = (m as f32 * Self::MAXIMUM_BUFFER_ELEMENTS_FRACTION) as u32;
        if max_buffer_elements <= 0 {
            return Err(ZetaError::new(format!(
                "max_buffer_elements must be > 0, got {}",
                max_buffer_elements
            )));
        }
        // We have no good way of checking whether the data actually contains the given number of
        // elements without decoding the data, which would be inefficient here.
        return Ok(SparseRepresentation {
            max_sparse_data_bytes,
            encoding,
            max_buffer_elements,
            buffer: BTreeSet::new(),
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

    pub fn cardinality(&mut self, state: &mut State) -> u64 {
        // This is the only place that panics instead of returning errors.
        // TODO: we should either (1) panic everywhere or (2) return an error here.
        self.flush_buffer(state).expect("could not flush buffer");

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
        return self.add_sparse_values(state, other, other_state);
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

    fn add_sparse_values(
        &mut self,
        state: &mut State,
        other: &SparseRepresentation,
        other_state: &State,
    ) -> Result<Option<NormalRepresentation>> {
        self.encoding.assert_compatible(&other.encoding);
        if !other.buffer.is_empty() {
            self.buffer.extend(other.buffer.iter())
        }
        if other_state.sparse_size < 0 {
            return Err(ZetaError::new(format!(
                "negative sparse_size: {}",
                other_state.sparse_size
            )));
        }
        if (other_state.sparse_size as u32) < self.max_buffer_elements {
            for e in Self::sorted_iterator(other_state.sparse_data.as_deref()) {
                let e = e?;
                self.buffer.insert(e);
            }
        } else {
            // Special case when encodings are the same. Then we can profit from the fact that
            // sparse_values are sorted (as defined in the add_sparse_values contract) and do a
            // merge-join.
            self.flush_buffer(state)?;
            let self_data = state.sparse_data.take();
            self.merge_and_set(
                state,
                Self::sorted_iterator(self_data.as_deref()),
                Self::sorted_iterator(other_state.sparse_data.as_deref()),
            )?;
        }
        // TODO: Merge without risking to grow this representation above its maximum size.
        return Ok(self.update_representation(state)?);
    }

    fn merge_and_set<Iter1, Iter2>(
        &self,
        state: &mut State,
        mut l: Iter1,
        mut r: Iter2,
    ) -> Result<()>
    where
        Iter1: Iterator<Item = Result<u32>>,
        Iter2: Iterator<Item = Result<u32>>,
    {
        let mut data = Vec::new();
        struct MergeState<'a> {
            encoder: DifferenceEncoder<'a>,
            size: i32,
        }
        impl MergeState<'_> {
            fn put_int(&mut self, v: u32) {
                self.encoder.put_int(v);
                self.size += 1;
            }

            fn consume<Iter: Iterator<Item = Result<u32>>>(&mut self, mut it: Iter) -> Result<()> {
                while let Some(v) = it.next().transpose()? {
                    self.encoder.put_int(v);
                    self.size += 1;
                }
                Ok(())
            }
        }
        let mut s = MergeState {
            encoder: DifferenceEncoder::new(&mut data),
            size: 0,
        };
        // First iteration.
        let (mut lv, mut rv) = match (l.next().transpose()?, r.next().transpose()?) {
            (None, None) => {
                let size = s.size;
                return Self::set_sparse(state, data, size);
            }
            (Some(v), None) => {
                s.put_int(v);
                s.consume(l)?;
                let size = s.size;
                return Self::set_sparse(state, data, size);
            }
            (None, Some(v)) => {
                s.put_int(v);
                s.consume(r)?;
                let size = s.size;
                return Self::set_sparse(state, data, size);
            }
            (Some(lv), Some(rv)) => (lv, rv),
        };

        let mut last = min(lv, rv);
        let mut last_index = self.encoding.decode_sparse_index(last as i32);
        loop {
            let next = min(lv, rv);
            let next_index = self.encoding.decode_sparse_index(next as i32);
            if last_index != next_index {
                s.put_int(last)
            }
            last = next;
            last_index = next_index;
            if lv < rv {
                match l.next().transpose()? {
                    Some(v) => lv = v,
                    None => {
                        if self.encoding.decode_sparse_index(rv as i32) != last_index {
                            s.put_int(last)
                        }
                        s.put_int(rv);
                        s.consume(r)?;
                        break;
                    }
                }
            } else {
                match r.next().transpose()? {
                    Some(v) => rv = v,
                    None => {
                        if self.encoding.decode_sparse_index(lv as i32) != last_index {
                            s.put_int(last)
                        }
                        s.put_int(lv);
                        s.consume(l)?;
                        break;
                    }
                }
            }
        }
        let size = s.size;
        return Self::set_sparse(state, data, size);
    }

    fn set_sparse(state: &mut State, data: Vec<u8>, size: i32) -> Result<()> {
        state.sparse_data = Some(data);
        state.sparse_size = size;
        Ok(())
    }

    pub(crate) fn sorted_iterator(sparse_data: Option<&[u8]>) -> DifferenceDecoder<'_> {
        return DifferenceDecoder::new(sparse_data.unwrap_or(&[]));
    }

    fn buffer_iterator<'a>(&'a self) -> impl Iterator<Item = Result<u32>> + 'a {
        self.buffer.iter().map(|v| Ok(*v))
    }

    /// Updates the sparse representation:
    ///    - If the temporary list has become too large, serialize it into the sparse bytes
    ///      representation.
    ///    - If the sparse representation has become too large, converts to a `NormalRepresentation`.
    ///
    /// Returns a new normal representation if this sparse representation has outgrown itself or
    /// `None` if the sparse representation can continue to be used.
    #[must_use]
    fn update_representation(&mut self, state: &mut State) -> Result<Option<NormalRepresentation>> {
        if (self.max_buffer_elements as usize) < self.buffer.len() {
            self.flush_buffer(state)?;
        }
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
        if !self.buffer.is_empty() {
            representation.add_sparse_values(state, self.encoding(), self.buffer_iterator())?;
            self.buffer.clear();
        }

        return Ok(representation);
    }

    pub fn requires_compaction(&self) -> bool {
        !self.buffer.is_empty()
    }

    pub fn compact(&mut self, state: &mut State) -> Result<()> {
        self.flush_buffer(state)
    }

    fn flush_buffer(&mut self, state: &mut State) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let data = state.sparse_data.take();
        self.merge_and_set(
            state,
            Self::sorted_iterator(data.as_deref()),
            self.buffer_iterator(),
        )?;
        self.buffer.clear();
        return Ok(());
    }
}
