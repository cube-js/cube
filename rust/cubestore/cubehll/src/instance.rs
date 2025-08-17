/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use crate::bias_correction;
use crate::error::HllError;
use crate::error::Result;
use crate::instance::HllInstance::{Dense, Sparse};
use byteorder::{BigEndian, LittleEndian, ReadBytesExt, WriteBytesExt};
use itertools::Itertools;
use serde_derive::Deserialize;
use std::cmp::{max, min};
use std::collections::HashSet;
use std::convert::TryInto;
use std::io::{Cursor, Read};
use std::mem::size_of;

#[derive(Debug, Clone)]
pub enum HllInstance {
    Sparse(SparseHll),
    Dense(DenseHll),
}

/// Implementation of HyperLogLog compatible ported from [airlift](https://github.com/airlift/airlift/blob/master/stats/src/main/java/io/airlift/stats/cardinality/HyperLogLog.java).
/// This implementation has to be binary compatible.
pub const MAX_BUCKETS: u32 = 65536;
impl HllInstance {
    pub fn new(num_buckets: u32) -> Result<HllInstance> {
        assert!(num_buckets <= MAX_BUCKETS);
        return Ok(HllInstance::Sparse(SparseHll::new(index_bit_length(
            num_buckets,
        )?)?));
    }

    pub fn num_buckets(&self) -> u32 {
        return match self {
            Sparse(s) => number_of_buckets(s.index_bit_len),
            Dense(d) => number_of_buckets(d.index_bit_len),
        };
    }

    /// Callers must check that `num_buckets()` is the same for `self` and `other`.
    pub fn merge_with(&mut self, o: &HllInstance) {
        assert_eq!(
            self.index_bit_len(),
            o.index_bit_len(),
            "merging HLLs with different number of buckets"
        );
        if self.merge_with_prepare(o) {
            self.make_dense_if_necessary()
        }
    }

    pub fn index_bit_len(&self) -> u8 {
        return match self {
            Sparse(s) => s.index_bit_len,
            Dense(d) => d.index_bit_len,
        };
    }

    /// Returns true iff `self.make_dense_if_necessary` has to be run.
    /// See comments inside the function for explanation on why we need this.
    fn merge_with_prepare(&mut self, o: &HllInstance) -> bool {
        match (self, o) {
            (Sparse(l), Sparse(r)) => {
                l.merge_with(r);
                // We need the make this call, but borrow checker won't let us use `self` here.
                // self.make_dense_if_necessary();
                return true;
            }
            (Dense(l), Sparse(r)) => {
                l.merge_with_sparse(r);
                return false;
            }
            (l, Dense(r)) => {
                l.ensure_dense().merge_with(r);
                return false;
            }
        }
    }

    pub fn cardinality(&self) -> u64 {
        match self {
            Sparse(s) => s.cardinality(),
            Dense(s) => s.cardinality(),
        }
    }

    /// Reads v1 of https://github.com/aggregateknowledge/hll-storage-spec and converts it to the
    /// Airlift representation of HLL. This means extra limitations on input and can produce
    /// different estimates due to implementation differences.
    pub fn read_hll_storage_spec(data: &[u8]) -> Result<HllInstance> {
        if data.len() < 3 {
            return Err(HllError::new(
                "Not enough bytes to read HLL header".to_string(),
            ));
        }
        // Read HLL header first.
        let v = (data[0] & 0xF0) >> 4;
        if v != 1 {
            return Err(HllError::new(format!(
                "Unknown HLL schema version ({}), can only read version 1.",
                v
            )));
        }
        const ENC_EMPTY: u8 = 1;
        const ENC_EXPLICIT: u8 = 2;
        const ENC_SPARSE: u8 = 3;
        const ENC_FULL: u8 = 4;
        let encoding = match data[0] & 0x0F {
            0 => {
                return Err(HllError::new(
                    "Cannot read HLL with undefined encoding".to_string(),
                ))
            }
            n if 1 <= n && n <= 4 => n,
            n => {
                return Err(HllError::new(format!(
                    "Unknown HLL encoding ordinal: {}",
                    n
                )))
            }
        };
        let reg_width = 1 + ((data[1] & 0b11100000) >> 5);
        if reg_width < 1 || 6 < reg_width {
            return Err(HllError::new(format!(
                "Register width must be between 1 and 6, got {}",
                reg_width
            )));
        }
        let log_num_buckets = data[1] & 0b00011111;
        // Note: the upper limit in storage spec is 31, but our implementation is limited to 16.
        if log_num_buckets < 4 || 16 < log_num_buckets {
            return Err(HllError::new(format!(
                "Log2m must be between 4 and 16, got {}",
                log_num_buckets
            )));
        }
        let num_buckets = 1u32 << log_num_buckets as u32;
        let _cutoff = data[2]; // Not used.

        // Read the HLL body.
        let data = &data[3..];
        match encoding {
            ENC_EMPTY => {
                if !data.is_empty() {
                    return Err(HllError::new(format!(
                        "HLL with encdoing EMPTY has data {} bytes",
                        data.len()
                    )));
                }
                return HllInstance::new(num_buckets);
            }
            ENC_EXPLICIT => {
                if data.len() % 8 != 0 {
                    return Err(HllError::new(format!(
                        "Size of EXPLICIT encoding is not a multiple of 8: {}",
                        data.len()
                    )));
                }
                let num_hashes = data.len() / 8;
                if 256 * 8 < data.len() {
                    return Err(HllError::new(format!(
                        "EXPLICIT encoding has {} hashes, expected no more than 256 ",
                        num_hashes
                    )));
                }
                // Convert to sparse representation in Airlift. By analogy with the code from postgress hll:
                // TODO Algorithm of conversion from EXPLICIT to SPARSE is not part of the storage specification,
                // so we use the algorithm from the PostgreSql as is.
                /*
                   uint64_t mask = nregs - 1;

                   size_t maxregval = (1 << nbits) - 1;

                   size_t ndx = elem & mask;

                   uint64_t ss_val = elem >> log2nregs;

                   size_t p_w = ss_val == 0 ? 0 : __builtin_ctzll(ss_val) + 1;

                   Assert(ndx < msc_regs_idx_limit());

                   if (p_w > maxregval)
                       p_w = maxregval;

                   if (mscp->msc_regs[ndx] < p_w)
                       mscp->msc_regs[ndx] = p_w;
                * */
                let mask = num_buckets as u64 - 1;
                let mut indices = Vec::with_capacity(num_hashes);
                let mut values = Vec::with_capacity(num_hashes);
                let mut data = Cursor::new(data);
                let maxval = (1 << reg_width) as u32 - 1;
                while data.position() < data.get_ref().len() as u64 {
                    let hash = data.read_u64::<BigEndian>().unwrap();
                    let ind = hash & mask;
                    let val = hash >> log_num_buckets;
                    let zeroes = if val == 0 {
                        0
                    } else {
                        val.trailing_zeros() + 1
                    };

                    let zeroes = if zeroes > maxval { maxval } else { zeroes };
                    indices.push(ind as u32);
                    values.push(zeroes as u8);
                }

                return Ok(HllInstance::Sparse(SparseHll::new_from_indices_and_values(
                    log_num_buckets,
                    indices,
                    &values,
                )?));
            }
            ENC_SPARSE => {
                let mut cursor = BitCursor::new(data);
                let entry_len = (log_num_buckets + reg_width) as usize;
                let mut indices = Vec::new();
                let mut values = Vec::new();
                while let Some(e) = cursor.read_bits(entry_len) {
                    indices.push((e >> reg_width) as u32);
                    values.push((e & ((1 << reg_width) - 1)) as u8);
                }
                return Ok(HllInstance::Sparse(SparseHll::new_from_indices_and_values(
                    log_num_buckets,
                    indices,
                    &values,
                )?));
            }
            ENC_FULL => {
                let expected_bits = num_buckets * reg_width as u32;
                let expected_len = expected_bits / 8 + (expected_bits % 8 != 0) as u32;
                if data.len() != expected_len as usize {
                    return Err(HllError::new(format!(
                        "Expected {} data bytes for encoding FULL with log2m={}, got {}",
                        expected_len,
                        log_num_buckets,
                        data.len()
                    )));
                }
                let mut values = Vec::with_capacity(num_buckets as usize);
                let mut cursor = BitCursor::new(data);
                for _ in 0..num_buckets {
                    values.push(cursor.read_bits(reg_width as usize).unwrap() as u8)
                }
                return Ok(HllInstance::Dense(DenseHll::new_from_entries(
                    log_num_buckets,
                    values,
                )?));
            }
            enc => panic!("Unhandled encoding ordinal {}", enc),
        }
    }

    pub fn read_snowflake(s: &str) -> Result<HllInstance> {
        #[derive(Deserialize)]
        struct SerializedHll {
            precision: u8,
            version: u8,
            sparse: Option<SparseEntries>,
            dense: Option<Vec<u8>>,
        }
        #[derive(Deserialize)]
        #[allow(non_snake_case)]
        struct SparseEntries {
            indices: Vec<u32>,
            maxLzCounts: Vec<u8>,
        }

        let ser: SerializedHll = serde_json::from_str(s)?;
        if ser.version != 4 {
            return Err(HllError::new(format!(
                "unsupported version of snowflake HLL: {}",
                ser.version
            )));
        }
        match (ser.sparse, ser.dense) {
            (Some(sparse), None) => {
                Ok(HllInstance::Sparse(SparseHll::new_from_indices_and_values(
                    ser.precision,
                    sparse.indices,
                    &sparse.maxLzCounts,
                )?))
            }
            (None, Some(dense)) => Ok(HllInstance::Dense(DenseHll::new_from_entries(
                ser.precision,
                dense,
            )?)),
            _ => Err(HllError::new(
                "expected exactly one of 'sparse' or 'dense' fields",
            )),
        }
    }

    pub fn read(data: &[u8]) -> Result<HllInstance> {
        if data.is_empty() {
            return Err(HllError::new("hll input data is empty"));
        }
        return match data[0] {
            TAG_SPARSE_V2 => Ok(HllInstance::Sparse(SparseHll::read(&data[1..])?)),
            TAG_DENSE_V1 => Ok(HllInstance::Dense(DenseHll::read_v1(&data[1..])?)),
            TAG_DENSE_V2 => Ok(HllInstance::Dense(DenseHll::read(&data[1..])?)),
            _ => Err(HllError::new(format!("invalid hll format tag {}", data[0]))),
        };
    }

    pub fn write(&self) -> Vec<u8> {
        return match self {
            Sparse(s) => s.write(),
            Dense(s) => s.write(),
        };
    }

    fn ensure_dense(&mut self) -> &mut DenseHll {
        if let Dense(d) = self {
            return d;
        }

        let converted;
        {
            if let Sparse(s) = self {
                converted = s.to_dense();
            } else {
                unreachable!("definitely sparse at this point")
            }
        }
        *self = Dense(converted);

        if let Dense(d) = self {
            return d;
        }
        unreachable!("definitely Dense() at this point")
    }

    fn make_dense_if_necessary(&mut self) {
        let should_switch;
        if let Sparse(s) = self {
            should_switch =
                DenseHll::estimate_in_memory_size(s.index_bit_len) < s.estimate_in_memory_size();
        } else {
            should_switch = false;
        }
        if should_switch {
            self.ensure_dense();
        }
    }
}

#[derive(Debug, Clone)]
pub struct SparseHll {
    index_bit_len: u8,
    entries: Vec<u32>,
}

impl SparseHll {
    // 6 bits to encode the number of zeros after the truncated hash
    // and be able to fit the encoded value in an integer
    const VALUE_BITS: u32 = 6;
    const VALUE_MASK: u32 = (1 << SparseHll::VALUE_BITS) - 1;
    const EXTENDED_PREFIX_BITS: u8 = 32 - SparseHll::VALUE_BITS as u8;

    pub fn new(index_bit_len: u8) -> Result<SparseHll> {
        SparseHll::is_valid_bit_len(index_bit_len)?;
        return Ok(SparseHll {
            index_bit_len,
            entries: Vec::with_capacity(1),
        });
    }

    fn new_from_indices_and_values(
        index_bit_len: u8,
        indices: Vec<u32>,
        values: &[u8],
    ) -> Result<SparseHll> {
        Self::is_valid_bit_len(index_bit_len)?;
        if values.len() != indices.len() {
            return Err(HllError::new("values and indices are or different lengths"));
        }

        // Turn indices into the entries array inplace.
        let mut entries = indices;
        for i in 0..entries.len() {
            // TODO: validate range of index values.
            // High bits are bucket index, followed by zeros and the actual value.
            // Airlift stores first EXTENDED_PREFIX_BITS bits of the hash in the available bits, and calculate leading
            // zeroes as leading zeroes in this hash part
            // if all bits of this hash part is zero then least zeroes count is EXTENDED_PREFIX_BITS - index_bit_len + number from least VALUE_BITS bits of value
            //
            // But inputs of this
            // function do not have this information, so we set leading zeroes manually.
            // See each_bucket function for more information about expected format
            let bucket = entries[i];
            let hash_part_bits = Self::EXTENDED_PREFIX_BITS - index_bit_len;
            let v = if values[i] <= hash_part_bits {
                1 << (32 - index_bit_len - values[i])
            } else {
                (values[i] - hash_part_bits - 1) as u32 & Self::VALUE_MASK
            };

            entries[i] = (bucket << (32 - index_bit_len)) | v;
        }

        // Sort by bucket index.
        entries
            .sort_unstable_by(|l, r| (l >> (32 - index_bit_len)).cmp(&(r >> (32 - index_bit_len))));

        Ok(SparseHll {
            index_bit_len,
            entries,
        })
    }

    pub fn read(data: &[u8]) -> Result<SparseHll> {
        let mut c = Cursor::new(data);

        let index_bit_len = c.read_u8()?;
        // TODO: review if LittleEndian is the right choice.
        let num_entries = c.read_u16::<LittleEndian>()? as usize;
        // TODO: use memcpy-friendly primitives for efficiency.
        let mut entries = vec![0; num_entries];
        for i in 0..num_entries {
            entries[i] = c.read_u32::<LittleEndian>()?;
        }
        if c.position() != data.len() as u64 {
            return Err(HllError::new("input is too big"));
        }
        return Ok(SparseHll {
            index_bit_len,
            entries,
        });
    }

    pub fn write(&self) -> Vec<u8> {
        let size = 1/*format tag*/ + 1/*index bit len*/ + 2 /*entries.len*/ + 4*self.entries.len();
        let mut r = Vec::with_capacity(size);

        r.write_u8(TAG_SPARSE_V2).unwrap();
        r.write_u8(self.index_bit_len).unwrap();
        r.write_u16::<LittleEndian>(self.entries.len().try_into().unwrap())
            .unwrap();
        for e in &self.entries {
            r.write_u32::<LittleEndian>(*e).unwrap();
        }
        return r;
    }

    pub fn cardinality(&self) -> u64 {
        // Estimate the cardinality using linear counting over the theoretical 2^EXTENDED_BITS_LENGTH buckets available due
        // to the fact that we're recording the raw leading EXTENDED_BITS_LENGTH of the hash. This produces much better precision
        // while in the sparse regime.
        let total_buckets = number_of_buckets(SparseHll::EXTENDED_PREFIX_BITS);
        let zero_buckets = total_buckets - self.entries.len() as u32;
        return linear_counting(zero_buckets, total_buckets).round() as u64;
    }

    pub fn merge_with(&mut self, o: &SparseHll) {
        self.entries = self.merge_entries(o);
    }

    pub fn to_dense(&self) -> DenseHll {
        // TODO: this can panic if Sparse HLL had too much precision.
        let mut d = DenseHll::new(self.index_bit_len);
        self.each_bucket(|bucket, zeros| d.insert(bucket, zeros));
        return d;
    }

    fn estimate_in_memory_size(&self) -> usize {
        return size_of::<SparseHll>() + 32 * self.entries.capacity();
    }

    fn each_bucket<F>(&self, mut f: F)
    where
        F: FnMut(/*bucket: */ u32, /*value: */ u8),
    {
        for i in 0..self.entries.len() {
            let entry = self.entries[i];

            // The leading EXTENDED_BITS_LENGTH are a proper subset of the original hash.
            // Since we're guaranteed that indexBitLength is <= EXTENDED_BITS_LENGTH,
            // the value stored in those bits corresponds to the bucket index in the dense HLL
            let bucket = SparseHll::decode_bucket_index_with_bit_len(self.index_bit_len, entry);

            // compute the number of zeros between indexBitLength and EXTENDED_BITS_LENGTH
            let mut zeros = (entry << self.index_bit_len).leading_zeros() as u8;

            // if zeros > EXTENDED_BITS_LENGTH - indexBits, it means all those bits were zeros,
            // so look at the entry value, which contains the number of leading 0 *after* EXTENDED_BITS_LENGTH
            let bits = SparseHll::EXTENDED_PREFIX_BITS - self.index_bit_len;
            if zeros >= bits {
                zeros = bits + SparseHll::decode_bucket_value(entry);
            }

            f(bucket, zeros + 1);
        }
    }

    fn merge_entries(&self, other: &SparseHll) -> Vec<u32> {
        let mut result = vec![0; self.entries.len() + other.entries.len()];
        let mut left_index = 0;
        let mut right_index = 0;

        let mut index = 0;
        while left_index < self.entries.len() && right_index < other.entries.len() {
            let left = SparseHll::decode_bucket_index(self.entries[left_index]);
            let right = SparseHll::decode_bucket_index(other.entries[right_index]);

            if left < right {
                result[index] = self.entries[left_index];
                index += 1;
                left_index += 1;
            } else if left > right {
                result[index] = other.entries[right_index];
                index += 1;
                right_index += 1;
            } else {
                let value = max(
                    SparseHll::decode_bucket_value(self.entries[left_index]),
                    SparseHll::decode_bucket_value(other.entries[right_index]),
                );
                result[index] = SparseHll::encode_entry(left, value);
                index += 1;
                left_index += 1;
                right_index += 1;
            }
        }

        while left_index < self.entries.len() {
            result[index] = self.entries[left_index];
            index += 1;
            left_index += 1;
        }

        while right_index < other.entries.len() {
            result[index] = other.entries[right_index];
            index += 1;
            right_index += 1;
        }

        result.resize(index, 0);
        return result;
    }

    fn encode_entry(bucket_index: u32, value: u8) -> u32 {
        return (bucket_index << SparseHll::VALUE_BITS) | value as u32;
    }

    fn decode_bucket_value(entry: u32) -> u8 {
        return (entry & SparseHll::VALUE_MASK) as u8;
    }

    fn decode_bucket_index(entry: u32) -> u32 {
        return SparseHll::decode_bucket_index_with_bit_len(SparseHll::EXTENDED_PREFIX_BITS, entry);
    }

    fn decode_bucket_index_with_bit_len(index_bit_len: u8, entry: u32) -> u32 {
        return entry >> (32 - index_bit_len);
    }

    fn is_valid_bit_len(index_bit_len: u8) -> Result<()> {
        if 1 <= index_bit_len && index_bit_len <= SparseHll::EXTENDED_PREFIX_BITS {
            Ok(())
        } else {
            Err(HllError::new(format!(
                "index_bit_len is out of range: {}",
                index_bit_len
            )))
        }
    }
}

#[derive(Debug, Clone)]
pub struct DenseHll {
    index_bit_len: u8,
    baseline: u8,
    baseline_count: u32,
    deltas: Vec<u8>,

    overflow_buckets: Vec<u32>,
    overflow_values: Vec<u8>,
}

impl DenseHll {
    const LINEAR_COUNTING_MIN_EMPTY_BUCKETS: f64 = 0.4;
    const BITS_PER_BUCKET: u32 = 4;
    const MAX_DELTA: u8 = (1 << DenseHll::BITS_PER_BUCKET) - 1;
    const BUCKET_MASK: u8 = (1 << DenseHll::BITS_PER_BUCKET) - 1;
    const OVERFLOW_GROW_INCREMENT: usize = 5;

    pub fn new(index_bit_len: u8) -> DenseHll {
        DenseHll::is_valid_bit_len(index_bit_len).unwrap();

        let num_buckets = number_of_buckets(index_bit_len) as u32;
        return DenseHll {
            index_bit_len,
            baseline: 0,
            baseline_count: num_buckets,
            deltas: vec![0; (num_buckets * DenseHll::BITS_PER_BUCKET / 8) as usize],
            overflow_buckets: Vec::new(),
            overflow_values: Vec::new(),
        };
    }

    pub fn new_from_entries(index_bit_len: u8, values: Vec<u8>) -> Result<DenseHll> {
        DenseHll::is_valid_bit_len(index_bit_len)?;
        let num_buckets = number_of_buckets(index_bit_len);
        if values.len() != num_buckets as usize {
            return Err(HllError::new(format!(
                "expected {} entries in dense HLL with precision {}, got {} entries",
                num_buckets,
                index_bit_len,
                values.len()
            )));
        }

        let baseline = *values.iter().min().unwrap();
        let mut baseline_count = 0;
        let mut overflow_buckets = Vec::new();
        let mut overflow_values = Vec::new();

        let mut process_for_delta = |bucket, mut v| {
            v -= baseline;
            if v == 0 {
                baseline_count += 1
            }
            if DenseHll::MAX_DELTA < v {
                overflow_buckets.push(bucket as u32);
                overflow_values.push(v - DenseHll::MAX_DELTA);
                v = DenseHll::MAX_DELTA
            }
            v
        };

        let mut deltas = vec![0; (num_buckets * DenseHll::BITS_PER_BUCKET / 8) as usize];
        for i in 0..values.len() / 2 {
            deltas[i] = process_for_delta(2 * i, values[2 * i]) << 4
                | process_for_delta(2 * i + 1, values[2 * i + 1]);
        }

        Ok(DenseHll {
            index_bit_len,
            baseline,
            baseline_count,
            deltas,
            overflow_buckets,
            overflow_values,
        })
    }

    pub fn read_v1(_data: &[u8]) -> Result<DenseHll> {
        // TODO: implement this for completeness. Airlift can read Dense HLL in V1 format.
        return Err(HllError::new(
            "reading of v1 dense sketches is not implemented",
        ));
    }

    pub fn read(data: &[u8]) -> Result<DenseHll> {
        let mut c = Cursor::new(data);

        let index_bit_len = c.read_u8()?;
        DenseHll::is_valid_bit_len(index_bit_len)?;
        let num_buckets = number_of_buckets(index_bit_len);

        let baseline = c.read_u8()?;
        let mut deltas: Vec<u8> = vec![0; (num_buckets / 2) as usize];
        c.read_exact(deltas.as_mut_slice())?;

        // Only the Format.v2 version here.
        let num_overflows = c.read_u16::<LittleEndian>()? as usize;
        if num_buckets < num_overflows as u32 {
            return Err(HllError::new("Overflow entries is greater than actual number of buckets (possibly corrupt input)"));
        }

        // TODO: use memcpy-friendly primitives for efficiency.
        let mut overflow_buckets: Vec<u32> = vec![0; num_overflows];
        for b in &mut overflow_buckets {
            *b = c.read_u16::<LittleEndian>()? as u32;
            if num_buckets < *b {
                return Err(HllError::new("Overflow bucket index is out of range"));
            }
        }

        let mut overflow_values = vec![0; num_overflows];
        for ov in &mut overflow_values {
            let v = c.read_i8()?;
            if v <= 0 {
                return Err(HllError::new("Overflow bucket value must be > 0"));
            }
            *ov = v as u8;
        }

        if c.position() != data.len() as u64 {
            return Err(HllError::new("input is too big"));
        }

        let mut baseline_count: u32 = 0;
        for i in 0..num_buckets {
            if DenseHll::get_delta_impl(&deltas, i) == 0 {
                baseline_count += 1;
            }
        }

        return Ok(DenseHll {
            index_bit_len,
            baseline,
            baseline_count,
            deltas,
            overflow_buckets,
            overflow_values,
        });
    }

    pub fn write(&self) -> Vec<u8> {
        // TODO: let r = Vec::with_capacity(estimatedSerializedSize());
        let mut r = Vec::new();
        r.write_u8(TAG_DENSE_V2).unwrap();
        r.write_u8(self.index_bit_len).unwrap();
        r.write_u8(self.baseline).unwrap();
        r.extend_from_slice(&self.deltas);
        r.write_u16::<LittleEndian>(self.overflow_buckets.len().try_into().unwrap())
            .unwrap();

        // sort overflow arrays to get consistent serialization for equivalent HLLs
        let (of_buckets, of_values) = self.sort_overflows();

        // TODO: use primitives that produce memcpy().
        for e in of_buckets {
            r.write_u16::<LittleEndian>(e.try_into().unwrap()).unwrap();
        }
        r.extend_from_slice(&of_values);
        return r;
    }

    pub fn cardinality(&self) -> u64 {
        let num_buckets = number_of_buckets(self.index_bit_len);

        // if baseline is zero, then baselineCount is the number of buckets with value 0
        if (self.baseline == 0)
            && (self.baseline_count
                > (DenseHll::LINEAR_COUNTING_MIN_EMPTY_BUCKETS * num_buckets as f64) as u32)
        {
            return linear_counting(self.baseline_count, num_buckets).round() as u64;
        }

        let mut sum = 0.;
        for i in 0..num_buckets {
            let value = self.get_value(i);
            sum += 1.0 / (1 << value) as f64;
        }

        let estimate = (alpha(self.index_bit_len) * num_buckets as f64 * num_buckets as f64) / sum;
        return self.correct_bias(estimate).round() as u64;
    }

    pub fn merge_with_sparse(&mut self, other: &SparseHll) {
        debug_assert_eq!(
            self.index_bit_len,
            other.index_bit_len,
            "Cannot merge HLLs with different number of buckets: {} vs {}",
            number_of_buckets(self.index_bit_len),
            number_of_buckets(other.index_bit_len)
        );

        other.each_bucket(|bucket, value| self.insert(bucket, value))
    }

    pub fn merge_with(&mut self, other: &DenseHll) {
        debug_assert_eq!(
            self.index_bit_len,
            other.index_bit_len,
            "Cannot merge HLLs with different number of buckets: {} vs {}",
            number_of_buckets(self.index_bit_len),
            number_of_buckets(other.index_bit_len)
        );

        let new_baseline = max(self.baseline, other.baseline);
        let mut baseline_count = 0;

        let mut bucket = 0;
        for i in 0..self.deltas.len() {
            let mut new_slot = 0;

            let slot1 = self.deltas[i];
            let slot2 = other.deltas[i];

            for shift in &[4, 0] {
                let delta1 = (slot1 >> shift) & 0b1111;
                let delta2 = (slot2 >> shift) & 0b1111;

                let mut value1 = self.baseline + delta1;
                let mut value2 = other.baseline + delta2;

                let overflow_entry;
                if delta1 == DenseHll::MAX_DELTA {
                    overflow_entry = self.find_overflow_entry(bucket);
                    if let Some(oe) = overflow_entry {
                        value1 += self.overflow_values[oe] as u8;
                    }
                } else {
                    overflow_entry = None
                }

                if delta2 == DenseHll::MAX_DELTA {
                    value2 += other.get_overflow(bucket) as u8;
                }

                let new_value = max(value1, value2);
                let mut new_delta = new_value - new_baseline;

                if new_delta == 0 {
                    baseline_count += 1;
                }

                new_delta = self.update_overflow(bucket, overflow_entry, new_delta);

                new_slot <<= 4;
                new_slot |= new_delta;
                bucket += 1;
            }

            self.deltas[i] = new_slot as u8;
        }

        self.baseline = new_baseline as u8;
        self.baseline_count = baseline_count;

        // all baseline values in one of the HLLs lost to the values
        // in the other HLL, so we need to adjust the final baseline
        self.adjust_baseline_if_needed();
    }

    pub fn insert(&mut self, bucket: u32, value: u8) {
        let mut delta = (value as i32) - (self.baseline as i32);
        let old_delta = self.get_delta(bucket) as i32;

        if delta <= old_delta
            || (old_delta == (DenseHll::MAX_DELTA as i32)
                && (delta <= old_delta + (self.get_overflow(bucket) as i32)))
        {
            // the old bucket value is (baseline + oldDelta) + possibly an overflow, so it's guaranteed to be >= the new value
            return;
        }

        if delta > (DenseHll::MAX_DELTA as i32) {
            let overflow: u8 = (delta - DenseHll::MAX_DELTA as i32).try_into().unwrap();

            let overflow_entry = self.find_overflow_entry(bucket);
            if let Some(oe) = overflow_entry {
                self.overflow_values[oe] = overflow;
            } else {
                self.add_overflow(bucket, overflow);
            }

            delta = DenseHll::MAX_DELTA as i32;
        }

        self.set_delta(bucket, delta.try_into().unwrap());

        if old_delta == 0 {
            self.baseline_count -= 1;
            self.adjust_baseline_if_needed();
        }
    }

    #[allow(dead_code)]
    fn insert_hash(&mut self, hash: u64) {
        let index = compute_index(hash, self.index_bit_len);
        let value = compute_value(hash, self.index_bit_len);

        self.insert(index, value);
    }

    fn correct_bias(&self, raw_estimate: f64) -> f64 {
        let estimates = bias_correction::RAW_ESTIMATES[self.index_bit_len as usize - 4];
        if raw_estimate < estimates[0] || estimates[estimates.len() - 1] < raw_estimate {
            return raw_estimate;
        }

        let biases = bias_correction::BIAS[self.index_bit_len as usize - 4];

        let position = search(raw_estimate, estimates);

        let bias;
        if position >= 0 {
            bias = biases[position as usize];
        } else {
            // interpolate
            let insertion_point = -(position + 1) as usize;

            let x0 = estimates[insertion_point - 1];
            let y0 = biases[insertion_point - 1];
            let x1 = estimates[insertion_point];
            let y1 = biases[insertion_point];

            bias = (((raw_estimate - x0) * (y1 - y0)) / (x1 - x0)) + y0;
        }
        return raw_estimate - bias;
    }

    fn find_overflow_entry(&self, bucket: u32) -> Option<usize> {
        return self
            .overflow_buckets
            .iter()
            .find_position(|x| **x == bucket)
            .map(|x| x.0);
    }

    fn adjust_baseline_if_needed(&mut self) {
        while self.baseline_count == 0 {
            self.baseline += 1;

            for bucket in 0..number_of_buckets(self.index_bit_len) {
                let mut delta = self.get_delta(bucket);

                let mut has_overflow = false;
                if delta == DenseHll::MAX_DELTA {
                    // scan overflows
                    for i in 0..self.overflow_buckets.len() {
                        if self.overflow_buckets[i] == bucket {
                            has_overflow = true;
                            self.overflow_values[i] -= 1;

                            if self.overflow_values[i] == 0 {
                                let last_entry = self.overflow_buckets.len() - 1;
                                if i < last_entry {
                                    // remove the entry by moving the last entry to this position
                                    self.overflow_buckets[i] = self.overflow_buckets[last_entry];
                                    self.overflow_values[i] = self.overflow_values[last_entry];
                                }
                                self.overflow_buckets.pop();
                                self.overflow_values.pop();
                            }
                            break;
                        }
                    }
                }

                if !has_overflow {
                    // getDelta is guaranteed to return a value greater than zero
                    // because baselineCount is zero (i.e., number of deltas with zero value)
                    // So it's safe to decrement here
                    delta -= 1;
                    self.set_delta(bucket, delta);
                }

                if delta == 0 {
                    self.baseline_count += 1;
                }
            }
        }
    }

    fn update_overflow(&mut self, bucket: u32, overflow_entry: Option<usize>, mut delta: u8) -> u8 {
        if delta > DenseHll::MAX_DELTA {
            if let Some(oe) = overflow_entry {
                // update existing overflow
                self.overflow_values[oe] = delta - DenseHll::MAX_DELTA;
            } else {
                self.add_overflow(bucket, delta - DenseHll::MAX_DELTA);
            }
            delta = DenseHll::MAX_DELTA;
        } else if let Some(oe) = overflow_entry {
            self.remove_overflow(oe);
        }
        return delta as u8;
    }

    fn add_overflow(&mut self, bucket: u32, overflow: u8) {
        // add new delta
        if self.overflow_buckets.capacity() == self.overflow_buckets.len() {
            self.overflow_buckets
                .reserve_exact(DenseHll::OVERFLOW_GROW_INCREMENT);
        }
        if self.overflow_values.capacity() == self.overflow_values.len() {
            self.overflow_values
                .reserve_exact(DenseHll::OVERFLOW_GROW_INCREMENT);
        }

        self.overflow_buckets.push(bucket);
        self.overflow_values.push(overflow);
    }

    fn remove_overflow(&mut self, overflow_entry: usize) {
        let overflows = self.overflow_buckets.len();
        // remove existing overflow
        self.overflow_buckets[overflow_entry] = self.overflow_buckets[overflows - 1];
        self.overflow_values[overflow_entry] = self.overflow_values[overflows - 1];

        self.overflow_buckets.pop();
        self.overflow_values.pop();
    }

    fn get_value(&self, bucket: u32) -> u32 {
        let mut delta = self.get_delta(bucket) as u32;
        if delta == DenseHll::MAX_DELTA as u32 {
            delta += self.get_overflow(bucket) as u32;
        }
        return self.baseline as u32 + delta;
    }

    fn get_overflow(&self, bucket: u32) -> u8 {
        for i in 0..self.overflow_buckets.len() {
            if self.overflow_buckets[i] == bucket {
                return self.overflow_values[i];
            }
        }
        return 0;
    }

    fn get_delta(&self, bucket: u32) -> u8 {
        return DenseHll::get_delta_impl(&self.deltas, bucket);
    }

    fn get_delta_impl(deltas: &[u8], bucket: u32) -> u8 {
        let slot = DenseHll::bucket_to_slot(bucket) as usize;
        return (deltas[slot] >> DenseHll::shift_for_bucket(bucket)) & DenseHll::BUCKET_MASK;
    }

    fn set_delta(&mut self, bucket: u32, value: u8) {
        let slot = DenseHll::bucket_to_slot(bucket) as usize;

        // clear the old value
        let clear_mask = (DenseHll::BUCKET_MASK << DenseHll::shift_for_bucket(bucket)) as u8;
        self.deltas[slot] &= !clear_mask;

        // set the new value
        let set_mask = (value << DenseHll::shift_for_bucket(bucket)) as u8;
        self.deltas[slot] |= set_mask;
    }

    fn bucket_to_slot(bucket: u32) -> u32 {
        return bucket >> 1;
    }

    fn shift_for_bucket(bucket: u32) -> u32 {
        // ((1 - bucket) % 2) * BITS_PER_BUCKET
        return ((!bucket) & 1) << 2;
    }

    fn is_valid_bit_len(index_bit_len: u8) -> Result<()> {
        if 1 <= index_bit_len && index_bit_len <= 16 {
            Ok(())
        } else {
            Err(HllError::new(format!(
                "index_bit_len is out of range: {}",
                index_bit_len
            )))
        }
    }

    /// Used as a threshold to move from sparse to dense representation.
    fn estimate_in_memory_size(index_bit_len: u8) -> usize {
        // These estimates can be different from those used in Airlift, so transition from sparse
        // to dense representation can happen at different points.

        // note: we don't take into account overflow entries since their number can vary.
        return size_of::<DenseHll>() + /*deltas*/8 * number_of_buckets(index_bit_len) as usize / 2;
    }

    /// Unlike airlift, we provide a copy of the overflow_bucket to to the reference semantics.
    // TODO: we should do this in-place.
    fn sort_overflows(
        &self,
    ) -> (
        /*overflow_buckets*/ Vec<u32>,
        /*overflow_values*/ Vec<u8>,
    ) {
        // Would be nice to replace with library sort.
        // Not clear how to swap elements in both overflow_buckets and overflow_values, though.

        let mut of_buckets = self.overflow_buckets.clone();
        let mut of_values = self.overflow_values.clone();

        // traditional insertion sort (ok for small arrays)
        for i in 1..of_buckets.len() {
            for j in (1..i + 1).rev() {
                // j = i, i-2, ..., 1
                if of_buckets[j - 1] <= of_buckets[j] {
                    break;
                }
                let bucket = of_buckets[j];
                let value = of_values[j];

                of_buckets[j] = of_buckets[j - 1];
                of_values[j] = of_values[j - 1];

                of_buckets[j - 1] = bucket;
                of_values[j - 1] = value;
            }
        }

        return (of_buckets, of_values);
    }

    #[allow(dead_code)]
    pub fn verify(&self) {
        let mut zero_deltas = 0;
        for i in 0..number_of_buckets(self.index_bit_len) {
            if self.get_delta(i) == 0 {
                zero_deltas += 1;
            }
        }

        assert_eq!(
            zero_deltas, self.baseline_count,
            "baseline count ({}) doesn't match number of zero deltas ({})",
            self.baseline_count, zero_deltas
        );

        let mut overflows = HashSet::new();
        for i in 0..self.overflow_buckets.len() {
            let bucket = self.overflow_buckets[i];
            overflows.insert(bucket);

            assert!(
                0 < self.overflow_values[i],
                "Overflow at {} for bucket {} is 2",
                i,
                bucket
            );
            assert_eq!(self.get_delta(bucket), DenseHll::MAX_DELTA,
                    "delta in bucket {} is less than MAX_DELTA ({} < {}) even though there's an associated overflow entry",
                    bucket, self.get_delta(bucket), DenseHll::MAX_DELTA);
        }

        assert_eq!(
            overflows.len(),
            self.overflow_buckets.len(),
            "Duplicate overflow buckets: {:?}",
            self.overflow_buckets
        );
    }
}

// TODO: replace with a library routine for binary search.
fn search(raw_estimate: f64, estimate_curve: &[f64]) -> i32 {
    let mut low: usize = 0;
    let mut high = estimate_curve.len() - 1;

    while low <= high {
        let middle = (low + high) >> 1;

        let middle_value = estimate_curve[middle];

        if raw_estimate > middle_value {
            low = middle + 1;
        } else if raw_estimate < middle_value {
            high = middle - 1;
        } else {
            return middle as i32;
        }
    }

    return -(low as i32 + 1);
}

fn index_bit_length(n: u32) -> Result<u8> {
    if n.is_power_of_two() {
        Ok(n.trailing_zeros() as u8)
    } else {
        Err(HllError::new(format!("expected a power of two, got {}", n)))
    }
}

#[allow(dead_code)]
fn compute_index(hash: u64, index_bit_len: u8) -> u32 {
    return (hash >> (64 - index_bit_len)) as u32;
}

fn compute_value(hash: u64, index_bit_len: u8) -> u8 {
    return number_of_leading_zeros(hash, index_bit_len) + 1;
}

#[allow(dead_code)]
fn number_of_leading_zeros(hash: u64, index_bit_len: u8) -> u8 {
    // place a 1 in the LSB to preserve the original number of leading zeros if the hash happens to be 0.
    let value = (hash << index_bit_len) | (1 << (index_bit_len - 1));
    return value.leading_zeros() as u8;
}

fn number_of_buckets(index_bit_len: u8) -> u32 {
    return 1 << index_bit_len;
}

fn alpha(index_bit_len: u8) -> f64 {
    return match index_bit_len {
        4 => 0.673,
        5 => 0.697,
        6 => 0.709,
        _ => 0.7213 / (1. + 1.079 / number_of_buckets(index_bit_len) as f64),
    };
}

fn linear_counting(zero_buckets: u32, total_buckets: u32) -> f64 {
    let total_f = total_buckets as f64;
    return total_f * (total_f / (zero_buckets as f64)).ln();
}

// const TAG_SPARSE_V1: u8 = 0; // Unsupported.
const TAG_DENSE_V1: u8 = 1;
const TAG_SPARSE_V2: u8 = 2;
const TAG_DENSE_V2: u8 = 3;

struct BitCursor<'a> {
    input: &'a [u8],
    pos: usize,
    bit_pos: usize,
}

impl BitCursor<'_> {
    pub fn new(input: &[u8]) -> BitCursor<'_> {
        BitCursor {
            input,
            pos: 0,
            bit_pos: 0,
        }
    }

    /// This will return [None] if we managed to read less than [num_bits] and hit the end of the
    /// input buffer. This allows to ignore padding bytes.
    pub fn read_bits(&mut self, mut num_bits: usize) -> Option<u64> {
        debug_assert!(num_bits <= 64);
        let mut r = 0;
        while num_bits != 0 {
            if self.pos == self.input.len() {
                return None;
            }
            let read_bits = min(num_bits, 8 - self.bit_pos);

            let high_zeros = (1u64 << (8 - self.bit_pos)) - 1;
            let b = (self.input[self.pos] as u64 & high_zeros) >> (8 - self.bit_pos - read_bits);
            r |= (b as u64) << (num_bits - read_bits);
            num_bits -= read_bits;

            self.bit_pos += read_bits;
            if self.bit_pos == 8 {
                self.pos += 1;
                self.bit_pos = 0;
            }
        }
        return Some(r);
    }
}

#[cfg(test)]
mod tests {
    use crate::instance::{compute_index, compute_value, number_of_buckets};
    use std::cmp::max;

    mod serialization {
        use crate::instance::HllInstance;

        #[test]
        fn test_snowflake() {
            let sparse = HllInstance::read_snowflake(
                r#"{"precision": 12,
                      "sparse": {
                        "indices": [223,736,976,1041,1256,1563,1811,2227,2327,2434,2525,2656,2946,2974,3256,3745,3771,4066],
                        "maxLzCounts": [1,2,1,4,2,2,3,1,1,2,4,2,1,1,2,3,2,1]
                      },
                      "version": 4
                    }"#).unwrap();
            let sparse = match sparse {
                HllInstance::Sparse(s) => s,
                HllInstance::Dense(_) => panic!("expected to read sparse hll"),
            };
            assert_eq!(sparse.index_bit_len, 12);
            assert_eq!(sparse.cardinality(), 18);
            assert_eq!(
                &sparse.entries,
                &[
                    234356736, 772014080, 1023934464, 1091633152, 1317273600, 1639186432,
                    1899102208, 2335703040, 2440560640, 2552496128, 2647719936, 2785280000,
                    3089629184, 3118989312, 3414425600, 3927048192, 3954442240, 4264034304
                ]
            );
            let mut lz_counts = vec![];
            sparse.each_bucket(|_, value| {
                lz_counts.push(value);
            });

            assert_eq!(
                lz_counts,
                vec![1, 2, 1, 4, 2, 2, 3, 1, 1, 2, 4, 2, 1, 1, 2, 3, 2, 1]
            );
            assert_eq!(sparse.to_dense().cardinality(), 18);

            let sparse = HllInstance::read_snowflake(
                r#"{"precision": 12,
                      "sparse": {
                        "indices": [223,736,976,1041,1256,1563,1811,2227,2327,2434,2525,2656,2946,2974,3256,3745,3771,4066],
                        "maxLzCounts": [1,2,8,10,11,12,13,14,15,16,17,18,19,20,60,52,62,63]
                      },
                      "version": 4
                    }"#).unwrap();
            let sparse = match sparse {
                HllInstance::Sparse(s) => s,
                HllInstance::Dense(_) => panic!("expected to read sparse hll"),
            };
            assert_eq!(sparse.index_bit_len, 12);

            let mut lz_counts = vec![];
            sparse.each_bucket(|_, value| {
                lz_counts.push(value);
            });

            assert_eq!(
                lz_counts,
                vec![1, 2, 8, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 60, 52, 62, 63]
            );

            let dense = HllInstance::read_snowflake(
                r#"{
  "dense":[0,0,3,1,0,1,2,2,0,0,3,1,2,0,2,1,0,4,1,4,0,6,2,1,1,5,1,3,4,0,2,4,0,0,1,2,1,0,3,1,0,0,1,1,1,2,0,2,1,2,0,0,2,1,1,1,3,4,0,1,1,1,0,3,4,7,8,2,1,1,2,7,2,2,4,3,1,0,3,6,2,1,3,4,8,1,0,3,2,1,2,2,3,1,1,3,1,2,2,0,0,3,6,7,2,3,0,1,3,0,5,0,5,4,0,2,0,0,5,2,1,2,2,4,0,12,2,4,4,2,2,2,1,1,1,0,0,1,0,3,0,2,2,1,2,2,0,0,0,4,2,4,2,2,3,3,1,3,3,1,0,1,4,1,3,2,0,1,1,10,3,0,2,3,2,1,2,5,0,4,1,4,0,0,3,1,3,2,0,1,1,8,1,1,5,6,1,1,0,0,0,3,3,0,1,0,2,0,3,0,2,2,1,1,1,4,1,2,1,6,8,0,3,10,0,4,0,1,3,1,0,4,4,0,3,4,3,0,0,3,0,6,0,1,3,7,3,1,2,0,1,4,2,9,3,2,0,2,0,0,2,1,0,1,7,1,0,4,4,4,4,1,0,0,1,6,3,1,3,6,3,1,3,2,1,5,0,1,0,2,0,3,2,8,2,1,3,3,2,1,0,1,0,1,3,1,3,2,3,1,1,1,1,0,4,2,4,0,1,4,2,1,1,2,2,1,3,0,4,3,3,1,2,2,3,5,3,1,0,2,2,3,3,1,0,4,2,2,1,0,3,3,3,1,2,6,2,2,3,0,2,2,2,2,5,0,0,2,2,1,0,1,0,3,4,5,3,4,4,2,2,0,0,2,2,0,2,7,2,3,3,1,3,4,0,1,7,4,3,0,7,0,0,1,0,1,0,3,8,7,2,1,5,4,3,1,5,2,3,0,2,3,1,1,2,0,1,0,1,3,1,1,2,4,1,2,1,3,3,2,4,2,1,1,8,0,2,0,1,4,3,7,1,2,2,3,2,0,3,4,2,0,1,1,0,0,2,2,6,2,1,1,1,4,2,0,2,2,1,5,0,0,4,0,3,3,0,2,1,3,2,1,0,6,6,5,8,0,2,2,1,1,4,0,3,2,0,2,3,1,0,0,1,2,2,0,1,1,3,3,0,1,4,1,1,3,1,1,3,2,3,4,0,4,3,4,0,6,1,0,3,2,1,1,0,2,5,0,3,2,3,3,3,1,2,9,2,1,1,4,6,1,5,0,4,1,4,1,2,0,2,1,2,1,0,2,0,0,1,3,6,0,9,12,0,7,3,0,1,2,4,5,2,0,1,1,9,1,4,8,3,0,1,1,2,1,3,0,0,1,1,1,3,4,7,8,0,1,2,0,0,5,2,0,1,4,2,1,1,5,2,8,2,3,1,2,0,6,3,3,1,0,3,0,1,2,3,3,2,0,0,2,3,0,3,6,3,4,4,2,2,0,1,0,2,2,1,0,0,2,1,1,5,6,1,6,0,1,6,2,2,0,1,1,3,0,1,2,3,1,2,2,3,1,3,0,2,5,4,1,1,2,2,3,1,1,2,2,1,0,2,1,3,4,0,1,0,2,0,0,1,5,2,1,2,0,2,1,5,2,4,3,1,0,2,3,1,4,1,1,1,3,2,0,2,1,2,3,5,0,1,2,5,4,2,1,1,1,1,1,4,4,1,2,1,4,3,3,1,2,9,0,2,2,3,2,1,2,3,0,0,3,1,0,2,4,0,5,0,3,2,4,1,4,6,1,3,4,1,4,4,5,1,1,0,1,2,0,0,1,1,1,2,0,1,1,4,4,0,3,0,2,2,2,0,6,1,1,4,3,0,1,2,4,2,1,0,3,3,1,5,3,3,0,7,2,0,2,2,0,0,3,5,4,1,3,1,1,1,0,3,0,2,1,0,2,2,5,5,1,1,4,3,1,5,1,2,0,1,0,2,2,0,0,4,4,0,0,0,3,2,5,4,2,2,6,1,0,3,0,3,0,0,2,1,4,3,1,6,4,1,2,0,1,3,0,0,1,1,2,3,1,3,0,2,3,1,2,0,1,3,1,4,1,3,3,1,3,1,0,4,1,5,3,2,3,0,3,0,0,3,2,3,2,1,2,1,2,1,1,7,2,2,4,1,0,2,0,0,1,3,1,3,2,1,1,0,1,4,0,5,2,3,0,3,0,1,2,2,6,3,3,2,3,1,0,2,1,5,1,2,0,0,4,3,4,3,1,0,7,1,0,1,0,2,1,1,2,1,1,2,0,1,3,1,1,0,4,3,7,3,1,3,0,1,1,1,1,0,0,6,0,3,1,4,1,1,1,0,1,0,2,1,3,5,2,3,2,0,1,10,3,3,2,1,2,0,3,1,3,1,0,0,3,0,1,1,0,6,1,5,0,1,2,1,2,1,1,3,3,0,3,1,1,2,0,3,3,1,2,4,0,0,2,1,3,3,1,3,1,1,0,3,1,0,0,6,0,1,1,4,1,0,0,5,0,2,0,1,1,4,0,3,1,3,2,1,7,4,3,3,1,4,1,1,4,0,4,0,3,2,2,2,3,3,3,0,4,8,0,0,1,3,1,1,1,5,2,0,3,1,2,1,4,2,1,2,0,4,2,2,0,0,0,3,1,2,0,3,3,3,3,2,1,2,5,1,4,3,1,4,2,0,3,4,1,2,1,1,0,5,1,0,4,1,1,2,0,1,2,0,0,5,2,1,2,1,2,0,1,0,5,3,0,2,3,0,2,2,0,1,4,1,0,0,0,3,0,3,1,2,1,0,1,1,5,2,2,2,6,1,3,2,1,0,1,0,6,2,3,5,2,2,1,1,1,0,4,1,4,1,1,4,0,0,2,2,1,1,2,3,1,3,2,0,4,3,9,9,3,0,2,2,4,5,0,7,1,2,5,3,3,1,1,2,1,5,4,1,0,3,5,5,2,2,1,2,1,2,1,4,0,2,4,1,3,0,0,3,2,2,3,5,0,2,2,2,3,4,2,0,0,2,0,7,5,0,1,5,3,2,1,4,3,2,3,1,0,1,3,5,0,2,1,3,4,10,2,0,6,3,3,3,3,4,4,5,0,11,2,1,1,0,1,0,0,2,2,2,1,4,6,2,2,0,6,5,4,2,2,3,2,4,6,0,3,8,1,2,1,2,1,0,2,2,0,1,2,3,4,5,3,2,1,0,1,1,0,0,4,3,4,1,3,0,6,2,3,0,2,3,0,2,0,0,0,1,3,0,0,1,1,1,2,6,2,0,1,1,1,0,2,1,0,3,1,0,2,3,1,4,6,2,0,6,0,2,2,0,5,0,0,1,5,0,3,2,4,0,5,3,0,1,0,4,3,2,1,0,0,3,3,3,3,4,1,4,2,1,3,4,3,3,2,1,0,4,1,4,5,2,1,2,2,1,3,3,2,0,2,2,3,7,2,1,1,1,3,5,0,0,1,2,2,0,1,1,0,0,4,1,3,3,1,2,0,3,1,1,1,4,0,3,1,1,4,2,0,2,0,2,1,3,0,2,2,2,2,1,1,0,1,2,3,1,9,1,1,0,3,9,1,1,4,5,1,0,4,0,0,0,2,4,2,3,1,1,2,7,2,0,2,2,0,5,0,2,1,2,0,6,1,0,0,1,0,1,0,0,2,3,2,2,2,0,5,1,0,1,1,3,3,1,1,4,1,1,2,2,1,1,2,4,5,2,3,0,0,2,3,0,3,1,0,2,6,1,1,2,0,1,0,1,0,0,3,0,4,4,2,3,3,0,2,4,4,3,3,1,0,2,3,4,0,1,3,0,2,2,3,1,2,1,1,2,2,2,1,5,0,2,3,2,2,2,4,2,0,0,1,1,1,4,1,4,1,1,5,4,0,6,4,1,1,1,2,8,0,0,3,0,2,5,0,0,2,0,3,2,1,2,6,3,1,1,6,2,0,2,5,1,0,1,0,1,0,5,2,0,0,3,5,2,3,1,3,0,1,1,1,2,3,1,1,3,1,3,3,1,0,1,9,3,0,2,3,1,0,3,2,1,1,0,0,2,2,1,5,3,5,0,2,0,1,0,0,5,4,3,0,3,3,1,4,1,4,0,3,0,4,1,1,0,0,6,2,0,0,2,2,4,2,1,1,1,1,1,5,1,0,5,5,1,2,1,1,2,4,0,0,2,1,2,4,5,0,3,1,2,0,5,3,2,5,1,0,5,2,0,0,2,4,3,0,4,0,3,0,1,2,0,1,0,0,4,5,1,1,1,1,4,2,1,0,2,4,3,2,2,2,2,1,3,2,3,0,2,1,2,0,3,1,1,3,1,2,1,0,0,0,1,0,2,0,0,0,0,2,2,7,2,3,6,1,1,3,0,2,5,1,1,3,4,4,2,1,2,4,2,5,0,0,0,0,7,2,2,2,1,3,2,2,0,4,0,2,4,0,3,6,1,3,1,2,3,3,1,1,0,3,0,2,5,2,4,2,2,1,2,2,5,3,3,4,2,3,1,0,1,1,0,6,1,3,5,2,0,1,0,0,2,0,5,0,7,2,6,0,1,0,6,1,1,3,1,2,2,2,1,3,0,2,4,3,1,1,0,3,2,5,2,4,0,0,1,1,2,2,0,1,0,2,1,2,0,0,4,3,1,1,2,1,3,2,5,2,0,2,1,0,1,4,0,4,3,2,2,1,0,2,1,0,3,2,5,2,2,0,1,0,6,9,2,1,3,1,4,3,3,0,0,1,1,2,5,3,2,1,2,2,3,0,4,7,2,0,1,1,2,2,2,0,0,2,2,6,1,0,1,2,0,4,0,3,1,1,1,1,2,0,3,3,1,3,0,3,1,1,3,10,7,2,0,7,2,4,2,3,1,2,3,1,1,5,5,0,1,1,1,0,0,1,2,4,4,5,4,1,2,11,4,1,2,2,1,5,11,1,3,3,1,2,0,0,0,3,3,1,3,1,6,1,5,7,2,1,3,1,2,3,6,0,0,2,0,1,3,3,0,4,3,1,5,0,1,2,2,0,1,2,1,2,3,4,1,2,6,3,0,6,3,2,0,0,0,1,4,0,0,4,5,5,4,1,2,2,1,0,0,1,0,4,0,1,1,1,0,3,7,3,2,1,0,5,1,2,6,2,0,2,2,2,2,1,1,2,2,2,1,1,2,2,4,0,3,0,3,4,5,1,2,2,4,5,1,3,0,2,9,1,4,3,3,1,2,0,1,0,3,0,5,2,0,0,4,1,1,1,1,2,2,1,3,0,2,0,5,1,4,4,4,5,2,3,1,1,0,3,2,3,3,0,0,0,4,3,5,3,3,4,4,4,1,4,7,1,1,1,2,1,5,0,2,3,2,0,0,6,4,1,3,2,1,1,1,1,4,2,0,2,4,2,0,5,5,0,3,1,3,1,2,0,0,0,1,2,1,4,1,1,2,1,2,2,4,2,4,2,2,3,4,0,4,1,3,0,3,2,1,3,0,3,3,1,3,3,5,2,0,3,1,1,0,1,2,6,2,1,2,1,5,3,6,0,2,1,0,2,1,4,2,3,0,0,0,1,2,1,1,2,0,1,1,1,3,0,2,0,3,5,1,4,5,1,1,2,1,1,1,0,2,2,0,3,4,6,4,2,3,4,0,5,1,2,3,3,2,6,2,1,3,2,0,3,2,3,0,1,2,3,10,5,0,4,2,3,0,0,3,1,2,3,1,5,1,5,4,2,0,1,4,4,3,1,3,1,0,1,0,1,2,3,2,0,3,1,0,4,2,1,0,0,0,9,3,0,2,5,2,2,3,1,3,4,0,0,4,0,4,0,1,0,2,0,4,3,3,0,0,0,0,1,1,0,3,1,2,0,4,6,1,1,0,4,1,1,0,0,1,0,1,1,1,1,3,2,3,1,4,0,3,1,3,2,2,3,0,0,0,0,4,0,1,0,0,2,2,1,4,2,1,2,4,3,1,2,1,2,1,1,6,0,4,3,1,2,0,1,2,5,0,2,0,1,1,2,2,3,2,6,0,3,7,3,4,4,6,1,2,2,5,3,8,0,0,3,3,0,1,3,2,1,2,1,0,3,4,0,0,2,3,2,1,0,2,5,5,1,4,1,0,4,1,2,0,0,2,1,0,0,0,1,0,3,0,1,0,5,0,2,0,1,1,1,0,0,0,1,0,0,2,5,5,0,0,3,0,3,4,1,1,1,4,4,4,4,2,2,6,0,0,4,0,2,3,0,0,0,2,0,2,7,2,2,2,2,3,2,0,2,3,0,0,1,0,2,0,2,7,0,1,0,2,2,3,1,3,3,7,1,1,2,0,5,6,3,2,0,2,1,1,1,4,3,2,0,2,1,0,5,0,0,6,1,0,2,3,3,4,1,1,2,1,1,1,3,0,2,0,0,5,6,4,0,3,0,2,0,0,1,0,0,4,4,4,2,1,2,2,2,1,1,3,2,0,4,0,3,0,1,1,1,0,2,4,1,1,3,0,0,0,2,2,2,1,2,0,4,2,0,4,5,0,0,3,0,1,2,6,2,1,4,3,3,1,1,2,2,2,2,2,3,2,0,2,1,0,5,1,0,4,4,1,0,3,1,0,5,2,4,0,4,3,3,1,3,3,4,3,1,2,1,1,2,2,0,1,7,1,1,3,0,2,4,1,2,7,6,3,0,0,2,1,1,1,1,0,4,0,2,4,1,3,0,5,1,3,4,4,0,0,2,1,2,1,1,3,3,0,2,3,1,0,2,4,0,2,1,2,1,3,4,2,4,2,1,5,4,2,2,2,1,1,0,0,0,1,1,1,1,6,4,0,0,4,0,0,2,0,2,3,2,1,0,11,1,5,2,3,0,2,2,1,3,1,1,3,1,2,2,0,1,3,3,3,1,1,8,2,1,6,0,2,3,3,0,5,2,6,1,2,5,0,1,6,1,4,0,4,0,4,1,2,0,4,0,4,1,2,1,2,3,2,1,0,2,0,1,4,2,5,1,1,2,1,0,3,1,1,0,2,0,2,3,0,1,3,2,1,1,4,1,2,0,5,1,0,1,1,2,3,1,3,3,0,2,2,0,2,2,0,2,4,1,3,4,0,0,4,4,2,1,5,3,1,0,0,4,1,1,0,5,5,2,4,1,1,4,2,2,4,2,0,3,2,4,2,0,5,1,2,3,3,1,2,4,7,2,2,2,0,5,2,2,9,0,8,3,0,4,1,1,3,0,1,2,3,2,0,2,2,0,0,4,1,2,1,3,1,0,2,1,0,4,1,3,2,1,2,2,0,1,1,5,1,2,2,0,2,0,1,4,2,0,4,4,2,2,1,2,3,0,2,1,4,0,2,3,4,1,2,1,0,5,2,0,1,1,1,4,2,0,3,5,0,2,2,0,3,2,2,2,3,3,1,0,0,0,0,4,2,4,1,1,1,0,2,3,2,2,3,1,5,3,1,1,2,3,0,2,1,2,2,2,2,2,1,4,3,2,1,2,2,0,1,7,0,0,6,1,2,1,4,1,4,2,1,1,2,2,1,7,0,2,3,1,2,3,1,1,4,4,6,7,2,5,1,0,4,1,1,1,6,1,3,4,4,7,1,0,3,0,1,0,0,1,0,0,1,6,1,0,1,5,2,3,8,0,0,1,4,3,2,1,3,5,0,1,4,1,1,0,0,2,2,4,2,1,0,1,3,1,5,2,0,1,0,3,0,0,4,0,4,1,1,4,5,3,4,0,2,5,2,4,0,9,4,1,2,0,2,1,5,0,0,4,1,0,1,0,5,6,5,0,4,1,2,3,0,1,4,1,3,1,5,2,2,0,0,1,7,0,1,3,0,1,4,1,2,0,3,0,4,1,1,0,4,0,3,2,0,4,0,1,2,2,1,1,1,0,0,0,1,2,0,1,1,0,4,0,3,1,6,0,1,3,4,0,0,1,4,1,5,2,0,0,2,1,0,3,2,0,2,3,0,1,7,4,3,2,2,3,0,4,1,4,0,2,5,2,2,0,1,3,1,2,1,4,1,0,0,4,1,3,1,2,0,4,2,1,5,3,0,3,1,1,0,1,1,1,0,0,0,1,0,2,0,2,3,2,3,3,2,1,7,3,1,3,3,1,5,0,0,2,2,10,2,0,2,0,4,7,4,0,1,0,3,1,0,2,3,6,0,4,3,2,0,1,2,3,3,2,0,0,4,0,2,2,0,1,3,0,4,0,0,6,0,3,3,3,0,8,2,5,0,1,1,0,3,2,6,0,5,4,6,1,0,4,3,4,2,1,2,3,1,3,0,0,3,1,1,1,1,1,1,6,2,3,3,0,6,1,5,0,1,3,2,4,2,0,2,0,1,4,0,0,0,0,1,3,2,1,3,3,4,0,0,5,3,0,3,2,1,1,1,0,1,0,1,1,0,0,1,2,2,0,2,3,5,3,0,2,2,1,2,0,5,0,2,3,2,2,1,2,2,0,1,1,0,0,2,0,2,0,2,2,0,1,3,2,1,3,4,7,1,0,3,1,1,4,2,0,0,2,2,6,1,0,0,5,2,2,2,0,1,0,0,2,1,2,1,1,4,1,2,0,1,5,3,1,6,2,1,1,1,2,5,3,7,4,1,2,2,3,6,6,4,2,3,5,1,0,2,2,1,2,1,0,0,2,3,5,1,3,0,6,1,2,1,5,1,2,5,2,0,0,2,4,0,5,0,2,3,3,4,4,3,0,2,0,2,2,1,2,1,0,0,3,5,5,3,1,1,1,1,5,2,0,1,1,0,2,3,5,2,4,2,1,0,0,2,1,5,7,1,2,0,3,3,2,2,1,4,2,0,2,2,2,1,2,3,2,2,0,2,4,1,3,6,1,0,2,1,0,0,0,7,3,5,4,0,2,0,3,2,3,1,1,3,1,3,1,2,1,2,1,0,1,2,2,0,4,4,3,3,0,1,2,1,3,1,3,7,1,0,1,1,1,1,2,0,0,4,0,1,0,0,1,2,4,6,4,0,3,3,0,4,0,1,2,1,5,0,3,5,2,3,3,2,0,2,4,1,1,3,2,2,2,1,0,1,4,0,1,2,0,1,3,5,5,2,2,0,0,0,2,1,2,1,1,1,0,4,2,0,4,2,0,0,5,3,3,3,3,2,0,0,3,2,1,0,2,3,1,0,0,1,4,5,1,1,1,0,9,0,6],
  "precision": 12,
  "version": 4
}"#).unwrap();
            let dense = match dense {
                HllInstance::Dense(d) => d,
                HllInstance::Sparse(_) => panic!("expected to read dense hll"),
            };
            assert_eq!(dense.index_bit_len, 12);
            assert_eq!(dense.cardinality(), 6241);

            // another one with non-zero baseline.
            let dense = HllInstance::read_snowflake(
                r#"{
  "dense":[4,7,7,8,5,4,5,7,5,5,4,4,7,3,3,5,5,7,6,4,5,8,5,7,5,6,6,4,7,5,6,10,9,7,3,8,5,3,8,6,4,7,3,6,9,8,4,7,6,7,5,3,5,8,7,6,7,5,8,5,6,4,5,4,5,8,8,5,5,4,5,7,4,8,4,7,9,7,6,6,8,6,7,9,8,5,4,8,4,3,7,5,7,5,5,4,7,6,6,4,5,4,7,7,6,10,10,9,6,7,8,4,6,5,9,8,4,4,6,4,6,8,5,5,5,12,6,6,11,8,6,6,8,5,11,8,4,9,4,5,4,6,8,8,5,6,8,5,9,7,3,5,5,6,7,5,5,6,5,3,7,4,6,9,8,6,5,6,8,10,10,8,4,7,5,8,4,5,6,6,8,4,4,5,6,6,5,5,7,9,5,8,5,9,7,6,6,5,7,4,5,14,6,4,7,9,7,5,4,4,6,4,4,5,3,8,5,5,3,9,8,6,5,10,5,6,6,5,6,8,6,12,10,4,10,5,4,5,7,7,5,7,6,5,4,9,7,6,8,5,8,6,6,9,7,9,5,6,6,2,6,4,7,6,7,7,6,4,5,5,4,6,4,7,8,7,5,5,5,6,8,5,4,9,5,5,6,8,4,4,4,5,6,8,6,6,8,5,8,7,6,5,6,4,5,5,11,4,8,6,8,5,4,6,6,7,6,5,6,6,4,5,8,8,6,5,4,9,4,6,9,4,6,4,5,5,4,10,4,7,5,5,7,5,10,6,5,4,4,7,10,6,6,5,8,10,5,4,5,7,4,4,6,4,5,5,11,5,3,6,4,4,6,8,4,7,4,6,4,6,6,5,5,7,7,4,5,7,10,10,9,7,7,5,3,4,10,6,5,7,7,4,6,5,5,4,17,10,8,7,6,4,5,5,6,4,8,6,4,6,8,4,7,7,5,5,8,3,6,5,7,7,10,5,6,6,7,5,3,4,6,10,9,7,8,7,5,10,4,5,4,7,3,5,6,6,5,6,5,6,10,5,6,5,3,5,10,5,6,7,5,5,5,6,5,5,8,5,7,10,6,4,4,5,6,5,5,8,8,3,6,7,5,10,11,7,11,5,5,4,6,9,5,7,6,9,4,4,7,5,7,6,8,4,4,8,4,7,4,5,4,6,8,6,6,4,4,8,5,8,8,6,9,5,6,6,4,6,5,5,4,7,5,4,3,6,5,9,6,5,5,7,6,4,11,9,7,9,8,5,9,6,7,5,6,6,6,5,15,5,3,5,6,3,5,5,6,7,4,9,6,6,9,12,4,9,5,4,6,4,9,6,6,5,6,3,9,5,4,8,4,4,5,6,6,4,8,4,7,4,3,7,6,6,9,8,4,5,5,4,4,6,5,6,5,10,8,3,7,6,4,9,8,4,9,9,4,8,6,4,7,8,4,6,4,7,3,8,6,8,8,10,4,6,5,7,7,10,5,5,4,9,5,7,5,8,7,5,6,5,4,4,9,6,6,7,4,6,6,6,4,7,5,5,6,4,4,4,6,6,9,3,4,5,4,5,6,5,4,7,4,3,4,6,4,6,4,5,5,6,8,8,8,5,3,5,6,9,6,7,5,5,6,5,7,6,7,6,6,10,6,4,7,5,5,5,5,10,7,6,7,7,7,5,7,4,7,6,6,5,6,8,5,7,5,3,5,4,9,5,4,6,4,4,6,5,6,5,4,4,9,4,7,6,7,3,7,10,4,6,4,5,8,5,3,5,7,9,9,7,6,6,6,4,6,3,8,5,5,5,9,7,7,5,3,6,8,6,6,4,9,4,7,8,6,4,5,4,5,5,6,7,5,6,4,6,5,4,7,5,8,6,8,5,6,4,7,9,6,6,7,5,5,5,7,6,7,6,4,5,8,8,5,7,5,6,3,5,10,6,6,5,6,8,10,6,5,5,5,6,8,4,6,5,6,11,5,6,5,8,11,5,6,6,8,8,7,7,5,4,4,5,6,6,5,8,4,4,6,7,7,7,6,6,6,4,5,5,7,4,5,6,6,5,5,7,7,6,7,7,5,9,4,5,5,5,5,5,5,8,5,5,5,5,4,3,7,7,4,8,4,6,10,6,3,8,6,8,6,9,6,4,5,5,6,4,5,6,10,6,7,10,4,6,5,6,6,8,9,8,6,8,6,5,5,3,7,7,4,6,7,8,7,4,5,7,5,5,8,6,4,7,5,7,5,5,3,7,5,5,7,4,4,4,9,5,4,7,4,7,10,5,6,4,7,3,3,11,4,4,7,5,7,5,10,6,13,6,8,7,8,4,5,5,6,6,6,7,7,5,6,5,6,5,6,5,6,7,8,4,4,7,6,9,6,7,4,7,5,8,10,3,5,6,9,4,4,7,9,4,6,4,4,4,7,11,4,4,6,3,5,3,5,3,6,10,7,3,5,3,5,6,7,7,4,6,3,7,5,7,5,7,7,5,7,8,5,6,3,3,4,4,10,8,5,9,6,7,3,6,5,8,5,5,6,4,3,7,8,5,4,3,7,6,7,8,5,7,6,7,7,4,4,6,7,4,5,6,10,4,5,9,6,6,8,3,12,5,8,6,3,12,5,9,8,8,7,7,4,6,5,7,10,4,7,5,7,3,8,7,6,6,5,6,10,6,4,8,3,5,7,6,6,4,6,6,4,6,4,7,7,3,6,4,12,7,5,10,4,4,9,3,6,5,5,8,7,5,5,9,7,4,6,9,4,7,3,5,6,6,9,5,3,5,5,3,7,4,5,6,6,6,7,6,5,4,3,5,6,6,4,7,7,6,9,5,11,5,9,6,9,5,5,7,7,6,7,6,5,4,8,9,5,5,6,7,6,4,9,9,7,5,8,7,7,10,8,5,6,8,4,4,5,4,5,6,4,9,9,9,4,5,8,7,6,6,7,3,6,8,6,7,4,6,4,4,5,4,9,8,5,6,5,8,4,4,4,4,4,4,7,4,6,8,6,5,3,6,7,5,4,4,7,7,5,5,6,7,7,10,4,6,5,5,8,10,9,7,7,9,4,8,4,5,4,5,7,4,5,4,7,10,4,8,8,6,10,9,4,6,5,10,8,5,4,5,5,4,11,7,4,6,3,3,4,9,8,5,6,2,5,6,5,4,5,8,5,5,7,7,6,7,7,7,3,7,8,7,13,4,8,6,14,7,7,6,5,7,3,9,5,4,4,10,6,6,7,9,8,6,8,4,6,8,8,10,6,6,5,5,6,6,6,9,8,6,6,5,10,5,6,5,7,6,8,5,5,4,5,5,5,7,5,4,7,4,8,4,11,7,4,6,5,7,6,7,5,5,7,7,3,5,6,5,5,4,6,4,8,6,5,6,6,5,6,6,4,7,7,6,5,4,6,11,9,5,13,6,6,9,6,5,6,5,7,5,5,7,6,6,6,4,6,6,6,6,6,7,4,7,4,4,7,4,4,6,7,5,5,7,7,6,4,4,8,4,8,5,5,8,3,7,7,6,7,3,4,6,7,5,5,4,3,7,7,6,6,3,6,4,8,5,6,7,4,5,4,7,7,7,6,3,3,5,6,9,10,6,6,4,9,4,4,5,8,6,8,6,6,5,5,7,5,7,4,5,6,5,7,8,7,5,6,4,5,7,7,5,5,8,6,4,9,3,5,4,4,8,6,7,6,6,7,7,8,8,4,8,7,6,6,10,6,4,6,3,4,7,10,4,10,7,6,9,10,5,8,5,7,5,7,6,4,7,8,6,6,10,4,5,5,3,4,6,5,6,2,5,6,4,7,4,7,4,4,6,6,3,6,5,5,6,5,6,6,4,6,7,7,7,6,7,10,7,7,5,5,8,8,5,4,5,5,8,7,5,6,5,3,5,6,4,6,5,6,6,6,5,4,7,6,4,6,5,5,5,8,7,8,6,4,5,6,7,7,7,5,4,5,4,4,6,13,6,4,6,6,6,6,5,4,4,4,7,6,5,5,3,7,5,5,5,6,7,4,4,3,6,12,10,5,7,5,10,5,9,4,4,6,9,8,9,4,5,8,6,6,4,7,5,10,7,3,10,6,4,8,7,7,5,4,8,3,9,3,5,9,6,4,6,6,7,5,6,5,5,5,6,4,5,6,5,8,5,6,6,4,9,7,4,5,4,5,6,5,5,4,7,8,5,9,5,5,5,10,6,8,5,4,6,8,4,5,6,6,7,7,12,6,6,5,6,6,7,5,8,6,6,4,4,8,5,3,9,6,4,5,4,5,6,6,6,6,5,8,11,6,8,4,9,5,5,7,6,5,7,4,4,5,7,5,6,4,5,6,6,4,5,4,4,4,5,7,5,6,7,4,5,5,4,8,11,4,3,8,8,6,4,8,7,10,8,6,7,5,4,7,5,5,6,4,5,4,9,6,5,4,6,7,8,9,6,4,6,9,5,4,4,5,4,7,4,5,5,3,5,7,7,7,6,5,7,8,8,9,6,3,8,7,4,7,6,5,8,5,5,5,5,5,6,5,8,5,5,5,6,10,7,8,6,6,6,6,5,5,6,3,6,5,7,4,7,7,4,7,7,7,5,6,5,6,8,4,10,9,4,5,6,7,4,5,5,4,6,7,5,6,9,4,5,6,6,4,4,3,6,7,5,5,9,11,7,5,7,9,7,5,7,7,4,4,8,7,6,5,6,6,8,6,7,6,4,7,4,7,6,6,7,6,4,11,3,5,6,7,6,5,7,6,4,7,9,5,5,5,5,4,10,6,6,6,7,7,6,8,6,6,6,5,6,7,10,6,7,5,5,7,6,7,5,5,5,4,5,4,6,10,5,5,6,5,5,5,6,5,8,6,5,4,4,4,6,6,8,8,6,5,8,5,10,7,4,7,7,6,6,4,8,5,4,5,5,8,7,5,6,7,6,10,7,5,5,5,4,4,5,7,6,4,11,9,5,2,6,4,6,11,6,7,6,4,9,8,4,7,5,6,6,5,4,7,4,5,7,8,2,6,7,5,5,6,6,5,3,6,4,7,4,4,7,8,6,5,3,5,8,5,6,6,5,6,6,5,6,5,9,6,4,5,6,4,4,7,6,7,7,6,4,5,8,8,7,4,4,7,6,7,4,9,8,5,4,9,4,7,5,4,5,8,4,4,5,5,8,7,5,7,4,5,6,4,5,3,8,4,5,6,5,4,6,6,3,8,4,5,6,5,7,11,5,4,8,7,5,4,5,7,4,9,4,6,5,3,6,5,6,5,9,4,8,6,9,9,7,7,7,3,6,5,5,4,7,5,7,5,6,5,6,5,7,5,5,9,7,6,5,4,5,8,3,4,7,4,6,7,5,5,6,7,7,6,5,6,6,7,4,5,6,4,4,6,5,4,4,5,7,5,6,5,3,9,5,6,4,5,6,4,7,5,6,4,9,5,5,8,5,6,6,6,7,6,6,4,6,4,8,7,6,6,5,5,6,5,6,6,4,8,3,5,6,4,10,7,6,3,4,8,8,9,4,7,7,7,4,3,4,10,7,4,4,6,5,4,6,4,6,6,7,7,5,5,5,6,5,6,4,6,4,5,5,6,7,6,4,9,8,6,4,7,4,7,6,5,7,4,3,4,8,6,5,4,7,6,4,6,5,4,6,3,5,6,4,4,6,6,6,4,5,6,9,5,5,6,4,8,8,9,6,4,4,6,4,6,5,3,6,6,7,7,3,10,5,6,10,4,4,5,5,6,5,4,10,8,9,11,7,4,8,8,8,7,4,9,7,6,8,7,8,9,4,5,5,10,9,5,6,5,6,4,5,4,8,6,9,8,3,3,5,6,5,9,5,7,4,6,5,4,5,5,6,6,4,5,5,7,6,8,8,4,7,4,5,4,5,4,9,7,8,4,6,4,4,4,9,6,5,7,4,6,5,6,7,8,4,5,4,4,5,8,3,5,10,3,7,4,3,6,8,5,5,7,6,6,4,4,4,5,8,6,6,6,6,7,5,7,6,5,4,5,6,6,9,4,3,5,7,5,5,7,7,3,7,5,8,5,6,6,3,5,7,8,6,12,5,4,5,6,7,5,7,5,3,8,5,6,7,5,5,5,5,5,4,5,7,4,6,4,10,9,4,12,5,3,7,5,5,7,7,5,3,6,6,6,5,5,4,5,7,3,5,6,4,4,4,5,9,6,5,5,6,5,5,6,5,4,5,3,5,7,9,5,5,10,4,6,4,8,7,5,9,6,5,8,4,4,6,6,4,9,6,7,4,8,11,8,8,4,4,4,5,8,7,4,7,6,5,8,9,6,5,6,4,7,4,6,7,3,5,7,7,4,7,3,6,4,8,7,8,7,10,6,4,6,9,8,4,5,6,5,5,6,7,4,5,5,10,3,8,6,5,4,5,6,7,9,6,7,6,6,4,4,7,6,6,4,6,5,5,8,3,5,6,5,8,8,4,4,3,3,9,3,5,7,4,6,6,6,2,8,4,6,5,7,9,5,4,6,6,5,6,11,4,6,5,5,4,7,5,4,5,4,2,6,4,5,5,4,5,5,3,5,5,6,4,5,8,4,5,6,8,7,4,4,6,5,6,4,5,3,5,7,8,10,8,6,6,7,5,7,5,5,4,4,4,5,6,4,6,6,10,5,4,7,6,6,4,6,6,5,7,3,6,7,5,5,5,4,7,5,4,6,5,4,8,11,5,7,6,5,7,7,6,5,11,5,4,5,4,5,4,5,7,10,8,7,3,6,5,6,3,7,5,6,7,5,6,8,7,7,5,5,8,5,3,5,5,6,9,3,4,5,4,4,11,6,4,5,6,9,7,6,6,11,5,6,7,4,5,5,8,6,4,7,5,6,5,5,7,5,6,5,8,9,8,11,4,6,6,5,8,5,11,6,8,4,5,7,7,5,7,4,5,9,7,5,9,6,8,5,6,6,7,6,9,4,6,5,4,6,6,5,7,5,5,9,10,9,8,4,5,4,7,6,7,4,5,4,4,7,5,4,4,3,8,9,5,6,6,6,5,5,7,4,5,5,6,5,4,5,8,5,6,7,5,5,7,4,5,5,4,7,6,6,6,6,5,6,7,5,5,5,4,3,4,5,6,4,3,10,6,5,3,4,8,11,5,4,5,8,8,6,3,5,7,5,6,7,5,4,9,9,7,5,4,5,6,4,5,4,5,6,5,7,6,7,6,5,2,6,5,5,8,6,4,4,6,7,4,6,6,4,5,6,13,9,7,8,6,5,8,6,6,7,6,5,5,7,4,4,6,4,4,4,9,9,7,4,4,6,5,6,9,7,7,7,5,6,6,6,5,5,6,3,5,6,5,5,4,5,6,5,9,8,6,6,6,4,7,6,4,5,4,4,6,4,5,5,5,7,8,4,10,5,6,4,3,5,6,8,4,5,7,9,5,6,7,3,3,5,4,5,7,6,3,10,6,6,12,4,6,7,4,7,11,4,5,4,4,7,4,11,6,5,5,4,4,6,6,5,7,4,4,3,7,6,6,3,5,3,7,6,10,4,5,8,7,6,5,7,6,9,4,5,5,4,4,7,6,9,8,4,7,5,5,6,4,4,5,5,6,7,6,6,7,3,6,5,5,6,7,4,5,7,5,7,5,4,7,8,7,3,7,6,5,4,7,12,3,4,6,8,6,5,6,5,10,8,11,6,5,11,8,4,7,8,6,7,6,5,8,5,4,9,10,11,5,7,4,4,5,5,5,5,7,11,3,6,4,6,4,8,8,5,3,6,7,5,4,6,3,5,8,6,6,7,9,5,6,5,7,13,6,5,5,5,4,6,8,6,9,6,6,5,4,5,7,8,5,5,8,5,8,4,7,5,6,5,6,4,3,7,7,6,4,6,7,6,5,3,6,9,7,7,7,4,5,4,3,9,5,4,5,4,5,9,8,5,6,4,4,5,6,11,5,4,5,4,8,11,3,7,5,6,6,4,4,6,7,5,3,5,5,5,4,3,5,8,7,2,6,6,5,5,5,4,7,7,5,5,5,7,6,8,6,4,7,8,4,5,5,6,5,7,6,5,5,6,6,5,4,2,8,4,3,4,6,5,5,12,7,5,6,3,5,11,3,6,6,4,5,10,6,5,4,8,4,4,8,4,4,3,6,5,5,7,4,5,6,4,4,5,6,4,4,2,10,4,4,4,6,10,7,4,4,10,4,7,7,7,7,6,6,4,7,6,5,6,4,5,5,5,6,8,5,6,5,4,3,5,7,8,4,6,5,11,7,5,5,6,8,8,8,4,6,9,5,8,4,7,8,6,4,6,4,6,6,4,6,4,8,5,6,4,4,8,9,8,8,5,7,7,6,6,3,4,12,3,5,5,6,7,5,5,3,6,4,4,5,6,4,6,6,9,4,5,5,5,7,4,5,4,6,4,8,4,4,7,5,5,8,8,3,9,4,6,9,5,6,7,5,6,6,10,5,4,8,5,7,5,6,8,5,3,4,8,7,5,4,5,7,5,5,3,6,4,5,9,5,5,6,5,5,7,8,7,8,6,5,6,7,7,5,7,11,6,6,7,5,8,4,8,7,7,6,4,7,4,4,5,5,11,6,13,4,6,4,6,7,4,5,5,3,4,5,6,8,5,5,6,4,7,5,4,6,6,4,10,4,9,9,6,3,7,4,7,9,10,7,8,6,8,6,6,6,4,5,6,8,4,8,4,5,6,4,6,6,6,5,4,5,5,7,5,5,4,4,7,4,7,5,7,6,9,4,7,6,4,7,7,5,7,7,4,7,11,6,5,9,9,5,7,4,7,6,8,5,4,7,8,8,4,5,10,12,5,4,6,7,4,4,4,10,5,7,4,5,5,4,4,7,7,15,5,8,6,7,7,9,6,10,7,5,4,6,8,6,8,4,6,7,6,6,7,4,5,7,7,11,5,4,6,6,5,14,5,4,4,5,9,8,5,7,5,8,11,4,5,14,12,6,9,6,7,5,6,7,6,5,4,6,6,4,7,7,5,5,6,4,8,5,8,5,12,6,5,6,4,5,5,8,6,5,4,11,6,6,4,4,7,6,7,7,9,5,6,6,8,7,8,5,8,5,7,6,5,10,4,8,7,4,8,9,6,6,3,6,3,3,5,5,6,4,6,9,8,6,5,8,4,4,6,4,8,6,6,6,5,5,9,5,4,9,8,5,7,7,9,3,3,2,8,2,5,6,4,4,6,4,8,8,5,5,5,9,6,6],
  "precision": 12,
  "version": 4
}"#).unwrap();
            let dense = match dense {
                HllInstance::Dense(d) => d,
                HllInstance::Sparse(_) => panic!("expected to read dense hll"),
            };
            assert_eq!(dense.baseline, 2);
            assert_eq!(dense.index_bit_len, 12);
            assert_eq!(dense.cardinality(), 99157);

            // Invalid inputs.
            //   - not json.
            HllInstance::read_snowflake("invalid").unwrap_err();
            //   - wrong number of buckets.
            HllInstance::read_snowflake(r#"{ "precision": 1, "dense": [0, 0], "version": 4 }"#)
                .unwrap();
            HllInstance::read_snowflake(r#"{ "precision": 1, "dense": [], "version": 4 }"#)
                .unwrap_err();
            HllInstance::read_snowflake(r#"{ "precision": 1, "dense": [0, 0, 0], "version": 4 }"#)
                .unwrap_err();
            //   - invalid precision.
            HllInstance::read_snowflake(r#"{ "precision": 0, "dense": [], "version": 4 }"#)
                .unwrap_err();
            HllInstance::read_snowflake(r#"{ "precision": -1, "dense": [], "version": 4 }"#)
                .unwrap_err();
            HllInstance::read_snowflake(r#"{ "precision": 1024, "dense": [], "version": 4 }"#)
                .unwrap_err();
            HllInstance::read_snowflake(r#"{ "dense": [], "version": 4 }"#).unwrap_err();
            //   - unknown version.
            HllInstance::read_snowflake(r#"{ "precision": 1, "dense": [0, 0], "version": 3 }"#)
                .unwrap_err();
            HllInstance::read_snowflake(r#"{ "precision": 1, "dense": [0, 0], "version": 5 }"#)
                .unwrap_err();
            HllInstance::read_snowflake(r#"{ "precision": 1, "dense": [0, 0] }"#).unwrap_err();
            //   - value in the bucket is too large.
            HllInstance::read_snowflake(r#"{ "precision": 1, "dense": [0, 256], "version": 4 }"#)
                .unwrap_err();
            //   - either both 'sparse' and 'dense' were specified or none.
            HllInstance::read_snowflake(
                r#"{ "precision": 12, "sparse": {"indices":[], "maxLzCounts":[]}, "version": 4 }"#,
            )
            .unwrap();
            HllInstance::read_snowflake(r#"{ "precision": 1, "dense": [0, 0], "sparse": {"indices":[], "maxLzCounts":[]}, "version": 4 }"#).unwrap_err();
            HllInstance::read_snowflake(r#"{ "precision": 1, "version": 4 }"#).unwrap_err();
        }

        #[test]
        fn test_hll_storage_spec() {
            let read = |s: &str| HllInstance::read_hll_storage_spec(&hex::decode(s).unwrap());

            // Empty encoding.
            let h = read("118b7f").unwrap();
            assert_eq!(h.index_bit_len(), 11);
            assert_eq!(h.cardinality(), 0);

            // Explicit encoding, 1 value.
            let h = read("128b7fee22c470691a8134").unwrap();
            assert_eq!(h.cardinality(), 1);
            let sparse = match h {
                HllInstance::Sparse(s) => s,
                HllInstance::Dense(_) => panic!("expected to read sparse hll"),
            };
            let mut lz_counts = vec![];
            sparse.each_bucket(|_, value| {
                lz_counts.push(value);
            });
            assert_eq!(lz_counts, vec![5]);

            // Sparse encoding, 169 values.
            // TODO: the estimate is off, fix calculation in sparse mode.
            let h = read("138b7f04a10642078507c308e309230a420ac10c2510a2114511611363138116811848188218a119411a821ae11f0122e223a125a126632685276327a328e2296129e52b812fe23081320132c133e335a53641368236a23721374237e1382138e13a813c243e6140e341854304434148a24a034f8150c1520152e254e155a1564157e158e35ac25b265b615c615fc1620166a368226a416a626c016c816d677163728275817a637a817ac37b617c247c427d677f6180e18101826382e1846184e18541858287e1880189218a418b818bc38e018ea290a19244938295e4988198c299e29b239b419c419ce49da1a1e1a321a381a4c1aa61acc2ae01b0a1b101b142b161b443b801bd02bd61bf61c263c4a3c501c7a1caa1cb03cd03cf03cf42d123d4c3d662d744d901dd01df81e001e0a2e641e7e3edc1f0a2f1c1f203f484f5c4f763fc84fdc1fe02fea1").unwrap();
            assert_eq!(h.cardinality(), 164);
            let sparse = match h {
                HllInstance::Sparse(s) => s,
                HllInstance::Dense(_) => panic!("expected to read sparse hll"),
            };
            let mut lz_counts = vec![];
            sparse.each_bucket(|_, value| {
                lz_counts.push(value);
            });

            assert_eq!(
                lz_counts,
                vec![
                    1, 2, 5, 3, 3, 3, 2, 1, 5, 2, 5, 1, 3, 1, 1, 8, 2, 1, 1, 2, 1, 1, 2, 1, 1, 3,
                    5, 3, 3, 2, 1, 5, 1, 2, 1, 1, 1, 3, 5, 1, 2, 2, 1, 2, 1, 1, 1, 1, 4, 1, 3, 5,
                    4, 1, 2, 3, 1, 1, 1, 2, 1, 1, 1, 1, 3, 2, 6, 1, 1, 1, 1, 3, 2, 1, 2, 1, 1, 7,
                    3, 2, 1, 3, 1, 3, 1, 4, 2, 7, 1, 1, 1, 3, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 3, 1,
                    2, 1, 4, 2, 4, 1, 2, 2, 3, 1, 1, 4, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 2, 1, 3, 1,
                    2, 1, 1, 3, 3, 1, 1, 1, 3, 3, 3, 2, 3, 3, 2, 4, 1, 1, 1, 1, 2, 1, 3, 1, 2, 1,
                    3, 4, 4, 3, 4, 1, 2, 1
                ]
            );

            // Full (dense) encoding, 10k values.
            let h = read("148b7f21083288a4320a12086719c65108c1088422884511063388232904418c8520484184862886528c65198832106328c83114e6214831108518d03208851948511884188441908119083388661842818c43190c320ce4210a50948221083084a421c8328c632104221c4120d01284e20902318ca5214641942319101294641906228483184e128c43188e308882204a538c8328903288642102220c64094631086330c832106320c46118443886329062118a230c63108a320c23204a11852419c6528c85210a318c6308c41088842086308ce7110a418864190650884210ca631064108642a1022186518c8509862109020a0a4318671144150842400e5090631a0811848320c821888120c81114a220880290622906310d0220c83090a118c433106128c221902210cc23106029044114841104409862190c43188111063104c310c6728c8618c62290441102310c23214440882438ca2110a32908548c432110329462188a43946328842114640944320884190c928c442084228863318a2190a318c6618ca3114651886618c44190c5108e2110612144319062284641908428882314862106419883310421988619ca420cc511442104633888218c4428465288651910730c81118821088218c6418c45108452106519ce410d841904218863308622086211483198c710c83104a328c620906218864118623086418c8711423094632186420c4620c41104620a441108e40882628c6311c212046428c8319021104672888428ca320c431984418c4209043084451886510c641108310c4c20c66188472146310ca71084820c621946218c8228822190e2410861904411c27288621144328c6440c6311063190813086228ca710c2218c4718865188c2114850888608864404a3194e22882310ce53088619ca31904519503188e1118c4214cb2948110c6119c2818c843108520c43188c5204821186528c871908311086214c630c4218c8418cc3298a31888210c63110a121042198622886531082098c419c4210c6210c8338c25294610944518c442104610884104424206310c8311462288873102308c2440c451082228824310440982220c4240c622084310c642850118c641148430d0128c8228c2120c221884428863208c21a0a4190a4404c21186548865204633906308ca32086211c8319ce22146520c6120803318a518c840084519461208c21908538cc428c2110844384e40906320c44014a3204e62042408c8328c632146318c812004310c41318e3208a5308a511827104a4188c51048421446090a7088631102231484104473084318c41210860906919083190652906129c4628c45310652848221443114420084500865184a618c81198c32906418c63190e320c231882728484184671888309465188a320c83208632144318c6331c642988108c61218812144328d022844021022184a31908328c6218c2328c4528cc541428190641046418c84108443146230c6419483214232184411863290a210824318c220868194631106618c43188821048230c4128c6310c0330462094241106330c42188c321043118863046438823110a041464108e3190e4209a11902439c43188631104321008090441106218c6419064294a229463594622244320cc71184510902924421908218c62308641044328ca328882111012884120ca52882428c62184442086718c4221c8211082208a321023115270086218c4218c6528ce400482310a520c43104a520c44210811884118c4310864198263942331822").unwrap();
            assert_eq!(h.cardinality(), 9722);
        }

        #[test]
        fn test_hll_postgr_test() {
            let t1 = "Eot/HUOjtjMdhpk=";
            let t2 = "FIt/OVKSkKYhSlQQhClKcpXEIRBEEKZJCHKlRjGKQhSlMwx0HQVRUFIdJkEOc5iEKY6TnIZJCFMRBUESgyimIhSDIKUxzGIgyCmWYpikIYxjHKVRknOVJzGMUyzoKUqTmKcpSIWQpTGMQ5CmQcxkEIgqiISoxzmWgpylQUxirQUhzGQkxDGQhJkjOhRyHKZBDlQRRjMSUhCmIVBTFKQqDGOcxUFOQpTlKkxSnMgqkJOU5iIKYxSGOY5TIQVRTGIMpiHOkozFKVRSlKgiSFIUpjlIchilQcpylGYZkIMghzHOgpzGQkpylIYhklMYpSFMYxTFKcxzHKkhyGOkpTmSQpjLcYpkFKhaCFMY6BnKYpknKdBUEOUxilMdBioMVCCIOYpzmSkxkGIgxSmQYpyqOcxjIKUpEHMc5VGMopzmKgxjJKYpiEGghjGKZRUFQQyTEMc5zGQcx0GGYhCqKQ6UnKc5inOUhUFIcpzlMUyEHOQxkJWUxkJQlCTlYRCymKchVJOdBzFQY5TlMUhiHMVJSlKUpkGIopSnUUhjFQcpjKQQpjoMYxDKKkxDpSYqCHUZJToQcijrIchTjI0x0GMciCKUghUJKkxTIQhJjGOYylHMwxzlIlSCnOkqCJKcpllOQiVHKZAzpOYhkHMU6zFGYpylKhRznKQxCGIQ5DFMYxmkKQxTFKYxznQUxCJMZRiLKUpzHQUyCoKVCTKKYpSkKg6UHIlBSmQYxTlQYqDIIdJimKY5zIKYpiGMcxylOkqCFQVJylOcxSmKkpjIIZRWEKUpTGIY5xsMdB0HKk6BjGYxjFKQxjmQZBSlMYhEpMlRSHKU5TmKQhjIKYxyEMU6DmMcpkFOgxEEKQpToScxxkMVBCIKVCVGOY5UqQYxikKUxCKSY5CnWQ6TlIg6TFKURTGKZCUJSlRRqKZJ2EOgiSESMpDmQQpyISRCDmOopilIQpSIeUo0lMYiSGKcpynQUhDkKRBDoOVpTIIU5SGKUhjIKcxTHIYxGEQY5UkIUhjHQUxDoGUiTlOUhzmGg6imOY5ilKUpCFQZBjGKYhjEMUhRqMUyCnMQxTIQkyTGMkwzHQg5RmKQiTJKQ50FORpypMVBxlKRBTLMcpEGMZBjlMg5TpMUqkpQkxjHScqjlUgqSmOVJlHIYxhmIQyEGWkxyEOMpTEKY5SoQZhjHQUpzGWVChlKghymOZhDmQYpEnQcpDnOUpTGGY5jlMc5TkKUxzFOcxSmIkpToQU5DoMgyhmIhRimIYhykKU5jmQk6FHcUpjJIc5TFIU5UoIUwzGKUhjJScpilMhJDFKNBjHKkpUHMYhkHQNBUEQpSDHOghhkKYpRmKYpzGOYxjFIYpjleY53FGU5SmKYpiJMRDjlMpBiHKgyhoMU5iHUUqEKOg5CJOUg0GOcpiESghDkac6UESUyTHSUy0FOgqDGUU5TGMUyTHKUpkGMdBzpGlBCFMU5TmMY5UGQYyEmKQxCnMRZyGKYqDJKZBilOUx0HIZBTFKMxiJSg5TEYQxUJOkxDlOYZ0EKRCTHOUxknQQh1FKQpSIOdBUnMVQzHKQ5imWsxhlQpBSoKZCDMIk5zQMUpilOMxUGIUxiFMU6THOUplEIYxyFORR1mKZJTFKUoyrIYhTFKY6DmOkiEFKZJEKMYhitIgyyGKUxjkKgxTmMZBCjKQpDmScpjkQchyFQYhinQhIzKMQ=";
            let t3 = "FIt/OVKSkKYhSlQQhClKcpXEIRBEEKZJCHKlRjGKQhSlMwx0HQVRUFIdJkEOc5iEKY6TnIZJCFMRBUESgyimIhSDIKUxzGIgyCmWYpikIYxjHKVRknOVJzGMUyzoKUqTmKcpSIWQpTGMQ5CmQcxkEIgqiISoxzmWgpylQUxirQUhzGQkxDGQhJkjOhRyHKZBDlQRRjMSUhCmIVBTFKQqDGOcxUFOQpTlKkxSnMgqkJOU5iIKYxSGOY5TIQVRTGIMpiHOkozFKVRSlKgiSFIUpjlIchilQcpylGYZkIMghzHOgpzGQkpylIYhklMYpSFMYxTFKcxzHKkhyGOkpTmSQpjLcYpkFKhaCFMY6BnKYpknKdBUEOUxilMdBioMVCCIOYpzmSkxkGIgxSmQYpyqOcxjIKUpEHMc5VGMopzmKgxjJKYpiEGghjGKZRUFQQyTEMc5zGQcx0GGYhCqKQ6UnKc5inOUhUFIcpzlMUyEHOQxkJWUxkJQlCTlYRCymKchVJOdBzFQY5TlMUhiHMVJSlKUpkGIopSnUUhjFQcpjKQQpjoMYxDKKkxDpSYqCHUZJToQcijrIchTjI0x0GMciCKUghUJKkxTIQhJjGOYylHMwxzlIlSCnOkqCJKcpllOQiVHKZAzpOYhkHMU6zFGYpylKhRznKQxCGIQ5DFMYxmkKQxTFKYxznQUxCJMZRiLKUpzHQUyCoKVCTKKYpSkKg6UHIlBSmQYxTlQYqDIIdJimKY5zIKYpiGMcxylOkqCFQVJylOcxSmKkpjIIZRWEKUpTGIY5xsMdB0HKk6BjGYxjFKQxjmQZBSlMYhEpMlRSHKU5TmKQhjIKYxyEMU6DmMcpkFOgxEEKQpToScxxkMVBCIKVCVGOY5UqQYxikKUxCKSY5CnWQ6TlIg6TFKURTGKZCUJSlRRqKZJ2EOgiSESMpDmQQpyISRCDmOopilIQpSIeUo0lMYiSGKcpynQUhDkKRBDoOVpTIIU5SGKUhjIKcxTHIYxGEQY5UkIUhjHQUxDoGUiTlOUhzmGg6imOY5ilKUpCFQZBjGKYhjEMUhRqMUyCnMQxTIQkyTGMkwzHQg5RmKQiTJKQ50FORpypMVBxlKRBTLMcpEGMZBjlMg5TpMUqkpQkxjHScqjlUgqSmOVJlHIYxhmIQyEGWkxyEOMpTEKY5SoQZhjHQUpzGWVChlKghymOZhDmQYpEnQcpDnOUpTGGY5jlMc5TkKUxzFOcxSmIkpToQU5DoMgyhmIhRimIYhykKU5jmQk6FHcUpjJIc5TFIU5UoIUwzGKUhjJScpilMhJDFKNBjHKkpUHMYhkHQNBUEQpSDHOghhkKYpRmKYpzGOYxjFIYpjleY53FGU5SmKYpiJMRDjlMpBiHKgyhoMU5iHUUqEKOg5CJOUhUGOcpiESghDkac6UESUyTHSUy0FOgqDGUU5TGMUyTHKUpkGMdBzpGlBCFMU5TmMY5UGQYyEmKQxCnMRZyGKYqDJKZBilOUx0HIZBTFKMxiJSg5TEYQxUJOkxDlOYZ0EKRCTHOUxknQQh1FKQpSIOdBUnMVQzHKQ5imWsxhlQpBSoKZCDMIk5zQMUpilOMxUGIUxiFMU6THOUplEIYxyFORR1mKZJTFKUoyrIYhTFKY6DmOkiEFKZJEKMYhitIgyyGKUxjkKgxTmMZBCjKQpDmScpjkQchyFQYhinQhIzKMQ=";
            let s1 = base64::decode(t1).unwrap();
            let s2 = base64::decode(t2).unwrap();
            let s3 = base64::decode(t3).unwrap();

            let h1 = HllInstance::read_hll_storage_spec(&s1).unwrap();
            let mut h2 = HllInstance::read_hll_storage_spec(&s2).unwrap();
            let h3 = HllInstance::read_hll_storage_spec(&s3).unwrap();
            h2.merge_with(&h1);
            assert_eq!(h2.cardinality(), h3.cardinality());

            let data = vec![
                "FIt/GEZDGAQSBBCYxBkCIgRkAIJwjYMoQjOQJgDEEgBFIIQyjKMIhiGEJSACQgQhEcRAiGMARgGEwhDOMABhCIIhACUQjDEQgCDKIRDDCQYRECAYjDAIJhGIcIhCKBAzCGRBhhEEYhDEMghgIYYRiGEIzFMIIhCGAhxBGQYiGCQZTkCIwQhIIAxDGUJCDIMAhDGI4QjGQIkDCJBCCEIYhBGUZAiKIBhEQMQxhIQYwCIEIiAEMhBCGMAQkGMJTHCEIAiEMZBkCEoglMEZRgEYQyDOQgxhAMIhnGEQlCIEZADEEQgiIgYhEIAgwiCIQRCEIAhiEEggiCMQRCIIojBIEQhlEIhBGEMYkjEEIAlCMZRjGIYkjCIAzDGMIwnGAYiBGEoinCYoBBIIIxhKEQxCGEIRhGQIRlAkYihIAQxIIIQwhIMAhiEUhRCAQYRBEIAhCIcZggEMZCBSQYhkIEoAiEYZBiEEQSDMIQxDGQgxCEIoCCAkJhkIMQCCEIQRCSMghiCUICgAEYgkMYAgiGYYQFAERCBEMBglCMxBBIMIAiKEIykGMoRCIMRTFCUQxkOMRQhGUg0hIMAxDAUghjIMQSFCE5AjEIYiCEUZBCGMQxACYIAjGMIhlKYYxCIQYBiIEgTCEMJAhGEoBjAUYSkEIRRCOMIBgAMZACEIZRiCIhRkAIJACEMZAiEIYyjCERABCUAwBAMARhEQKBCEMZiiGEQhBCQ4hDIQQQkCIQzCMcTBlEMRhCCIZDFEARQhKIgjjAIRRkEIYyCEIwwDAQgiCKMZQFCIYiECIgwECUIRFOMYRgEMQSAEMQyBCQgQhAAQzgEIQSAGEQQACMYSAAIIwmIIIhiCMYAiEAYhAIcYSiCMYhiCEIQCKIJAiGMwRjCQgxCIEYQDEMoShKEYRiEAYgiGYgQgCMRhiAMQCBAUoTiKIZyAGMZRBEIZSAOcghGKcYwgCIohCCMowCOQYjCCYQgkGMRADGIghBCQQwlIEYhDIEAxBEQYRHKcgRAEYQQDCMRBhKEYTECQIgCGEQwEKEgxkMEYiiGQIxDEMYRGAFJhDQIYgjGIxBjEUgiDKIJhjEEQwiGIhSEEMICHEIpRCEI4xBEUIxjGQIxBCIQlrEQQigAEQQjMEgACCIQhCIIIjCGMoxkEIQBhIQCBCYIggiEIIhDIMoiCEI4wlGUhBBGYQwjCIIiEKQQQhCEABBEEZABKIoAjGcIRgCIoxjEIYyECQoxjEEYhnKMJRlIgYyjEEYhkMAIQDCIIRCEUZBCGIAyCEQAxCGEARiKAIgkEUIRjEEQhkIUIQjKEYwiAJARACIghDCIgAnIBATCKUgAjCYghFKQYBGEUZSiGIZRDEMgSBCMIRDEEIglGRBCiCEQhiGUIxCKIhhBCUJBDIIgiEEcBQmGMZBiKIQjBIUhyCAMYTAEEJhEKcAhlGMIyBEMgQiEIJRCGUYRBCEIhDMMpBAIEYwjEgIRnEEogDIQKyhOIxhhCAYhhEIYRKIIAiBGIAxEOMQgiGEoikEEIzBIIYQmGUghCEYg0HGIRDjGYYihAEYSFEMoxjEUIyDCQAyiAMQBhGIA0AOEwiCCIRiBGEohCEkpSFEYJRjCEYSCGUwikEMgijCEYhEIIJxhCMQhAGEQSiCEQyiEQBBjGUYxFKYIxCCIYhCIUISFCIJBBCMRSjEMYglEIAhCCMohBCMQgBEA=",
                "Eot/N/V0+Dra68JKY2b5infzYg==",
                "Eot/nfbBQcQmvryrcT/HeEIr+TjmHhUfcAhxS8JqwhqTkVc=",
                "Eot/i+Maa9q3Pl2rcT/HeEIr+dia8C0nGCAp32rJA0BsgmjqDgl0H80IFwAgw9nW8KyqAfMaTuSaSd4DpLQ1/wbePwn7TSReKmlKDY9/upc1F7EXJma/UlPIlB1Do7YzHYaZKoZ81rErd7M8oiKE48iUBUvCasIak5FXTVvP7qD1bL1ZEdvj/XdSn1kcl825sFyfW59PXFg63Xs=",
                "Eot/q3E/x3hCK/n1ez0SRvHbdQOktDX/Bt4/DY9/upc1F7EdQ6O2Mx2GmT7tBKe0OOhnS8JqwhqTkVdTolBQZ3OBiA==",
                "Eot/meyhcPXwINercT/HeEIr+cfRuk85mN3Xyr17q/7Vsbb1ez0SRvHbdQOktDX/Bt4/DY9/upc1F7EdQ6O2Mx2GmT7tBKe0OOhnS8JqwhqTkVdTolBQZ3OBiF6O7ZUxlIvn",
                "Eot/i5sL9kkhF32Va4iiJAu05pnsoXD18CDXq3E/x3hCK/nEt+0x8tYDBMdkCa7rbU/Ox9G6TzmY3dfKvXur/tWxtuoOCXQfzQgX7j2z4n57XQfyucQN3nza+fV7PRJG8dt1AfMaTuSaSd4OZTh4gGsmOCiJ+YZ9f69hNEwUhJajGDw2Xmst196uez0a3g/OhU5dPu0Ep7Q46GdEeUrZY43XEUvCasIak5FXXo7tlTGUi+c=",
                "Eot/hcAJsOLh1jSZ7KFw9fAg16OYIybf6b2MxLftMfLWAwTHZAmu621PzsfRuk85mN3X6g4JdB/NCBfuPbPifntdB/IPx3OOduIe8rnEDd582vn1ez0SRvHbdfgFvOX+82nw+2Ki4IUoK6oB8xpO5JpJ3g5lOHiAayY4KIn5hn1/r2EqhnzWsSt3sy7lDvgGMuoEM19zGxVkuYw0TBSElqMYPDZeay3X3q57OLzfa+2Zad89Gt4PzoVOXT1BnKktRP5EPu0Ep7Q46GdEeUrZY43XEUvCasIak5FXU3j20JANH7BUFmOsisFcfFVhinXSpw6bXO+cZIKmHJpeju2VMZSL522ThR9aHW6Eb9Cg/IT8/tFwz2PdVyWupHf8N0m+YCRbeuiUcbw8O0t9TgF40Z3XDg==",
                "Eot/gg7mggN4JvaDxlqkBg+CDoXACbDi4dY0iwYSXz9MKYSUdmltfSU3Xpd13r+XY6C3meyhcPXwINea1FV5stU0KJv/47gurOdVnZmQ4JyQm0Cd9sFBxCa+vKBDUeVuqAwSo5gjJt/pvYynjvL1pOitwakkjZXrSIDbqWs95MMDzp6zprnmbILIE7ubKwq6WpKfvl5t80wECODECzf0ibItvMY+EJQhvCWnxuXBb4e+cjvHZAmu621PzsfRuk85mN3XzLusZayMBr/PqVRNfB+ENtD+qkP6lgAF0dL/kXOoqbzVNo8Cg3obbNcKTUNinx6R3UCPX8yQ3vXipJUbWvvD3uO798hd9HqT5xcAyvL5WTLqDgl0H80IF+pBr1JzZ6Pb6kjckO2VFz/qlW0V93VFw+vfq2Vu4ucT7j2z4n57XQfvNTLvQSN18vKCmDCAnTVM8rnEDd582vn0p91ibFklHPV7PRJG8dt1+AW85f7zafD45fsBYd46R/qe1CuYRtPv+2Ki4IUoK6r+y+UTgEgSigAEn0AlBX8gADwoS0dVe5QBEunwu1csSgHzGk7kmkneDmU4eIBrJjgS8+iVuPJoWSDrfYnvbt/pIijirl0GdysoifmGfX+vYSmTVC1hhv/VLbx+Sb2mBlQu5Q74BjLqBDNfcxsVZLmMNybER4tfZjc4vN9r7Zlp3z0a3g/OhU5dPUGcqS1E/kQ+7QSntDjoZ0R5StljjdcRRez4F3QIbSpM+NIfUQU9eE0SqSr4hdYvUycSIGnILVNUFmOsisFcfFVhinXSpw6bXo7tlTGUi+dirfQ40siHFWc3ib9uql07bZOFH1odboRumQtO3quRfW7dJCBNzMhnb0YYRivavrJv0KD8hPz+0XDPY91XJa6kdqq47AJdmQ96Cq5pau2km3rolHG8PDtLfSjmgFilk40=",
                "Eot/gcrQwjF27KiEtR2aiLKqoYchpZ+alKTXh5DHMgPXHlmIULN/kEFZuoqJV9tWZaE7jaktDPojYA6OVuJmTOAuq4/90kXjY3VHklsIwQBiGZ6SccGF+ht5z5NUfLp0t8T2k11Tser7evSUdmltfSU3XpSfKXOy1j6YlzFmAllc+DGXdd6/l2Ogt5lmIytsVXGVm0UxTOysljOb/+O4LqznVZwZD99QMmy1nIWb99wBHkidmZDgnJCbQJ5P1DVWZ12Unxr1lcRRKPuj/Gcmk5B9rKulm6Ot+YuFri2QpIGHRzuwOSpXWX6hurB7VlwCP1VNsVys/KrF3zWzOoHNvtsqYbOmueZsgsgTs+Mqtaqp/1i3EOKGB6lVULekePx3DMzct8zqcnLjx8m4TepGYNncQbhfzcDWfxkqu5srCrpakp+7nGIWg+LcF70FJR/00sFMvl5t80wECOC/bNlFE+/8rMIBnIehff7Yw7euRuPy8eDGLLPIPFAIdsfRuk85mN3XyEpsKKKm3GjIi6raHFTpWcivoPXZ1sMSyaf+83B2H/HL2+56liRJfs2EsJHu/o8dzvpMhqueNSHPqVRNfB+ENtCchzTglv7p0OivBUeVUbXRM0Iq4v6TaNXc2nKlmRn/1hCozhh9BvXWtWnk8FhotNjHQLQFO1Rq2opF4Do2rhnfgJ8frHqdduATmhYPaGSF4JyWvqQU5L/ju/fIXfR6k+oOCXQfzQgX6kGvUnNno9vqSNyQ7ZUXP+qWkBfilMEH6saH9tM/UNLrHUoywcHsA+vfq2Vu4ucT6/8lSrfBQsLv8eqXnyWNhvG7Q1ok+IEb8e/gVoyVHaTygpgwgJ01TPPcUoXHxLu39KfdYmxZJRz1ez0SRvHbdfgFvOX+82nw+p7UK5hG0+/74OYr5SLd4/7L5ROASBKK/1ndauVl1oMABJ9AJQV/IACsbvCSwZ8fARLp8LtXLEoB1J8VNQQFgQHzGk7kmkneBp6qAA7YcokJrWTxpAiQAAnnkozxDygrDK4VEJGCWS4Q8J1p7cYjPhSasfK10qXoHVS5AMjhTqgevLzwBRKdmiDrfYnvbt/pJVHL4Pa9k/wmSO9EePozqChlMm6Ashs9KIn5hn1/r2Epi+SlirXZhCms1j8s+MGlKg3+hZh7iNgqgBXzZSNaZy7lDvgGMuoELzB2JHus1ugvVKTQ0+8JUi/y9UAvEhi6MIULIna4vtI0k4ecVxKq2TXbTSiZOrW5NzxbjpEd95g3PKo6TFL7YzwWvouRWt9BPUGcqS1E/kREJunJpa2GRURgwsWM2WdRR7CwhyVwtKRIvMkiWTNKhUoDie9b6cNeSrTSznBVRRFNB6JDcJkTEU0SqSr4hdYvThh7k3ka53BQ76CXpCLJhlJ5mWdlKCSuWF8/2vbIJXde/+y3r9fzJl/EDyqKK0GXYKVukuFerQVirfQ40siHFWc3ib9uql07Z9MaFaFp3VZtk4UfWh1uhG7dJCBNzMhnbz1NwNYfiyRv0KD8hPz+0W/i6KxOH28hcLKkbI3B5E9wz2PdVyWupHE6ODhjFnBMcptBOcuSeMV2G4U71cAYfXiHZHoyMliHfSjmgFilk40=",
                "E4t/AAEBIwHjAkICYwKBAuEDAgNBA8EEhATBB4EIQwiBCKUI4QmCCaEKIwphCuELRwtkDOINAQ4BDkMPohAhESMSoRMGEyEUAxUBFWEWAhZDFuMXBRdBF8MYYRiCGeIaQhrjHEEcghzDHeEeAx5CHwEgQyHDImEigSLDI0IlQidBJ2MnoimEKgIrISyiLqEvYTCBMYEyATKiMyIzwTSBNeQ2BTZhN0M3YTeBOCE4QTjhOQE64TsBPCE9Yj4hPoI+oT8DP8Q/4UBhQIFAwkECQqFFYUWCRiRGQUdBSuFMok2iTuZPZVAEUONRQlGhUmFSgVTJVgJWJVfBWERZAVsBW6NcoV0BXcJfIWEiYeJjYWXDZsFnoWhCaOFpImmBaeFqo2wFbOFtAm4LbqFu4m8jb2ZwInFjcgFyg3OCc6V1QXWCdaF2pXbCduF3YXfDeAN4QXkheiJ7Y31ifaR94n7Bf4KA4YKhg2GEgYSmheGGIoZDhsWIIYkCiUGJwonhiiGKoYsii6SNAY1CjgKPwZCjkWGRwpHikkGSxpNClcGWoZbCl0OXg5ehmIGaA5qhmuObBZzCnUGfIaDCoSKiJKJDo8GkoqVhpaKmQqdhqCKpgauBrQGtI63CryOvgq+isAGwgrDBsaOzQbRjtOG1IbXhtyK4ZLiluWG6JLrhvWW+Yb8CwSHDIcSBxUHFosZhxwPHgsilyWTJwspiyqjLIcuBzGLMxM0FzeTOJM+B0ATSIdMk04HT4tUB1aHXwtlh2iPaQ9rh3QLdId1j3aPfI+IB4iLiY+KB4qXjQ+Ph5QLlQ+Xl5qLnAuci54HnyOkC6WHpweoB6qProuwh7KLtY+4h7mHuoe7k7yLvYu+h8AHyQfLk82PzofSB9aH14/oh+qP7A/wk/KL8xv0h/cH+Qf6H",
                "E4t/AAEAgQHjAiICYQLhAwIEYgTBBWMGQQeDB+UIAQiDCKUIwQjhCYUKBAojCmMK4gtmDEEM4g0hDWINgw3iDgEOIQ6BD6IP4xAhEEEQYhDEEQIRIxKFEqETghOiE+EUARRBFQEVIRVCFcEWAxbCFuMXgRghGIIYoRrjGwIbQhvhHAEcIR2hHeEeAx5BHuEfYR+hICEgQyDCISIhgSLhI2Ej4iRhJIElIyVCJgImgibCJ2MnwSfkKYQpwyoBKkEqYishK4IsoizGLWEvQi+FMIEwoTDBMSExQTGBNEE0YTUBNUM15DZhNsI3RTgBOCE4RjihOWM6IjphOoI6oTtiO8E8ITyCPKM85D1iPYM9wT3mPoI/CT+DP+FAoUECQgNDgUPCQ+FEgUWCRiRG4UchR0JH4UhBSMFKJEqhSyFLQUvhTCFMxU2iTmFO5k/CUCFQQVCBUKRQwlDjUSRRQlIEUmFShVKhUwVTxFQhVGFUyVUCVUNVYVWiVgJW4VciV2FXwVgHWERYg1ikWQJaIlpjWuFbA1siXKVdAV1CXWJegV8BXyNfQV+jYGZgwWJBYuFjJGPBY+NkAmWBZcJmRWbBZwJnYmekZ8Vn42gBaCFoQmlEaqNsQmzIbQJtzW4BbiNug3BCcGNwwXEBcSNx4XIBcsVzIXOCc8F1RnVidcN2YXaDd6R3w3lBeYF5onnjeiJ6gXrhewR7Y3ume8F8AnwjfGF8gXzCfYN+gX7BfyGBYoIigkGCwYOBg6KEIYSChSSFwYZhhoSGxYiCiUGJwYohi8SMIYyljMKNQo6hjuGPA5ABkKGQxJFBkcGSoZTBlOKVIZWhlgGWwpdDmAGYgZmBmgGaRJqBmuObBZthm6GcoZzCnoOfAp+ooCOgQqBhoQGhgaGjo8GkYqSBpQKlQaWnpeKmIqcip2GoIahkqKGowqjiqYepoqnFqeGrgavhrGGsoazBrOKtAa0jrkGuwa9Dr4KvxbAhsMKxIbGjsgKzJLNBs4G0ArSBtMG1A7ZhtoK2wbcit4K4AbiBuKO5A7lhuYK6JLpjuoK6orrIuuG7gbxhvWG+Yb8Cv0K/or/owAXAw8EBwSLBocIBwkLCgcKiwwHDIcWixiLGpMcDx2PIAcglyIPIpclkyeLKqMtBy2TLgcxhzILM4c0izYHOJM8BzyPPgc/D0AHRAdFB0eHSAtOB0+LUpdTC1QHVJNXi1oLW4dcG18LX5NgB2CTY4doD2iHaY9th2+PcAdyi3ULdo93B3gHeQd6h3uPfAt+h4CHhBOEh4ULhYeIC4iLiY+Kh4uPjAeNC5ITkweUh5aHmIeZB5qLngeiC6MLpQemB6cHqBeoj6qPqw+th6+HsAuwh7ILswe0B7WPtoe4m7oHuwe8h72LwgfEG8ULyQ/Lh8wHzIvSB9YL1pvXj9qL3Zfgh+GH4gfoC+iH6ofsD++H8JPxi/KL9Av2j/kH+gf8C/2H/wQ==",
                "E4t/AAEAoQDCAWEB4wInAmEC4wNmA6MEIQSBBMEFIwWCBaEFwwYCBkIHgQemB8EH4QhECKEIwgkiCYIJxAnhCiMKpAsBCyML4QxBDOINAQ7DDuEPohAhEGIQohDBEOERAhEjEWER5RKhFIEUohUDFSEWQxhEGOEZQRmiGkIaoRrCGuMbQxzjHQUdQx2CHcQd4R5CIEMhIiFmI2Ej4iTkJaMlxCYiJqEnYyfkKIoowSjhKWIphioiKkEqYSrCKyErRyuBK+IsAS2hLeEuYi9CL4EwAjBhMIExAzEkMWExojJCMqIywzNhNIE1RjVhNYE15DYFNmE2wjcBN0U3oTfCN+M4QTjkOaI54zohO2I8IT1jPcE/4UCBQQNBxUKjQyNDgUPCQ+JEAkTBRORFJUXBRiRGpkcBR8FIQUhhSQRKJEphSwFLIUuBTCFMYk0CTWJNgk2iTmFOwU7mTwJQIVBBUKRQ41EhUUJSAVIhUqNTAlRhVINUyVUBVgJWRFdhWIFYpFkBWSFZQVnCWqFbIVuiW8FdQl4EXoFeol+CX8Jf4mCiYMNhQmPBZUJlgWWiZgNmoWbiZwJnqGfjaEJopWnBaqNrImvDbMJtAm3BbgNuIW6CbsJu4m+lb8FwAnGDcoNyxXMhc4J0YnTmdQF1YnXBdoF2wncBd2N3pXfEd+F44nmieiJ64Xtje8F8gnzCfON9IX2hfcF+YX6hfsJ/IYAhgQOCIYMig0SEIYUkhUOFYYbFhuGHg4hDicGKAYpBiueLQYzCjUKOQo6hj+GQoZFBkcGR4ZKhk6OT4pQBlGOUgZTClOGVIpWElcGW4pdDl4OYIpiBmKGYwpjjmUOZYZrDmuObY5uBnWKdo53BniSgQqBhoKWgwqDhoQShSqPBpIKkxKVBpaKmwadhp8Go4amCqaKpxaoBqoGqwavhrGGtYa3hroWvYrABsCGwwbFisaOyI7LjsyKzQbPhtIK1AbZGtoK2wrbhtyK3orghuQe5YbqiuuG7BbuBu6G8ArxhvKK9ZL2BvcK94r6BvsG/Qr+Bv6HCosMhxaLGYscCx+LIQchiyMHI4clkyaHJwsomykTK4stBy2TLgcwhzGHMhM1BzcHOJM8jz4HPw9DE0UHSotOB0+LUYtTC1QHWI9bB10HXYdhh2IHY49oD2iHaY9sh22Hbod3k3kHfAt/B4AHgIuDB4cLiAuIi4mPigeKl4uPjQ+Oj5ITkoeTB5QLlIeVD5cHl4eYh5oPmxuci54HoA+iB6aPpwenh6iPqo+rh64HsAuwh7ILtY+4B7mLuwe7j7wPvYu/B8OLxBvFB8mHywvSB9KH2IvZi90L3ZPgi+IT4ofkh+UL6Avoh+oH7AvtH++H8Bvwk/GH85f0i/UP+Qf9h",
                "E4t/AAIAgQDBAQEBQQGBAaEB4wInAmEDogPBBCEHgQeiCAEIoQjCCSIJgwnhCsMLAQtjC6MMQQykDUENwg4hDmIOoQ7hDyEPYQ+iD+IQIhBiEKEQxBDhEQERYhHCEqETAhNDFIEUohUBFSIVRBWjF4EXoRgEGEIZQRlhGaIaIRrjG0Mc4x1hHgIeIx5CHwIfIR/EIMIhwyIiIoQjASNhJaMlxCaCJqEm4SciJ2MoRCiKKOEpBikhKaIqASrCKyErQSvBK+IsQSykLMEtQy2hLkEuYi7BLwEvQS+BL+MwYTEDMcEzoTPCNIE0oTTBNWE15DZhNqE2wjcBNyE3RTfCOCQ4QTihOOQ5ZDpBPCE8QTzBPWI+AT/hQIFAokEDQSJBgUJBQsZDQUPCQ+JFwUYkR2VIgkkESYNJo0uhTCFMYkzjTURNYk2CTuZPAVABUCFRJFFCUYJSAVJkUoFUAVQkVEFUgVTJVQFVIVYCVkNWwldhV6FYAViCWaJZ41oBWmNbolwEXIRdA16iXwJgIWCBYKFgwWFBYeJiRWKhY8Fk4mVCZcJl4mbhZ4FnoWfjaANogWijacFqo2tBbAJsImzCbQJtwW4BbwFvQm9lb6Vv5HACcIJxAXFhciJyxXLic4JzpHQFdEN0onUDdUF1YnZjdsJ3onjBeqF6wXrhe2N8JHxEfIJ9A30hfgF+Qn6jfwR/goABgSGBYoMBg8GD4YRhhOKFJIVChYOFwYXihsWG4YhCiGGJIYlBigGKgYtBi+KMwo1CjqGPAY8jkCGQopDCkcGSA5Jhk2GUAZTClOGVQZXBleGWBpbhl0OXhZjimQKZIplBmWGZ4ZoBmiGaQZpkmuObAZuDnCKdw53hnmKew58hn6SgQqBhoIKiAqKho8GkgaTBpUGlYqaGpuKnYafBqOGpoqnFqiGqgaqhqyKsAaylrYGuIq6BruGvobChsMGxAbGjscGzQbOjs+G1AbXBtgG2grcit4S3orghuMK5QblhukS64bsFu6G8ArxhvIK8orzkvQW9Ib1FvWS9gr3ivkG/Ir9CwKLCA8Mhw0HDYcOhxQTFosakxwPHgcflyKLJZMmDykfKY8qBy4LLwc1BzWLN4c4BziTOwc7kzwHPgdEh0WHSAdKB0qHTAtPk1MLU4tUB1STVQdWF10XYg9oD2iLag9rh22HbodwC3KLeQd8B38HgAeAi4QPh4+IB4iLiROJj4qXjQ+Nj48HkhOTB5SHlQ+XB5eLmg+bE5uLnIedh54HoBOiD6OHpwenh6iPqo+vC7CHsQeyh7MLtAe3B7gHugu7B7uPvYu/C8EPwY/FB8YTxo/Hh8kLyYfLk8+H0ofTi9SL1YfWB9aH2Yvah9sH3Rvgi+IT5gfoB+iH6YfqB+wL7Yvvh/Ab8Y/yh/QH9Qf5B/uE=",
                "FIt/EAAAgCAIBhAAAwHAEIBAENABCCAAQBAAABAAIAQgAEYABAAIBACIQAAAAQAAAAABAAAABCAAYAgAYAAAEEIgACMAAAGAQABAIABBAAIAgEAIgAAMgQBAAIgAAAAABAIQAAAAAABAAAhACIAQACUAFBCAIQCAAIBAAAAQBCAAQjEIAQAAAAgDCAAgAAAAAAAEAwAAEAAAAMAAAAEAQAAAAAAEAABGAAAQAAEAgACAIAACAwAAAAAQAAAAAAAMgAAAAQTAAIAAAEAAAAAAITAEAAAAGAABBAAAQgAAIgAKIBAgAAAAEAAAACAEAAAEAABBAEAwhMMZBAEMIQFAAIAAAEYAAGARQACAQAAAMQAEAAAQAEMAgCAAQQACAAQECAAAAAEAQAEEAAAEAAAgAEQBAAAAxAAAAAAAAABAAAAAgEEAAACAABhAAAAAEMIABAAIADAAAAEAAABCAAAAACMxAAAAAgAEUAAEAAAAAAAAAAAAABkAAAAgCAAABAAAgiAEAABAAgAACEQQACAIAAEABAiAEZAAAQQBAAAIgCAAAAAAAAAAAAAACSAgQACMIhAGAAAAAAAgGAAIAAAAQQAAAIQAAIAxAAAIAAAIgAAAEAwDIIAAAAAgAgCIAAgAUARAAAAgBCAAAAAAAQCAAAQAAAAAgAAIAAAIACgEAJECEAQABAEQAAAgQwAAAAwBIAYAACAAQAAAAACIAAgACIQAAEEQQAAAIAAAAABEAMIAAEQQAAAAAQAAEIQgAIogAKEAAAGUAAAGEAAAAAYgAAAAADGEQQACEAAAGAAAAIEAACAAAgAAAAABCAIwAAMIABEIQBhCAgAAAEQQkAAhQAEEAABAAAgDAYAgCAAAAAAMAAjIAAABAAYQEAIABAGAIgAGAQigAAAABAAARBCAAACCAIBCGIYgAAAAAAEAQAEAAAAGEAQAAAAIgAAAAAAAAAQAAAAACEAAAAAAAQAAIAAgAEQAAAAIADAAQQgCAAwgMAAAAAIABgAAQABAAAQAAIYRgIAIAAAEAygGEYAAAEAAAAEAACAAAACAAIBgAEAAgMAIBBGUIABAEYAAEAAAgAAYAAAMAAAAAAABOAIQBIAYgAMAAAAAEAAgAEYBAAAAABAIIACAAIAgAEAQAAAIgBAU4wAAUIBgAIAAAIEAAACEABBAAAAAAAQADCIAAAAAoAACAAwgAEAACAAgAgEAAgAMAABAEAICCMABQAIAAgAGEAAAAAgAAAMoAAAEABACMAhDKEpACAEAAACEAAAEAASBCEAAAAAAABCAAAACAAwAAEIQAAAAQCAIACBIAAggAAAgkAEYAAAUAAAAAYggCMAAAEMADhAAJgACAYAGCAAQAEEQAiAAAAkAAAAkCAoAjGAAAAAEAAAAEABDIMAwAAAIADAQpAhAABRCCAQSgAAAADCAAAAAIABhAAAgAIQBRBAAAAAAQYSBAAARBAAAAAAIAAAAAIAgAAgAAAARwAAAZACAEAAAGAIQAAAIiDAEAAAGQQAhAEAiACEAQAAEAwgAIYQAAIAQgCAAAACAAAACAAABAMQADAAAQAAEQAAAAAABCQAAACAIAACAIxgAIJBDMEAAgAAAAAIMAQAAEAgCAAAgAAAIQACEIACAERgAAAAABCAADACMAQCAEgAFGEYQgEEAQAEAABAOAADgEAAQAAIIBhAIAAACAAwCAAAQgAQ=",
                "FIt/GQQgiEEoxjEYZCHGAQBBENASjGYQgCGMQRgKIoRhGEYRAGQxwBGIQgDGUYhkEMRiiCUYhDKIJQlKYgQhEIhAoCIQkhEAAxhGMRhhGAJSDCIQyhCMITCCMIRFAMAREEEQxiCMZhjAQIgAIEIglCAIBjEAQhiEQAQECMIgjIAQDDCIIAhEUhBDCYQSgKQIRkGMQSCCEIhiCEIRECEYAhMEQigGMQRGOEBhiCIRQlKMQwiEMIxECQZBAGMYggCEgxkCIQDDCMYUkKMZBiEIgTDEEQRDCEYRCEYQRCAIYhBAIAxBGE5xlEEIRAKMYhBKIYwCCEIyDUMJAgEQIxAOApAiGIYxkMIgRCEMIxBIEZAlKQIjCGQYwBCMQxEGQJiEGIAxBCIhhAKARRjGEQADGEohkIERAiEEoRgCIIxGGUAxDGMQinEAohhEM4wiAMACCEFARhGIaRnKcIkDCIAQjCARBjEIgghIAowiEcKgCGRIghCIQiDIMZBjEEYjBMcwwjGIYxFEEgBFEUaACCUIAnEIZBiAMZBCEUZRACAYhGEQJCBMEQkCEEIQgSEQBEAIJAiGIwwhEEhQhGQQRlEQIQlEsIxJEAYyDAEQTEGIgQjKMYxBCEQAiEUoiFEAYRDIUQTDCQAhBKIQQDKEJAlEIIBDEcYBCCQIRBCIYiFCAoyCEMQQjCYQBjAUYxhAIYhhAIYAiCIRQCAARCDCEoRBEERwiEQQREAQQThGMQxmAIQFCIYAykGEJRkGEJACOFARDKAgRhEIQzkAERRhCQQgkAYJAAGEQRFEQxBDGAqRAEUQgDCMYBCCIISoKMQxhENAgkGEohBGIICjCIYxFIMhRgEQQSiEEIwDIgQBiGAgxCMAYhBEEgAjCEJCBIEAgiCMQhCQEQijCEAQhCURAjIMgwDCQRCiIEYAgCIIQiEMIxkMMAhEEEgAjUFAhjCABgCEEQThGMYhDIIQhCGQQwGCAJAlCYQAjAAghEAMgzDEUQQnEEpADAAJAiAQgxBCQAhhAEIQiQUoSnGMAyjCQhAhMIIxiGIQQBEMxBEEQYBhGEYQkCIIRCEMJSDEEIyCEIIBBGEYRDCIIRjKMIQhOAIRBGIpQDMYYxBAIJREEEYxBEQJiGIMYyCCcIQiCMIQgIEJRDGUQSDOcIxBCQBwpCIIwFCQwhDAIQgiCQpDjCMQyjMApBFEMZgEEEYhCCEghlCIYAiEYxBhQIBiFEARCEIERBHIIIAkGAIxoEEoRBIEZACKM4yDGQZBjEExAjIEIwgEQQwhCWowjAMJCFEEYiBMEYgBIEYQEAAYwjAAQSCIIYyBIIgxHAEZBkCEYQEGMoghEEYgiGMRCiEAAQAEAQxiCA4RBOMQwiAMJAEIEAQCCIYTAKIQwhCIIyFEMQQCIMYSAGYgBCGIQilIMKBDGFIwDIMIAiEMARDGIAhhEMIgkKAghBGIJRDCMYhjEYBAgEMIRhEIIhlEYAxEUEIwBEIAxHGIYhDGIIwjGMJBjCQoyDCEQRDMQQhEGUAzACAxBBGMAxkEEYhCCMQxjQIZBgCAQikCAIhDEUgxjIIQihAEQCCEQgDDCQRgGGMoRFGIRgkEIghCEEIxjGEQiAIEQhAAUpkBCIBRhGYQiAEMgRCCFRAhEYZBDKQgjBGARiCAAhBFGAIxkEIgSBEEYCDOQIzjEQwhDIMYCCGMYxkKEgxiCIgQCEQ=",
                "FIt/QQQShGMYxjEYZSnIUhQlEVAwiEcQykKJACDGkghhEIYiIGchAlEIZxkEIhSiQUQhjEMIylIURRmKYoSnGQhBCCgghkGQQxjEMZCiQMoyDGQYwiEYgyDKMIxkIMQQhCMhBEKUYiBGMoykCQpiDGQhSDEQQhiIMJjBQMoxJEdYxlQMYxhEUYxJGYZSiGIgxlKIZCEIYYhkEMQhlGIhSESIIyoOIwTFIgQwjCIRUFKIQxDGMRCFCUZSoKMRAiIUyiEEAhRjOMpUjEQhEFUIYjBGM5TEEMYxDEEgxEIQYhiEoxSEMI4yFIQggjKghREKIYgnOEQikKIRCiEQowjKIwjEEsZCCMQxSCIMozEGIRiEGUJRkGcpCCIMRSkEUhRkKEQyCSMxhDIMgxkGYgxjEQI0DKohiDEQZjkKMojlAIYhjGQg0GGIgykCMwyCENAxDGcQyiGMxRkEQZjEScZCmGEhijEUYxEIQowkCQRjCIUIDCGgoyGQQRRkYIqzCCMRCDMMgQhEMoxEEQghkGQRyCIIpBhIQZSiMQhhqEQoimGQYhEMZZCCOUQxiSIRQiMIQiiGNJDiGEZTiKIggiIQQhEMQQypEUaBEIMYRjIYYTBGIYzCGIwxHCUhhCKQgiEGIZDCMURihCMYymKUJRlKcRQhQMoiFMMZClIJJRHGMohjKQQhEGEYRBEQQzJIUYjkGIgxBOMRBFMMxAjEERxjIMQxkKIYxDIIQTiGQYyDCQZTCEEJHlEYoykEMYzhIMrhhIMQSDGFJBEGQIxIKEYhDGghBkIMQ1EIgYClMIwwjEIRSFGMhCCIQpRmIIxCDQYojDIcghnMIpCBIMwzjEQiAkEEQxFGgYjEIQRRiGcYjlMMQzCOEZBmMMRSDKQYhjQMhCiIEgilMIoxIIIQhDKIKREGIhCCKIoRiOMYhiEMpBCGUhCEEIZBjIQyRlMMZiDIAhhkEQhCDEQgyDIIpCIEUghkEMJBEKQgiDGMYzDSUhgkIgRRjIUySnGQiUEEIRwiGQpCFKJKDkGUJBiYIZRGGQhSFKIYgjEUQxFKEoxiGcRCiKMRBGGJJDiOMZBmMRgykGIYhFEMZCEEIYSEEQoxFOU5CjEUYjkIERRkGIIhkCIZBkOYJBoCYYyoMY5CjGEJBhGU4iFGcgwjEIJCJIkBCEGMZSIMIghGEIRClGMyQlEMoxjOE4xiGQIyGIMYiHGYhCiEQpgjEMhClYYYyGIMgiHCMYxkEERzoMIpCCGYZBCEQJBiGcpRDGERBiIUgyjGEx1iEQhhDEIZBkGUgyiGURBDMEiDEKUYxDGcQyJGIgxjMQYxkEMYzEKUYgjGEZglKIBShGEIzmGMZQjEI4SCEM5RFIUZhECUoSEGApBFKMQhCGcRyEKcZgkIIxRDIQwxEIIYjiIIpRIIMSCGGMqCjMIgiiGQYhEGUAhkGMBSEGIgjEKIQxmGQoyDIsxjlGMQhmEIiRlEYYxKYIRSGEMwiEGIZxiKUIhhOQoyEMQQgkMVQijMMRChGUoyhMUJhlKIYzGCMggjGQZBiQIpBEGEQiAKJKhEIMQhjCUYykGMZBDQQgxGOQZgjKMRAiKQZSmEI4RkGMJSCOQwSDIIRFCGMR0EOYQziIMhCEGIgSCKIYijKQYxiCMhAlGEggkKQhChIIQ2FIMJCCKMYioEURBjGMxxDEMZBjKUYyCQMohCOMoRnKM=",
                "FIt/OQRCFUMZTDMU4zHKMgzjIRAyjOUpRmKUZBkKMgxjOUYijGQqRkMIZyDGUxDkQMYyGIMxyHEIZhFIcohkGogxjGYwyHMQSDjEUoxpKYoylIIpiHGUoyEENAxjKUpDEMUpBkKcYiGGYgxDIMoyCIQiiFUEZECOMoxEQQpzFQMozDQQZTIQM4ynKYhmjGQhVpQQZCEGYZhlIQpkEIQhCoKcZxEKMgijGUxhlKQSEFKQZhkGQgzlKUZSoOcRRGIZoiFGQhBiMQpRpKRZDFIMZDDGU4zDGMhCDKIYiDKQZCFMQqiDGchBFIUZCDGY5yGMQZjDMMoiEKMZUEOUpDMOMpUGKYxzDKN42mUMaBkMMhCDGURSCGcxSFMIRCoGMgyDGMhCkSgxSGEQZRkGUhRmGIpBkGUayjMI5xkOIYjGKNQxmIUhyGIQYiEGMoykGIhylGYYxiQQwjkYIgjEScpjGIkxFjGIpBmIUZCDIcgjFGgxBjIM5SGKYZyFUIpDFQMpiHGcpBEGMwxlKNJhkKURyFKQhSDGcZEDMQhDHUMhimGMyCmMY4xjKU5iESMiTEKUxTEMMyhkGMhjlUIgjDKVBDFMQhSHKYZyEOQgzEQUgjEMkZDDIMZCjMURCEKYZDnOcpjGIUyClKIwzmKUJCGKchikQcYTFOQZBGKMxyFIQRBkKQQiHIYxCIGUxTEKRRCkMQZRDOYoyIGcpyGMIxiFMVAijGUayCIcxVoIQwyHCQphkKERVDIQZTFIRBjmGRBDFOMhCKGJJRDGMgxEIUhiEMgiUoEMhjFMgoxiGJBSmGkgzFEUgyDGkxSlGUZElWQwxkMQ4yDGYhBmIZJBpIkiCGIQiTlIQpRFMUhTkGchTlIkxDFIIZSDIMaCpIUYiiGQpBDGYqimMYpSIKEhTiMNBSEGQ4zDIMpRFIIwijIIZRCYEhRiMMgyjIIZikKUpylMURjkOQJjDGcZhmKMZDHIRpCEGMYzEOoxiIIUYzEGUhjEEgZTmOExiDOQg0EGQpCIMYYzlKYpTmMIgyEGQpSGEYhUFEkhhkKcxCJEIhzEKJBSkOQoxmOJJiIEYiSFIQYxGIRJSEOQZSjKMpCnGUhSmENRBjORRBmIUZTkKMhhlEUpSDGYpDJMQ5UDMMZTFIUZzCGlBCEIchhIKMZypKU0jFEUZBEGMhimGUhDFKMhDJGMRBEOU5SEMYgyGIQZDHKUY0GOYxCkMQhhkGUhCDIQxSEEYxAkIk5RkKIpDEEMpjlKYRFCGcRSjIMxjEKIiBkQQxTIGQpTDEMZCoIYxBkGMZCEKQgTGKUYyGGUpSJWIpRjOYZDJGIhCFKUoikGghQjKEgyKGYxEDIQiSGEQhCCGIZBnIMhSEKUpRlEUIhkIs5CmKQpikKUaEDMIwzEGMxSkIM5ToKYhCDGYiDDGMqBkIQhSDIMxSEIJBEHEcRRDGYxECKMxBmKU5hCGsZRDIUwzDEUiRjGYhyqUUQxlEUySEGgZlDKZIyjOcg0EGMgkmMMphiGYpBkEUoyEKYgyEKkZCEIQxEHOQhRlQMhClMMxhmKdBCGKQoyFGUw0qKYZSFQIZSFOQyRoIMhBlGMQymGcQykMUxCFOU4yCMMRhkINB0FIQZClIMhDMGYpCIIIRClIMhjmMUhkrKMphmMUpCmGQYymEUhymKgaTIKQaBmGQpzGMoZRFQMhRmQYZTEGMgyHIU=",
                "FIt/QQhSGKYZSEMIhTHIUhToOgo2FKYxDFKQhUkMUpDFKYxyFKQpSkKMiTEKYpyHOVZEFKgpiiMUpjEIY5TlGMxDDMQozDMRJimWQ6iGIM5CFKYZCHCYiSHMRCCkKQoyjMdRCGOcZTGMUpSEKYoxmKU6ikSUyDEKRIzjQQpTJGcgzHIgRDGQgaDkUYhClMUpSHIQZUsIQxSDGQyEFSdBzmIcxTEGQyTFGYYypKMpCFMQiDEKUo0nMYwyIMUihEIgxSHOQhSGMYp0kMUzjlMQiEmGMZTlIQhSIMIxSEMZJjLGcpjFGQ5TlMQhTFOcpUEOYZDHMM5DEKcxSFIUozoKchTCKQyCjGcZRFUMqEmIopSmMURTEKcyBoMQpkIGchSnOQqSpKUyCDKIgxpQI4ymIQ5SGKMZSmMc40FMQijmMQxTmMkhUGMIhSlKkpTFOdZTGGcwiqIUxTGMQhCnSYhDjIYZjHOhKEkIQhClKURSFMUxDGOQoykIYqBoQgxEGOZBilKYoyHMYxRFMNLxoKUgxlIY5hmKQxTGKQwzjIQ5TGGQqDmMQpSjMMZyFSMhylOQ5DJMQpSkMkhjtKY5jFGUhikMg4zHGYxzGSUgykGQiDlKwZSkKYyCkMkZiEKYZDlMcpzGMUhCFKchznMUxSGKcpinQcxjFKYwykGQhilKMpxkGYZDHKYZTmIYhRoMgpRlIQxTkKYpSIIcxDkGQpSjOQhCkKQhSEMUwzoMU5iFMkZiJaYpCEKMpSFIQpTjMY5BoKYpTKGMxSlOchilMcpyEIMxTFIMxSFIYpSlMUySGGNBSFOYgxlIZAyHIQZkFKgyxkSQ5SlEYpDlOZIhlKkyBmKYpklIY42DIZRSlIkpTkQQpCmIc5CGIMpDqQUxDlGZJSEIMqioOgxRoKRBiLMUhRpIU5EEMMpiHOQoklIQhSjIQpCoMM5TEUdJiFIQpEFIgpDkKQZlEKY5CFGUiyDIRBBkIVCTHOUhCFIQ4zDUUxinOQhjDKQxCnMQozGMQpSrGc5UjKdKTmIcpjlIQ5SGEYpSFMcpiEMcZCkIhBVjKQhCjMQqBmOYpiIIUhBmKUaCFIpJDkMVCSFOQ5ijOUhSjIQ4yEOYZTEKYhSFMYyyHIQhTEOYwxpSZxCISohUoIQZClKg5hkQQiRoINAyGKQxhmKQpikMUhSlIUZzFIYhzmKMiDDIQ5DJOQ5CGMgxDEIUhSnQUhknIhRyHKYpEGIQximKQxymGkyioKgSTIKUhClIYpiDIUhSIKQxiDKMoyjUUhyFGapBEKQhBnIYZDFIUpyMSQgyGIMpCnKQ4iFIpBzkOQgynIUxDFKoZCFKMyTlIUojpIcxDpGYxCDIQhClGYaTjIchSEGZBBkKgpylMQp0HIRCimKQ5iFGYxTGMQyTEOQ5CnMYpCmGUaEGKMhlEOMxjkIUZkFKkaCmMQ5iFKYhxiKNJBmKchDDSYxSEKQ5SEOQqRkKYxCKIRBCFGQ5TGGgZjEIVIyDMQZDFQMxCFMQpTlKQpDDGUwynIciDFEUiBjKQZzmGkqTlQYxiFMgpBnKcxCCMMpipMUwjjOcpRlgQhinKQySoOgpEmGNhiEOdAzjOMhSHKUoiDIUZCnIQiEIUYhTjKc5TDOYpyIKZREkIVCDjIQ5jGSYxzmMUxjmIQwjIKUJClMZBjGORBTkIQ5CIKchRqOghzjQYYzDIMZSKIQ=",
                "FIt/McZDIKQxylMkxDHIUpSoIQxEEIQxSGKgqjFOkgkmIc5FmQYpiEKQpzEKYpyFOIozLIMZCkIghUoKM6joMkhSkKNRSGOhJUmWYqilKQxjIOUhSHIVBDmMRCymaQxCmSchCFKc5DIOUpzGKUo0FIU6jlIUxTmKQxDoQUxDIMQhkFIQhxmSYpinIg5zHGcpTnMQxSsIMxDJKZRCpScpDGKUhSFKM4ylKUhyoKcpUFOVJymIUxUHQchimGYhkFGQoynOQoylMY5SDKUqEFOVBjFQVJUkQQqTlMgxkpKQZRmKQpzEOU5CLMYwxnMcpTEIZZDEQY5TKIsZBmIUqToKYhjHKUySFKUhzFUZyCIIY5DnIUaikIUoykORg0lGQyTEQopynEUxiIIYhjjQUYxkKYhilMQpjFIYhTlOQpzkIUxkjIYqBlIU6CoOQyCFMQpDFSchjqKUxSGKc5ykEYpUlMYpSnOYazlOVJynKUxDHOQxjGUcpSDKQqDHKcqUGKQpkDIspDHMQpynGQpDFMYZyHKQxhmIQaDmMVJUnQQ5SGOQqCkQQpSqKcpTJSgqTKOlBDlKUqjHMQhTlMkhyGKUhymKg5iHIZRSlMc5zGIYw0GIwhhkIRAypMcZSFKYpjmMU5jlMQo0EMMhTnOMpjlKopSHMcpzEKUhDCKU5CGOopmmMUZEHIYhDnMQxSkMgpRpUVJUHOUhTkOghiFOUoyGKMhSHKkiyDGIxTkKY5imMghhoKVBSHQUpknKhBSHKZBCIMQhSKMUpCGKQhCEMVBzlIgyFFIYpTFSYyCEOUySmIUhxlKUpSHQg5BlOQqEJMYxTFMQ5DDKYyznGYyEIIUqElIUhikOcpjEIhBTmIZBTQKcxhlMY5EDIMpTKMMxTFGchSHQQhikMNCRlIUgzJKUhFFGc5kHIUZSkIZKkkOcxUjSNBSkIQhUsIUhSlIchCGMQhClKYxjKKQaSkIUqDDGRBDmMdCTnOUxioGVYyjKUiiKKghDIIghDkKcoxlIQpxmQY5olQYiSEMUhTkGchyGIghzFMU5SkMchEmQYZSmI0hznQQwyGMYhiHIU5iGKRBSlIdJiFGQaiEQZAzCOQqSkKYpikIQpkmIcZDFIYxiEKIhioOcxSmOcxCIKw5TlKgqTFMU5RoUMxDFOQhEFOkg0jKkhykIohSmKRJToGYhSlKZCDDIUxCEOQqTlKQhDGIUpCDMYhhFKMhSEMUxSFOYpSlKZBxmMcqDoMMiTjIUwzkIc5iFMUhSmSkzDDKcZCFSgpilGgYzFKQhEkQYxClGQhFsKkyDIOVJBGMUxEqIpBzHOQhDHIQxjHQopCFGNBTlEQg0HMYpzpWdBFnIUpTFKVBSMKQhCGEVBiHQU5yGKQjxlOkozHOUaElIghTlIQxhmMQxTEKYhBGGMiEDKQYzEMUpiGMcpEFMRSSHOQpyGOQx0DKoxRmMcpjjIk5CkKQ5zFIUxhEIZRSFIopSGKUiRnIUxioOUhSIOYxELUo5DGKIhTlKQoznOQwyFMUpSmOQhjFMRCDEIchTmSdBhkMQqTqKMxiHIYZilORBzKOMxiEMUpijKUxjIMkhUkIRiCEMcpxkMchTHS1ByEMcZCHOYZEIIU6DkGVRiKOUpyEIYhSlIUiBlIQhSlMUxSkMUyDoKQhCoKY5TkIhRDGGUxiEOQpVHKZRjGMcpykQYxRkIQZxkKU=",
                "FIt/KUREFUYhylMIqFHIcpXHMY5mFMchTGKM6ElKUpyjMRxCLOYxjGQYhCkIRBSkIMxDDMUiCEKYpSEKQijnKdhimKhRzFOhJCqWQ5TFQQxDGMQpSnKlBjnKQhTDQgxDjOYxCFQcxSGIcpykMUhTEKY6jkSYwzlOgpSEKYpTFKcqUEKUp0kKVpEpOg5znGUxzFMUxCsMYxBkKQ5SEQU6UMKU6RmKQxTFKUplFMMhkEOQgzIMQgysOUyEHIQhClMVRBnMUpiFMYyhmOUZTFKUpjGMRJiGOUpxkKgpjEMQh0HKQxjFIMxymIYZDkMYhSEGQhSEMcojGKcpVFOkpSoKYxzlMcyCjOk5imMRyGpKQpikKQpCFKUpSkGQhUEMUxioMZZzGUgpRmQQhTEQU5SGKUhTjOYqSmOcpjmKMiDmMgxEkQYxBjSghSCOZKCmMUiSwMcZDJQchSGKUhCEOUZDFKU5kHIYxClMYpikKYhTnOQhkFMghSnQM5yEOdCzmScpjlIMxklKkhxnIQgylMdIzlGc6RlIUhSHIUhDFUYpVEOkZykMYhCqKUaCDSghUEIdBSFMUpiFIc5RmIVQzGMcpCDOg5inKdRzKKY4zFQM6jFOw5SjOcpUnKkpyHKUpEGONRjmQUxlkMUiCnKURijIk5VGQcxTJOQpTHIU5SmKQ5zmMVCDEKch0nKYpSIMdRBlQlJDnMMhCISUpkkOdBTlKU5kHMkhSlMUqDEQQ5DlIcximKcplFMQpikGUpSHIZBClOQpCHOYpDkGQhilIQxzFOQxilIYhFFSQiBnKUySkGUhDFKYhjkMcxjFKQ5kHOdBTGMQ5ikMchDHGcxClWUyCjIUpiFKYaCGMdBRlSlJDGKghSnMUxTHIc5iKIYxzEMQhSFSYpinQQwxmMUxzFOQhSkSdBRmKRJSlOYhWEOhJDjMRJiEMkqCFOgRBkIQxUEOUpEHKkhjFOMySmUUpSEOc5HEMZBBHKU5CIIIxCHKUpTqIQxTESUxCkIYhDLKQpBoKchSFIVJDkMYpSGIQ5lDEo5TFMkhlFKggymMYhjFKVBSkOQpRIMYhiFKchzEKlBylOUpDmKQaREKY5iFOcpSmIQ5CkKRBDFIYhjpGYxCEKgxSpOM5zIOYxSESU5ioIgiTmMU5SHOYhSEMQhTpIk40HMUhyjIoqimIViClQdBTJKVCCjMQ5DGOcihmMMhClKQxiEMUqllKM5SlOUxTEKYxiGKVByHKkqjFKYxTIGchzkKUyEFMRKRmKQpEEKU5TFSQZinIQhzKMY5UGQYpTHIcpVpKciBmMYgzIOUpDFMYyinOYhCmMVBilQohDFGoxCFQQxUHOQhSmGM5SFIcxjFKQpyIQQhyGKhAyHIQxylMUxDEIchSkMQhRLMYyDFMU5UEKQxVHKkhTmIk6DmKMZylOQpjjMU5DGMUpiJKVJSGKgpkGMMpSLMIaUDSkxylMVJClKYqSmOY0CGUchyGGM6TnKMxjIYYpSmMchELIcxTFUZRDEKY5TDQUpjmMYpCnOUxzEKQjCrOcqElIsxxmOQZSlKlBiHIUxDDMUhToKUhiFIUphjKQ5jIOQpCmKUxylSgZSGIchjIO0xTmOURCkOcwzDIlIyjGYpjjOYoyrKY5CkMYiSnIU6jGQRBSmKUxSFMM5SIKYhSDMgpjnOUxileNJDFIYpDFOc5yDQYgyHIghVmKQ=",
                "FIt/OU5TkKYpiLIU6DHOU5RqMdBTHIYpjGMY4xlKVBylQYpDGMRRiEKcySkQUpyoKUgxlIYxzoIUyUFSg6jHKQhREKUhCHOgyCFGYxCDIUxTFMc6DHKUiCkMRC0GIRQyEOcpimMc5DnIc6CnKdBiIKYpzJSYpzlOgpTlKQxjFQYZDGQU6lEIQZEGOgZDHWUpRlKVBhsMQxykMQ5jlOQhynOYhDFQYh2lIgpCGMUyUJMUhFDMQpDpKYhimIUqiEOUpinOgpSjOMpUHeVBTlIUpTGMRJjEMcx1FIQximMUh0nMM6DDMUxyHGchjEOYZyrKRRSFIUpSnKkxCEOYpSlOYxiLMlSDFIM6CGIgqmmKYxDFKcpjEMQ5SjMUpUkQYpBmMoqSlSVBEoQg5SmKYxylMRRDlKYpDEQQxToKVBiFMgxClGYpTGSVBCmMYxVFMQijFMgpiFIMxCmKchkGMUx0EKQhyHMUhCkIwiCFKchzHIYxEGMkpDFMUaVGQZBCmGUxjlOc5SmMkxylGRBiFKUhSKKVKCmIUpjmUVBDDMYoyGOYpzmMQ5BlKQRipSUhynOchDoOlKCqKQiTFKVJzFQcqFKKg5ipKNSzFKghimOUgzlMwhDlIU50IIQqBqKUpTkMVRhlOYhSEIYpzHMUqTHOYqVGKVRCFIcpijMYpymKQxiIQQ5SnWYhDlOYZisKYhSGKsxDkIUhEISIxSjOUpCGKY5iHSlBDIKUo0mMY5DmMdJimQUpxoKYpCmOcpiEMZCSjKQhSHIcpiEOcphkIc50FWY5TIKcxyESchTHOUyTFQUhEFKQhiGMcxjEIY50mMVCClIY5SESQpEjGc6ToIlCDmIU6iFKYiCEQRBkGKYpTmGQxClMYp1DIcpjHKYxyoMUZ0KSYgyFMcxSIIcxzmIUZWFIdBSmKcpSHOYhGFOgpjFIUZiqKcw0kKRBSmKQxikUQpVMMUxTEKQpEoMYpSJIRpjFIRBTFOUpiFIUpTJSgqjqGMxDIMYoylIYpTHGQpDGKYpzlMYZDmOchyGIUpimKY5CGKkwxmIUxCDMYhDGQRCCoMQo0nMMhSFKtCTGMUpSnOYhCFKYqRjMQwykKQgykQU5hnKVDjEKc6DkGgxjkOghUpKUqjIMQxymSVJUoIgyTDKg5CEUZJiGOZByGKUxClMcpSmKciinKVhHFQYZEpIZBhkQMxkFUYhCEKYaBnQcxCkMYxjFIM5SnOdSCEIggzGKYpzHQgqCoOgxDFGUzDFOMxTkIZCSFIkpTEKYxVGMkhhnMapBlGUhhoMMxzGKYZ1lMdJjGMdJTlQk5DHIY6TGKMhDGKRZSnQpRDFKpBjlKQxCmQY5hmOVB0pScxyEIUhypGUpilKoZSGIU5TlMZBzHMcyEIMMhTLKUxyFKU5FHKU5TkMghClGMyDlMhBzIKUxymOUpSFQQqEIKRJCHIUhzjKppirKYpzGIkxiGIYxkGKoqUHKZEEHQUhUlIU5SGKkhTJKMxilMU50FWdpjkSYxijIYpSnKUoyqQYpSnOQxjmKUxzlOYx0lQgxUlKU5DFIUyCJOM5CmIshSlQxJjkKdBkqGQ6ymMU5iwORJkFSdBUFMMhhlKM5SGKYpjkScpTEIUpUHKcpTkOUhTlMY6RLQYxSmMUhilKdByqSM5iFOchBoMUpjGKMxzlOUxijIYxTIKYpCnMgpSmQYxTGKY5TkKQ=",
                "FIt/OVKSkKYhSlQQhClKcpXEIRBEEKZJCHKlRjGKQhSlMwx0HQVRUFIdJkEOc5iEKY6TnIZJCFMRBUESgyimIhSDIKUxzGIgyCmWYpikIYxjHKVRknOVJzGMUyzoKUqTmKcpSIWQpTGMQ5CmQcxkEIgqiISoxzmWgpylQUxirQUhzGQkxDGQhJkjOhRyHKZBDlQRRjMSUhCmIVBTFKQqDGOcxUFOQpTlKkxSnMgqkJOU5iIKYxSGOY5TIQVRTGIMpiHOkozFKVRSlKgiSFIUpjlIchilQcpylGYZkIMghzHOgpzGQkpylIYhklMYpSFMYxTFKcxzHKkhyGOkpTmSQpjLcYpkFKhaCFMY6BnKYpknKdBUEOUxilMdBioMVCCIOYpzmSkxkGIgxSmQYpyqOcxjIKUpEHMc5VGMopzmKgxjJKYpiEGghjGKZRUFQQyTEMc5zGQcx0GGYhCqKQ6UnKc5inOUhUFIcpzlMUyEHOQxkJWUxkJQlCTlYRCymKchVJOdBzFQY5TlMUhiHMVJSlKUpkGIopSnUUhjFQcpjKQQpjoMYxDKKkxDpSYqCHUZJToQcijrIchTjI0x0GMciCKUghUJKkxTIQhJjGOYylHMwxzlIlSCnOkqCJKcpllOQiVHKZAzpOYhkHMU6zFGYpylKhRznKQxCGIQ5DFMYxmkKQxTFKYxznQUxCJMZRiLKUpzHQUyCoKVCTKKYpSkKg6UHIlBSmQYxTlQYqDIIdJimKY5zIKYpiGMcxylOkqCFQVJylOcxSmKkpjIIZRWEKUpTGIY5xsMdB0HKk6BjGYxjFKQxjmQZBSlMYhEpMlRSHKU5TmKQhjIKYxyEMU6DmMcpkFOgxEEKQpToScxxkMVBCIKVCVGOY5UqQYxikKUxCKSY5CnWQ6TlIg6TFKURTGKZCUJSlRRqKZJ2EOgiSESMpDmQQpyISRCDmOopilIQpSIeUo0lMYiSGKcpynQUhDkKRBDoOVpTIIU5SGKUhjIKcxTHIYxGEQY5UkIUhjHQUxDoGUiTlOUhzmGg6imOY5ilKUpCFQZBjGKYhjEMUhRqMUyCnMQxTIQkyTGMkwzHQg5RmKQiTJKQ50FORpypMVBxlKRBTLMcpEGMZBjlMg5TpMUqkpQkxjHScqjlUgqSmOVJlHIYxhmIQyEGWkxyEOMpTEKY5SoQZhjHQUpzGWVChlKghymOZhDmQYpEnQcpDnOUpTGGY5jlMc5TkKUxzFOcxSmIkpToQU5DoMgyhmIhRimIYhykKU5jmQk6FHcUpjJIc5TFIU5UoIUwzGKUhjJScpilMhJDFKNBjHKkpUHMYhkHQNBUEQpSDHOghhkKYpRmKYpzGOYxjFIYpjleY53FGU5SmKYpiJMRDjlMpBiHKgyhoMU5iHUUqEKOg5CJOUg0GOcpiESghDkac6UESUyTHSUy0FOgqDGUU5TGMUyTHKUpkGMdBzpGlBCFMU5TmMY5UGQYyEmKQxCnMRZyGKYqDJKZBilOUx0HIZBTFKMxiJSg5TEYQxUJOkxDlOYZ0EKRCTHOUxknQQh1FKQpSIOdBUnMVQzHKQ5imWsxhlQpBSoKZCDMIk5zQMUpilOMxUGIUxiFMU6THOUplEIYxyFORR1mKZJTFKUoyrIYhTFKY6DmOkiEFKZJEKMYhitIgyyGKUxjkKgxTmMZBCjKQpDmScpjkQchyFQYhinQhIzKMQ=",
                "FIt/OMyEpQkikHIc6THKdKFHKNDDGIYpjGQtAzlKcpCmKxxjGMUxjHIUhyEKYpSlMZBimMYhyGKZBTEIUqipKcxioMcxFEKkpzGOUxTmWoxRnOQ50kKU5TnKNSzEMYpjGIUpDHOY5jEI05DnQcpUIMZiikSUxjmOgxCoQsZiMQc5jFOU5ElQU5EjQMpCnKgpSFKY5TsMUgzGGVBjoKkZynKUyCFGc5TlKlBTlOZCkEORKTFMcxTLKVBSlMUqDkKRRTuQQxTDMVRSIGYiTFSYijnMoiDGScxSlOZZkGIgqEmOg5SkMYqBEIUxzIKYaTlOUhilQUxynIQh0FMlBjHMgxylKcqCkQUhTFMUyEpKMqTHMcplHIUpCoIZBCoKQ5ilOcpzEQZBWmSQp0mQYhDkKUxjoMc50HOc5iKKQxymKUyTGKcpSlUYxTFOZBSkMVCSkOY50GKUxiqIUykGMQ5WIMZBjIMkxTHOVJiJUUpCHKY5EFIYZiHKkqCGQUximKUykGOoxzEQtBTmUQ5ilQZBjEKUZjmKcxTnUUpklMUpjEKUZTmKgpSlGUqCqSYp0lINIzEQkyCkOtAykOYxCkGZBkGUgiClMY6TGOg6EFOUylHQxBjkMYpCoMkxzpQgpTsOgqynKUhyoIY5THQVJUFOYhinKVRznMQ5yFIYxClMY5DFMcqDIKYxTjOQhjJMUxiHOYxjFMgxikMY5joOU6DGKc5FGMUpTjIZCCkMUhTJIU6SmKYpCkKUxzlKQqxnOQyTmKQg1KOcxxmMciSlIg5RlMY5hmKcxzIMU5TFKY5zINAxTIKU5kHKkySmMUxCHKYxzHIM5imOhhDGKYxkIIgiFlOkhTHIgxiJKYhUFSZBClKY5CHIkpCmKgxEKIgyDHKYpxnSUxTIMQxCIGYhTjKYxyHKQ5znOc5iEOk5TGMQxjGUQiykUgpilIZBjGKc5ymMYxVMOUxEHMYqTGQgqynMVpTKGkpjkOUozoMcZznMY5UFOY5DJSZBTkIcpSGIQxTnKUhUoMUhjoWUhTmMU5SmKc5iHOcxTFMU5CEKQ5EHWMpWqMgximUZBjKKQxjGIYpTGMYpClIQiTEKNSElKppDJOdBilKZBTEOcpinMc4ykMohymIYp0oMkpjFIcqjEOgqSEKU5UEQY5jGMg5ilWhJioSgxyoKk6iKKkpzmKciCoMVCSlKU5DmSg5znQUpSmMgxVlKVBzEIY5iFUU5ClMUxynKs5knQkxSFUUwzoKgzGFMUxipMQpzoGgxjnUUpFLMcxzmMepjGOUxEGKY5EGIdJkFKYZjJOZJTFKRJknKUximIQxTIIYpCmQoqUFUUxDEKRZiFQg5iGWYyDJIUpzHKZBCFIUpSmOYpjGMUhSlOpRSpKc5inIUiDIUUqCFMZJCpIcpTEKchTFSk6SEQYxyHMgqSoOUpTHQgyClUU5ylMghVkKYq0GMYpTEMVBEkMYySEOcyUGQUxTHUUhWFSVaDmKUZiHIYyxlMcpELIVJjlIYpUlQgxTlKU6EFKUpjmIQpzpMciEmMQxUlKYhSkOohSlMUxipOcqDnMQpClWc5jFMohCpKQ6ksOchjwKUxDkMUpylIRJjnMQxykKYpElQdB0FKY5TmWU6jGQVBjnIY5SqOYaDEOUZjGKgpjmMQZkFMdRTkKUxDnUhRDFMYxSlKQpkFIcqCGMgpjmQchEGig5FKMU=",
            ];

            let mut hll = HllInstance::new(2048).unwrap();

            for d in data {
                let s = base64::decode(d).unwrap();
                let h = HllInstance::read_hll_storage_spec(&s).unwrap();
                hll.merge_with(&h);
            }

            assert_eq!(hll.cardinality(), 260925);
        }
    }

    mod dense {
        use crate::instance::tests::TestingHll;
        use crate::instance::{number_of_buckets, DenseHll};
        use hex::FromHex;
        use std::hash::Hasher;
        use std::ops::Range;
        use twox_hash::XxHash64;

        fn bit_lengths() -> Range<u8> {
            return 4..17;
        }

        #[test]
        fn test_multiple_merges() {
            for prefix_bit_len in bit_lengths() {
                let mut single = DenseHll::new(prefix_bit_len);
                let mut merged = DenseHll::new(prefix_bit_len);

                let mut current = DenseHll::new(prefix_bit_len);

                // airlift loops i up to 10M, but too slow in debug builds.
                for i in 0..1_000_000 {
                    if i % 10_000 == 0 {
                        merged.merge_with(&current);
                        current = DenseHll::new(prefix_bit_len);
                    }
                    let mut hasher = XxHash64::default();
                    hasher.write_i32(i);
                    let h = hasher.finish();

                    current.insert_hash(h);
                    single.insert_hash(h);
                }

                merged.merge_with(&current);

                for i in 0..number_of_buckets(prefix_bit_len) {
                    assert_eq!(single.get_value(i), merged.get_value(i));
                }

                assert_eq!(single.cardinality(), merged.cardinality());
            }
        }

        #[test]
        fn test_high_cardinality() {
            for prefix_bit_len in bit_lengths() {
                let mut testing_hll = TestingHll::new(prefix_bit_len);
                let mut hll = DenseHll::new(prefix_bit_len);
                // airlift loops up to 10M here, but too slow in debug builds.
                for i in 0..1_000_000 {
                    let mut hasher = XxHash64::default();
                    hasher.write_i32(i);
                    let h = hasher.finish();

                    testing_hll.insert_hash(h);
                    hll.insert_hash(h);
                }

                for i in 0..testing_hll.buckets().len() {
                    assert_eq!(hll.get_value(i as u32), testing_hll.buckets()[i]);
                }
            }
        }

        #[test]
        fn test_insert() {
            for prefix_bit_len in bit_lengths() {
                let mut testing_hll = TestingHll::new(prefix_bit_len);
                let mut hll = DenseHll::new(prefix_bit_len);
                // airlift loops up to 20k here, but too slow in debug builds.
                for i in 0..1_000 {
                    let mut hasher = XxHash64::default();
                    hasher.write_i32(i);
                    let h = hasher.finish();

                    testing_hll.insert_hash(h);
                    hll.insert_hash(h);
                    hll.verify();
                }

                for i in 0..testing_hll.buckets().len() {
                    assert_eq!(hll.get_value(i as u32), testing_hll.buckets()[i]);
                }
            }
        }

        #[test]
        fn test_merge_with_overflows() {
            let mut testing_hll = TestingHll::new(12);
            let mut hll1 = DenseHll::new(12);
            let mut hll2 = DenseHll::new(12);

            // these two numbers cause overflows
            // TODO: ported directly from Java.
            //       Ensure hashes for the numbers are the same in rust implementation.
            let mut hasher1 = XxHash64::default();
            hasher1.write_i32(25130);
            let hash1 = hasher1.finish();

            let mut hasher2 = XxHash64::default();
            hasher2.write_i32(227291);
            let hash2 = hasher2.finish();

            hll1.insert_hash(hash1);
            testing_hll.insert_hash(hash1);

            hll2.insert_hash(hash2);
            testing_hll.insert_hash(hash2);

            hll1.merge_with(&hll2);
            hll1.verify();

            for i in 0..testing_hll.buckets().len() {
                assert_eq!(hll1.get_value(i as u32), testing_hll.buckets()[i]);
            }
        }

        #[test]
        fn test_merge() {
            for prefix_bit_len in bit_lengths() {
                // small, non-overlapping
                verify_merge(
                    prefix_bit_len,
                    0..100,
                    100..200,
                    "small, non-overlapping (1)",
                );
                verify_merge(
                    prefix_bit_len,
                    100..200,
                    0..100,
                    "small, non-overlapping (2)",
                );

                // small, overlapping
                verify_merge(prefix_bit_len, 0..100, 50..150, "small, overlapping (1)");
                verify_merge(prefix_bit_len, 50..150, 0..100, "small, overlapping (2)");

                // small, same
                verify_merge(prefix_bit_len, 0..100, 0..100, "small, same");

                // large, non-overlapping
                verify_merge(
                    prefix_bit_len,
                    0..20000,
                    20000..40000,
                    "large, non-overlapping (1)",
                );
                verify_merge(
                    prefix_bit_len,
                    20000..40000,
                    0..20000,
                    "large, non-overlapping (2)",
                );

                // airlift uses 10x larger set sizes, but too slow in debug builds.
                // large, overlapping
                verify_merge(
                    prefix_bit_len,
                    0..200_000,
                    100_000..300_000,
                    "large, overlapping (1)",
                );
                verify_merge(
                    prefix_bit_len,
                    100_000..300_000,
                    0..200_000,
                    "large, overlapping (2)",
                );

                // large, same
                verify_merge(prefix_bit_len, 0..200_000, 0..200_000, "large, same");
            }
        }

        fn verify_merge(prefix_bit_len: u8, one: Range<u64>, two: Range<u64>, descr: &str) {
            let mut hll1 = DenseHll::new(prefix_bit_len);
            let mut hll2 = DenseHll::new(prefix_bit_len);

            let mut expected = DenseHll::new(prefix_bit_len);

            for value in one {
                let mut hasher = XxHash64::default();
                hasher.write_u64(value);
                let h = hasher.finish();

                hll1.insert_hash(h);
                expected.insert_hash(h);
            }

            for value in two {
                let mut hasher = XxHash64::default();
                hasher.write_u64(value);
                let h = hasher.finish();

                hll2.insert_hash(h);
                expected.insert_hash(h);
            }

            hll1.verify();
            hll2.verify();

            hll1.merge_with(&hll2);
            hll1.verify();

            assert_eq!(
                hll1.cardinality(),
                expected.cardinality(),
                "bit_length is {}, on '{}' set",
                prefix_bit_len,
                descr
            );
            assert_eq!(
                hll1.write(),
                expected.write(),
                "bit_len is {}, on '{}' set",
                prefix_bit_len,
                descr
            );
        }

        #[test]
        fn test_dense_linear_counting() {
            // This HLL will use uses the linear counting code.
            let hll = DenseHll::read(&Vec::from_hex("0c004020000001000000000000000000000000000000000000050020000001030100000410000000004102100000000000000051000020000020003220000003102000000000001200042000000001000200000002000000100000030040000000010040003010000000000100002000000000000000000031000020000000000000000000100000200302000000000000000000001002000000000002204000000001000001000200400000000000001000020031100000000080000000002003000000100000000100110000000000000000000010000000000000000000000020000001320205000100000612000000000004100020100000000000000000001000000002200000100000001000001020000000000020000000000000001000010300060000010000000000070100003000000000000020000000000001000010000104000000000000000000101000100000001401000000000000000000000000000100010000000000000000000000000400020000000002002300010000000000040000041000200005100000000000001000000000100000203010000000000000000000000000001006000100000000000000300100001000100254200000000000101100040000000020000010000050000000501000000000101020000000010000000003000000000200000102100000000204007000000200010000033000000000061000000000000000000000000000000000100001000001000000013000000003000000000002000000000000010001000000000000000000020010000020000000100001000000000000001000103000000000000000000020020000001000000000100001000000000000000020220200200000001001000010100000000200000000000001000002000000011000000000101200000000000000000000000000000000000000100130000000000000000000100000120000300040000000002000000000000000000000100000000070000100000000301000000401200002020000000000601030001510000000000000110100000000000000000050000000010000100000000000000000100022000100000101054010001000000000000001000001000000002000000000100000000000021000001000002000000000100000000000000000000951000000100000000000000000000000000102000200000000000000010000010000000000100002000000000000000000010000000000000010000000010000000102010000000010520100000021010100000030000000000000000100000001000000022000330051000000100000000000040003020000010000020000100000013000000102020000000050000000020010000000000000000101200c000100000001200400000000010000001000000000100010000000001000001000000100000000010000000004000000002000013102000100000000000000000000000600000010000000000000020000000000001000000000030000000000000020000000001000001000000000010000003002000003000200070001001003030010000000003000000000000020000006000000000000000011000000010000200000000000500000000000000020500000000003000000000000000004000030000100000000103000001000000000000200002004200000020000000030000000000000000000000002000100000000000000002000000000000000010020101000000005250000010000000000023010000001000000000000500002001000123100030011000020001310600000000000021000023000003000000000000000001000000000000220200000000004040000020201000000010201000000000020000400010000050000000000000000000000010000020000000000000000000000000000000000102000010000000000000000000000002010000200200000000000000000000000000100000000000000000200400000000010000000000000000000000000000000010000200300000000000100110000000000000000000000000010000030000001000000000010000010200013000000000000200000001000001200010000000010000000000001000000000000100000000410000040000001000100010000100000002001010000000000000000001000000000000010000000000000000000000002000000000001100001000000001010000000000000002200000000004000000000000100010000000000600000000100300000000000000000000010000003000000000000000000310000010100006000010001000000000000001010101000100000000000000000000000000000201000000000000000700010000030000000000000021000000000000000001020000000030000100001000000000000000000000004010100000000000000000000004000000040100000040100100001000000000300000100000000010010000300000200000000000001302000000000000000000100100000400030000001001000100100002300000004030000002010000220100000000000002000000010010000000003010500000000300000000005020102000200000000000000020100000000000000000000000011000000023000000000010000101000000000000010020040200040000020000004000020000000001000000000100000200000010000000000030100010001000000100000000000600400000000002000000000000132000000900010000000030021400000000004100006000304000000000000010000106000001300020000")
                .unwrap()).unwrap();
            assert_eq!(hll.cardinality(), 655);
        }
    }
    // TODO: port tests for Sparse HLLs and HLLInstance.

    struct TestingHll {
        index_bit_length: u8,
        buckets: Vec<u32>,
    }

    impl TestingHll {
        pub fn new(index_bit_len: u8) -> TestingHll {
            return TestingHll {
                index_bit_length: index_bit_len,
                buckets: vec![0; number_of_buckets(index_bit_len) as usize],
            };
        }

        pub fn insert_hash(&mut self, hash: u64) {
            let index = compute_index(hash, self.index_bit_length) as usize;
            let value = compute_value(hash, self.index_bit_length);

            self.buckets[index] = max(self.buckets[index], value as u32);
        }

        pub fn buckets(&self) -> &[u32] {
            return &self.buckets;
        }
    }
}
