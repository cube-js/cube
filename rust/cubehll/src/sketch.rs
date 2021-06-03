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

use crate::error::Result;
use crate::instance::HllInstance;

/// HyperLogLog sketch estimates a size of a set (i.e. the number of unique elements in it) without
/// storing all the elements in the set.
///
/// Port of the HyperLogLog from Airlift.
/// You can deserialize sketches produced by Airlift by using `read()`.
/// In fact, the code to add new elements has not been ported yet, so reading existing sketches is
/// the only way to produce non-empty sets at this point.
#[derive(Debug, Clone)]
pub struct HllSketch {
    instance: HllInstance,
}

impl HllSketch {
    /// Create a sketch for an empty set of elements.
    /// The number of buckets is a power of two, not more than 65536.
    pub fn new(num_buckets: u32) -> Result<HllSketch> {
        return Ok(HllSketch {
            instance: HllInstance::new(num_buckets)?,
        });
    }

    /// Maximum number of buckets used for this representation.
    pub fn num_buckets(&self) -> u32 {
        return self.instance.num_buckets();
    }

    pub fn index_bit_len(&self) -> u8 {
        return self.instance.index_bit_len();
    }

    pub fn read(data: &[u8]) -> Result<HllSketch> {
        return Ok(HllSketch {
            instance: HllInstance::read(data)?,
        });
    }

    /// Read from the snowflake JSON format, i.e. result of HLL_EXPORT serialized to string.
    pub fn read_snowflake(s: &str) -> Result<HllSketch> {
        return Ok(HllSketch {
            instance: HllInstance::read_snowflake(s)?,
        });
    }

    pub fn write(&self) -> Vec<u8> {
        return self.instance.write();
    }

    /// Produces an estimate of the current set size.
    pub fn cardinality(&self) -> u64 {
        return self.instance.cardinality();
    }

    /// Merges elements from `o` into the current sketch.
    /// Afterwards the current sketch estimates the size of the union.
    ///
    /// EXPECTS: `index_bit_len` of both sketches are the same.
    pub fn merge_with(&mut self, o: &HllSketch) {
        self.instance.merge_with(&o.instance);
    }
}
