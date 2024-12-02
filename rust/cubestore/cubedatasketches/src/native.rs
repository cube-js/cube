/*
 * Copyright 2024 Cube Dev, Inc.
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

pub use crate::error::Result;
use std::fmt::{Debug, Formatter};

use dsrs::{HLLSketch, HLLType, HLLUnion};

pub struct HLLDataSketch {
    pub(crate) instance: HLLSketch,
}

unsafe impl Send for HLLDataSketch {}
unsafe impl Sync for HLLDataSketch {}

impl Debug for HLLDataSketch {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HLLDataSketch")
            .field("instance", &"<hidden>");

        Ok(())
    }
}

impl HLLDataSketch {
    pub fn read(data: &[u8]) -> Result<Self> {
        return Ok(Self {
            instance: HLLSketch::deserialize(data)?,
        });
    }

    pub fn cardinality(&self) -> u64 {
        return self.instance.estimate().round() as u64;
    }

    pub fn get_lg_config_k(&self) -> u8 {
        return self.instance.get_lg_config_k();
    }

    pub fn write(&self) -> Vec<u8> {
        // TODO(ovr): Better way?
        self.instance.serialize().as_ref().iter().copied().collect()
    }
}

pub struct HLLUnionDataSketch {
    pub(crate) instance: HLLUnion,
}

impl Debug for HLLUnionDataSketch {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HLLUnionDataSketch")
            .field("instance", &"<hidden>");

        Ok(())
    }
}

unsafe impl Send for HLLUnionDataSketch {}
unsafe impl Sync for HLLUnionDataSketch {}

impl HLLUnionDataSketch {
    pub fn new(lg_max_k: u8) -> Result<Self> {
        Ok(Self {
            instance: HLLUnion::new(lg_max_k),
        })
    }

    pub fn get_lg_config_k(&self) -> u8 {
        return self.instance.get_lg_config_k();
    }

    pub fn write(&self) -> Vec<u8> {
        let sketch = self.instance.sketch(HLLType::HLL_4);
        // TODO(ovr): Better way?
        sketch.serialize().as_ref().iter().copied().collect()
    }

    pub fn merge_with(&mut self, other: HLLDataSketch) -> Result<()> {
        self.instance.merge(other.instance);

        Ok(())
    }

    /// Allocated size, not including size_of::<Self>().  Must be exact.
    pub fn allocated_size(&self) -> usize {
        // TODO upgrade DF: How should we (how can we) implement this?
        1
    }
}
