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

pub use crate::error::DataSketchesError;
pub use crate::error::Result;
use std::fmt::Debug;

#[derive(Debug)]
pub struct HLLDataSketch {}

unsafe impl Send for HLLDataSketch {}

unsafe impl Sync for HLLDataSketch {}

impl HLLDataSketch {
    pub fn read(_data: &[u8]) -> Result<Self> {
        Err(DataSketchesError::new("Not supported on Windows"))
    }

    pub fn cardinality(&self) -> u64 {
        unimplemented!();
    }

    pub fn get_lg_config_k(&self) -> u8 {
        unimplemented!();
    }

    pub fn write(&self) -> Vec<u8> {
        unimplemented!();
    }
}

#[derive(Debug)]
pub struct HLLUnionDataSketch {}

unsafe impl Send for HLLUnionDataSketch {}
unsafe impl Sync for HLLUnionDataSketch {}

impl HLLUnionDataSketch {
    pub fn new(_lg_max_k: u8) -> Result<Self> {
        Err(DataSketchesError::new("Not supported on Windows"))
    }

    pub fn get_lg_config_k(&self) -> u8 {
        unimplemented!();
    }

    pub fn write(&self) -> Vec<u8> {
        unimplemented!();
    }

    pub fn merge_with(&mut self, _other: HLLDataSketch) -> Result<()> {
        unimplemented!();
    }
}
