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
use std::fmt::{Display, Formatter};

pub type Result<T> = std::result::Result<T, DataSketchesError>;

#[derive(Debug)]
pub struct DataSketchesError {
    pub message: String,
}

impl Display for DataSketchesError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DataSketchesError: {}", self.message)
    }
}

impl DataSketchesError {
    pub fn new<Str: ToString>(message: Str) -> Self {
        return Self {
            message: message.to_string(),
        };
    }
}

impl From<std::io::Error> for DataSketchesError {
    fn from(err: std::io::Error) -> Self {
        return DataSketchesError::new(err);
    }
}

#[cfg(not(target_os = "windows"))]
impl From<dsrs::DataSketchesError> for DataSketchesError {
    fn from(err: dsrs::DataSketchesError) -> Self {
        return DataSketchesError::new(err);
    }
}
