/*
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
use protobuf::ProtobufError;
use std::fmt::{Display, Formatter};
use std::num::TryFromIntError;

pub type Result<T> = std::result::Result<T, ZetaError>;

#[derive(Debug)]
pub struct ZetaError {
    pub message: String,
}

impl Display for ZetaError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl ZetaError {
    pub fn new<Str: ToString>(message: Str) -> ZetaError {
        return ZetaError {
            message: message.to_string(),
        };
    }
}

impl From<std::io::Error> for ZetaError {
    fn from(err: std::io::Error) -> Self {
        return ZetaError::new(err);
    }
}

impl From<ProtobufError> for ZetaError {
    fn from(err: ProtobufError) -> Self {
        return ZetaError::new(err);
    }
}

impl From<TryFromIntError> for ZetaError {
    fn from(err: TryFromIntError) -> Self {
        return ZetaError::new(err);
    }
}
