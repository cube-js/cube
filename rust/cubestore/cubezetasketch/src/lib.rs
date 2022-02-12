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
mod data;
mod difference_encoding;
mod encoding;
mod error;
mod normal;
mod sketch;
mod sparse;
mod state;

pub use error::Result;
pub use error::ZetaError;
pub use sketch::HyperLogLogPlusPlus;
