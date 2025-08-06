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

use crate::error::Result;
use crate::ZetaError;

/// Difference encoding can efficiently store sorted integers by storing only the difference
/// between them, rather than their absolute values. Since the deltas between values should be small,
/// the representation additionally compacts them by using varint encoding.
///
/// The encoder only supports writing positive integers in ascending order.
pub struct DifferenceEncoder<'l> {
    buf: &'l mut Vec<u8>,
    last: u32,
}

impl DifferenceEncoder<'_> {
    pub fn new(buf: &mut Vec<u8>) -> DifferenceEncoder<'_> {
        return DifferenceEncoder { buf, last: 0 };
    }

    /// Writes the integer value into the buffer using difference encoding.
    /// Panics f the integer is negative or smaller than the previous
    /// encoded value.
    pub fn put_int(&mut self, v: u32) {
        assert!(
            v >= self.last,
            "{} put after {} but values are required to be in ascending order",
            v,
            self.last
        );
        write_varint(self.buf, v - self.last);
        self.last = v;
    }
}

fn write_varint(buf: &mut Vec<u8>, mut v: u32) {
    loop {
        // Encode next 7 bits + terminator bit
        let bits = v & 0x7F;
        v >>= 7;
        let b = (bits + (if v != 0 { 0x80 } else { 0 })) as u8;
        buf.push(b);
        if v == 0 {
            break;
        }
    }
}

fn read_varint(data: &[u8]) -> Result<(/*result*/ u32, /*bytes read*/ usize)> {
    let mut result: u32 = 0;
    let mut shift = 0;
    let mut offset: usize = 0;
    loop {
        if 32 <= shift {
            return Err(ZetaError::new("varint too long"));
        }
        // Get 7 bits from next byte
        let b = data[offset];
        offset += 1;
        result |= (b as u32 & 0x7F) << shift;
        shift += 7;
        if (b & 0x80) == 0 {
            break;
        }
    }
    return Ok((result, offset));
}

#[derive(Debug, Clone, Copy)]
pub struct DifferenceDecoder<'l> {
    data: &'l [u8],
    last: u32,
}

impl DifferenceDecoder<'_> {
    pub fn new(data: &[u8]) -> DifferenceDecoder<'_> {
        return DifferenceDecoder { data, last: 0 };
    }
}

impl Iterator for DifferenceDecoder<'_> {
    type Item = Result<u32>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.data.is_empty() {
            return None;
        }
        match read_varint(self.data) {
            Ok((n, cnt)) => {
                self.data = &self.data[cnt..];
                self.last += n;
                return Some(Ok(self.last));
            }
            Err(e) => {
                self.data = &[]; // stop on error.
                return Some(Err(e));
            }
        }
    }
}
