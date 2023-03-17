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

use crate::error::Result;
use protobuf::rt::ProtobufVarint;
use protobuf::{wire_format, CodedInputStream, CodedOutputStream};
use std::convert::TryFrom;

/// This is actually an enum from .proto file. This is the only place that would require `protoc`,
/// so we choose to store the value directly and avoid a heavy dependency on the proto compiler.
pub type AggregatorType = i32;

/// Representation of a HyperLogLog++ state. In contrast to using just a normal protocol buffer
/// representation, this object has the advantage of providing a simpler interface for the values
/// relevant to HyperLogLog++ as well as fast and low-memory (aliased) parsing.
#[derive(Debug, Clone)]
pub struct State {
    /// See `AggregatorStateProto.getType`
    pub type_: AggregatorType,
    /// See `AggregatorStateProto.getNumValues`
    pub num_values: i64,
    /// See `AggregatorStateProto::getEncodingVersion`
    pub encoding_version: i32,
    /// See `AggregatorStateProto::getValueType`
    pub value_type: i32,

    /// Size of sparse list, i.e., how many different indexes are present in `sparse_data`.
    /// See `HyperLogLogPlusUniqueStateProto::getSparseSize`.
    pub sparse_size: i32,

    /// Precision / number of buckets for the normal representation.
    /// See `HyperLogLogPlusUniqueStateProto::getPrecisionOrNumBuckets`
    pub precision: i32,

    /// Precision / number of buckets for the sparse representation.
    /// See `HyperLogLogPlusUniqueStateProto::getSparsePrecisionOrNumBuckets`
    pub sparse_precision: i32,

    /// Normal data representation.
    /// See `HyperLogLogPlusUniqueStateProto::getData`
    pub data: Option<Vec<u8>>,

    /// Sparse data representation.
    /// See `HyperLogLogPlusUniqueStateProto::getSparseData`
    pub sparse_data: Option<Vec<u8>>,
}

impl Default for State {
    fn default() -> Self {
        return State {
            type_: DEFAULT_TYPE,
            num_values: DEFAULT_NUM_VALUES,
            encoding_version: DEFAULT_ENCODING_VERSION,
            value_type: DEFAULT_VALUE_TYPE,
            sparse_size: DEFAULT_SPARSE_SIZE,
            precision: DEFAULT_PRECISION_OR_NUM_BUCKETS,
            sparse_precision: DEFAULT_SPARSE_PRECISION_OR_NUM_BUCKETS,
            data: None,
            sparse_data: None,
        };
    }
}

pub mod aggregator_state_proto {
    pub const TYPE_FIELD_NUMBER: u32 = 1;
    pub const NUM_VALUES_FIELD_NUMBER: u32 = 2;
    pub const ENCODING_VERSION_FIELD_NUMBER: u32 = 3;
    pub const VALUE_TYPE_FIELD_NUMBER: u32 = 4;
    pub const HYPERLOGLOGPLUS_UNIQUE_STATE_FIELD_NUMBER: u32 = 112;

    pub const AGGREGATOR_TYPE_HYPERLOGLOG_PLUS_UNIQUE: i32 = 112;
}

// Protocol buffer tags consist of the field number concatenated with the field type. Because we
// use these in case statements below, they must be constant expressions and the bitshift can not
// be refactored into a method.
const TYPE_TAG: u32 =
    aggregator_state_proto::TYPE_FIELD_NUMBER << 3 | wire_format::WireType::WireTypeVarint as u32;
const NUM_VALUES_TAG: u32 = aggregator_state_proto::NUM_VALUES_FIELD_NUMBER << 3
    | wire_format::WireType::WireTypeVarint as u32;
const ENCODING_VERSION_TAG: u32 = aggregator_state_proto::ENCODING_VERSION_FIELD_NUMBER << 3
    | wire_format::WireType::WireTypeVarint as u32;
const VALUE_TYPE_TAG: u32 = aggregator_state_proto::VALUE_TYPE_FIELD_NUMBER << 3
    | wire_format::WireType::WireTypeVarint as u32;
const HYPERLOGLOGPLUS_UNIQUE_STATE_TAG: u32 =
    aggregator_state_proto::HYPERLOGLOGPLUS_UNIQUE_STATE_FIELD_NUMBER << 3
        | wire_format::WireType::WireTypeLengthDelimited as u32;

const DEFAULT_TYPE: AggregatorType =
    aggregator_state_proto::AGGREGATOR_TYPE_HYPERLOGLOG_PLUS_UNIQUE;
const DEFAULT_NUM_VALUES: i64 = 0;
const DEFAULT_ENCODING_VERSION: i32 = 1;
const DEFAULT_VALUE_TYPE: i32 = 0;

pub mod hpp_unique_proto {
    pub const SPARSE_SIZE_FIELD_NUMBER: u32 = 2;
    pub const PRECISION_OR_NUM_BUCKETS_FIELD_NUMBER: u32 = 3;
    pub const SPARSE_PRECISION_OR_NUM_BUCKETS_FIELD_NUMBER: u32 = 4;
    pub const DATA_FIELD_NUMBER: u32 = 5;
    pub const SPARSE_DATA_FIELD_NUMBER: u32 = 6;
}

const SPARSE_SIZE_TAG: u32 =
    hpp_unique_proto::SPARSE_SIZE_FIELD_NUMBER << 3 | wire_format::WireType::WireTypeVarint as u32;
const PRECISION_OR_NUM_BUCKETS_TAG: u32 = hpp_unique_proto::PRECISION_OR_NUM_BUCKETS_FIELD_NUMBER
    << 3
    | wire_format::WireType::WireTypeVarint as u32;
const SPARSE_PRECISION_OR_NUM_BUCKETS_TAG: u32 =
    hpp_unique_proto::SPARSE_PRECISION_OR_NUM_BUCKETS_FIELD_NUMBER << 3
        | wire_format::WireType::WireTypeVarint as u32;
const DATA_TAG: u32 = hpp_unique_proto::DATA_FIELD_NUMBER << 3
    | wire_format::WireType::WireTypeLengthDelimited as u32;
const SPARSE_DATA_TAG: u32 = hpp_unique_proto::SPARSE_DATA_FIELD_NUMBER << 3
    | wire_format::WireType::WireTypeLengthDelimited as u32;

const DEFAULT_SPARSE_SIZE: i32 = 0;
const DEFAULT_PRECISION_OR_NUM_BUCKETS: i32 = 0;
const DEFAULT_SPARSE_PRECISION_OR_NUM_BUCKETS: i32 = 0;

impl State {
    // TODO: remove, change data from Option<> to Vec<>
    pub fn has_data(&self) -> bool {
        return self.data.is_some() && !self.data.as_ref().unwrap().is_empty();
    }

    /// Parses a serialized HyperLogLog++ `AggregatorStateProto` and populates this object's
    /// fields.
    ///
    /// Fails of the stream does not contain a serialized `AggregatorStateProto` or if fields are set
    /// that would typically not belong
    pub fn parse_stream(mut input: CodedInputStream) -> Result<State> {
        let mut s = State::default();

        while !input.eof()? {
            let tag = input.read_tag()?;
            let (_, wire_type) = tag.unpack();
            match tag.value() {
                TYPE_TAG => s.type_ = input.read_int32()?,
                NUM_VALUES_TAG => s.num_values = input.read_int64()?,
                ENCODING_VERSION_TAG => s.encoding_version = input.read_int32()?,
                VALUE_TYPE_TAG => s.value_type = input.read_int32()?, // TODO: ValueType.forNumber(input.readEnum()),
                HYPERLOGLOGPLUS_UNIQUE_STATE_TAG => {
                    let size = u32::try_from(input.read_int32()?)?;
                    Self::parse_hll(&mut s, &mut input, size)?;
                }
                _ => input.skip_field(wire_type)?,
            }
        }

        return Ok(s);
    }

    /// Parses a `HyperLogLogPlusUniqueStateProto` message. Since the message is nested within an
    /// `AggregatorStateProto`, we limit ourselves to reading only the bytes of the specified
    /// message length.
    fn parse_hll(s: &mut State, input: &mut CodedInputStream, size: u32) -> Result<()> {
        let limit = input.pos() + size as u64;

        while input.pos() < limit && !input.eof()? {
            let tag = input.read_tag()?;
            let (_, wire_type) = tag.unpack();
            match tag.value() {
                SPARSE_SIZE_TAG => s.sparse_size = input.read_int32()?,
                PRECISION_OR_NUM_BUCKETS_TAG => s.precision = input.read_int32()?,
                SPARSE_PRECISION_OR_NUM_BUCKETS_TAG => s.sparse_precision = input.read_int32()?,
                DATA_TAG => s.data = Some(input.read_bytes()?),
                SPARSE_DATA_TAG => s.sparse_data = Some(input.read_bytes()?),
                _ => input.skip_field(wire_type)?,
            }
        }
        return Ok(());
    }

    pub fn to_byte_array(&self) -> Vec<u8> {
        let (size, hll_size) = self.get_serialized_size();
        let mut result = vec![0; size as usize];
        let mut output = CodedOutputStream::bytes(result.as_mut_slice());
        self.write_to(hll_size, &mut output);
        output.check_eof();
        return result;
    }

    fn write_to(&self, hll_size: u32, stream: &mut CodedOutputStream) {
        // We use the NoTag write methods for consistency with the parsing functions and for
        // consistency with the variable-length writes where we can't use any convenience function.
        stream.write_uint32_no_tag(TYPE_TAG).unwrap();
        stream.write_int32_no_tag(self.type_).unwrap();

        stream.write_uint32_no_tag(NUM_VALUES_TAG).unwrap();
        stream.write_int64_no_tag(self.num_values).unwrap();

        if self.encoding_version != DEFAULT_ENCODING_VERSION {
            stream.write_uint32_no_tag(ENCODING_VERSION_TAG).unwrap();
            stream.write_int32_no_tag(self.encoding_version).unwrap();
        }

        if self.value_type != DEFAULT_VALUE_TYPE {
            stream.write_uint32_no_tag(VALUE_TYPE_TAG).unwrap();
            stream.write_enum_no_tag(self.value_type).unwrap();
        }

        stream
            .write_uint32_no_tag(HYPERLOGLOGPLUS_UNIQUE_STATE_TAG)
            .unwrap();
        stream.write_uint32_no_tag(hll_size).unwrap();
        self.write_hll_to(stream);
    }

    fn write_hll_to(&self, stream: &mut CodedOutputStream) {
        // We use the NoTag write methods for consistency with the parsing functions and for
        // consistency with the variable-length writes where we can't use any convenience function.
        if self.sparse_size != DEFAULT_SPARSE_SIZE {
            stream.write_uint32_no_tag(SPARSE_SIZE_TAG).unwrap();
            stream.write_int32_no_tag(self.sparse_size).unwrap();
        }

        if self.precision != DEFAULT_PRECISION_OR_NUM_BUCKETS {
            stream
                .write_uint32_no_tag(PRECISION_OR_NUM_BUCKETS_TAG)
                .unwrap();
            stream.write_int32_no_tag(self.precision).unwrap();
        }

        if self.sparse_precision != DEFAULT_SPARSE_PRECISION_OR_NUM_BUCKETS {
            stream
                .write_uint32_no_tag(SPARSE_PRECISION_OR_NUM_BUCKETS_TAG)
                .unwrap();
            stream.write_int32_no_tag(self.sparse_precision).unwrap();
        }

        // Static analysis can not verify that stream.writeUInt32NoTag does not null out this.data
        if let Some(data) = &self.data {
            stream.write_uint32_no_tag(DATA_TAG).unwrap();
            stream.write_bytes_no_tag(data).unwrap();
        }

        // Static analysis can not verify that stream.writeUInt32NoTag does not null out this.sparse_data
        if let Some(sparse_data) = &self.sparse_data {
            stream.write_uint32_no_tag(SPARSE_DATA_TAG).unwrap();
            stream.write_bytes_no_tag(sparse_data).unwrap();
        }
    }

    fn get_serialized_size(&self) -> (/*size*/ u32, /*hll size*/ u32) {
        let mut size = 0;

        size += TYPE_TAG.len_varint();
        size += self.type_.len_varint();

        size += NUM_VALUES_TAG.len_varint();
        size += self.num_values.len_varint();

        if self.encoding_version != DEFAULT_ENCODING_VERSION {
            size += ENCODING_VERSION_TAG.len_varint();
            size += self.encoding_version.len_varint();
        }

        if self.value_type != DEFAULT_VALUE_TYPE {
            size += VALUE_TYPE_TAG.len_varint();
            size += self.value_type.len_varint();
        }

        let hll_size = self.get_serialized_hll_size();
        size += HYPERLOGLOGPLUS_UNIQUE_STATE_TAG.len_varint();
        size += hll_size.len_varint();
        size += hll_size;

        return (size, hll_size);
    }

    fn get_serialized_hll_size(&self) -> u32 {
        let mut size = 0;

        if self.sparse_size != DEFAULT_SPARSE_SIZE {
            size += SPARSE_SIZE_TAG.len_varint();
            size += self.sparse_size.len_varint();
        }

        if self.precision != DEFAULT_PRECISION_OR_NUM_BUCKETS {
            size += PRECISION_OR_NUM_BUCKETS_TAG.len_varint();
            size += self.precision.len_varint();
        }

        if self.sparse_precision != DEFAULT_SPARSE_PRECISION_OR_NUM_BUCKETS {
            size += SPARSE_PRECISION_OR_NUM_BUCKETS_TAG.len_varint();
            size += self.sparse_precision.len_varint();
        }

        if let Some(data) = &self.data {
            size += DATA_TAG.len_varint();
            size += (data.len() as u32).len_varint();
            size += data.len() as u32;
        }

        if let Some(sparse_data) = &self.sparse_data {
            size += SPARSE_DATA_TAG.len_varint();
            size += (sparse_data.len() as u32).len_varint();
            size += sparse_data.len() as u32;
        }

        return size;
    }
}
