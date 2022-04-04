/*
 * Copyright 2021 Cube Dev, Inc.
 * Copyright 2019 Google LLC
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
/// Encapsulates the encoding and decoding of singular HyperLogLog++ values. In particular, this
/// module implements:
///    - Retrieval of HyperLogLog++ properties such as the index and the *ρ(w)* of a
///      uniform hash of the input value.
///    - Encoding and decoding of HyperLogLog++ sparse values.
use std::cmp::max;

/// Computes HyperLogLog++ properties for the normal encoding at a given precision.
#[derive(Debug, Clone)]
pub struct NormalEncoding {
    pub precision: i32,
}

impl NormalEncoding {
    pub fn new(precision: i32) -> NormalEncoding {
        assert!(1 <= precision && precision <= 63,
         "valid index and rhoW can only be determined for precisions in the range [1, 63], but got {}", precision);
        return NormalEncoding { precision };
    }
}

/// An object that computes HyperLogLog++ properties for the sparse encoding at a given precision.
///
/// Sparse values take one of two different representations depending on whether the normal
/// *ρ(w)* can be determined from the lowest *sp-p* bits of the sparse index or
/// not. We use an (appropriately 0 padded) flag to indicate when the encoding includes an explicit
/// sparse *ρ(w')*:
///
/// <pre>
///   +---+-------------------+-----------------------------+
///   | 0 |      padding      |        sparse index         |
///   +---+-------------------+-----------------------------+
///        max(0, p+6-sp) bits           sp bits
///
///
///   +---+-------------------+-------------------+---------+
///   | 1 |      padding      |    normal index   |  rhoW'  |
///   +---+-------------------+-------------------+---------+
///        max(0, sp-p-6) bits       p bits         6 bits
/// </pre>
///
/// Note the subtle difference in nomenclature between *ρ(w)* for the number of
/// leading zero bits + 1 relative to the *normal* precision and *ρ(w')* for the
/// number of leading zero bits + 1 relative to the *sparse* precision. See the HLL++ paper
/// (https://goo.gl/pc916Z) for details.
#[derive(Debug, Clone)]
pub struct SparseEncoding {
    pub normal_precision: i32,
    pub sparse_precision: i32,
    /// Flag used to indicate whether a particular value is *ρ(w')* encoded or not. The
    /// position of the flag depends on the normal and sparse precisions. We store it here to avoid
    /// having to recompute it every time we encode or decode a value.
    rho_encoded_flag: i32,
}

impl SparseEncoding {
    /// The number of bits used to encode the sparse *ρ(w')* in the ρ-encoded form.
    const RHOW_BITS: i32 = 6;

    /// Mask for isolating the *ρ(w')* value in a ρ-encoded sparse value.
    const RHOW_MASK: i32 = (1 << Self::RHOW_BITS) - 1;

    pub fn new(normal_precision: i32, sparse_precision: i32) -> SparseEncoding {
        // We want the sparse values to be sorted consistently independent of whether an
        // implementation uses signed or unsigned integers. The upper limit for the normal precision
        // is therefore 31 - RHOW_BITS - 1 (for flag).
        assert!(
            1 <= normal_precision && normal_precision <= 24,
            "normal precision must be between 1 and 24 (inclusive), got {}",
            normal_precision
        );
        // While for the sparse precision it is 31 - 1 (for flag).
        assert!(
            1 <= sparse_precision && sparse_precision <= 30,
            "sparse precision must be between 1 and 30 (inclusive), got {}",
            sparse_precision
        );
        assert!(sparse_precision >= normal_precision
                , "sparse precision must be larger than or equal to the normal precision, normal: {}, sparse: {}", normal_precision, sparse_precision);

        // The position of the flag needs to be larger than any bits that could be used in the rhoW or
        // non-rhoW encoded values so that (a) the two values can be distinguished and (b) they will
        // not interleave when sorted numerically.
        let rho_encoded_flag = 1 << max(sparse_precision, normal_precision + Self::RHOW_BITS);
        return SparseEncoding {
            normal_precision,
            sparse_precision,
            rho_encoded_flag,
        };
    }

    /// Checks whether a sparse encoding is compatible with another.
    /// Only exactly the same encodings are compatible in our fork, ZetaSketch is more permissive.
    pub fn assert_compatible(&self, other: &SparseEncoding) {
        assert!(
            self.normal_precision == other.normal_precision
                && self.sparse_precision == other.sparse_precision,
            "Precisions (p={}, sp={}) are not compatible to (p={}, sp={})",
            self.normal_precision,
            self.sparse_precision,
            other.normal_precision,
            other.sparse_precision
        );
    }

    /// Decodes the sparse index from an encoded sparse value. See the class Javadoc for details on
    /// the two representations with which sparse values are encoded.
    pub(crate) fn decode_sparse_index(&self, sparse_value: i32) -> i32 {
        // If the sparse rhoW' is not encoded, then the value consists of just the sparse index.
        if (sparse_value & self.rho_encoded_flag) == 0 {
            return sparse_value as i32;
        }

        // When the sparse rhoW' is encoded, this indicates that the last sp-p bits of the sparse
        // index were all zero. We return the normal index right zero padded by sp-p bits since the
        // sparse index is just the normal index without the trailing zeros.
        return ((sparse_value ^ self.rho_encoded_flag) // Strip the encoding flag.
            >> Self::RHOW_BITS) // Strip the rhoW'
        // Shift the normal index to sparse index length.
        << (self.sparse_precision - self.normal_precision);
    }

    /// Decodes the normal index from an encoded sparse value. See the class Javadoc for details on
    /// the two representations with which sparse values are encoded.
    pub fn decode_normal_index(&self, sparse_value: i32) -> i32 {
        // Values without a sparse rhoW' consist of just the sparse index, so the normal index is
        // determined by stripping off the last sp-p bits.
        if (sparse_value & self.rho_encoded_flag) == 0 {
            return sparse_value >> (self.sparse_precision - self.normal_precision);
        }

        // Sparse rhoW' encoded values contain a normal index so we extract it by stripping the flag
        // off the front and the rhoW' off the end.
        return (sparse_value ^ self.rho_encoded_flag) >> Self::RHOW_BITS;
    }

    /// Decodes the normal *ρ(w)* from an encoded sparse value. See the class Javadoc for
    /// details on the two representations with which sparse values are encoded.
    pub fn decode_normal_rho_w(&self, sparse_value: i32) -> u8 {
        // If the rhoW' was not encoded, we can determine the normal rhoW from the last sp-p bits of
        // the sparse index.
        if (sparse_value & self.rho_encoded_flag) == 0 {
            return compute_rho_w(
                sparse_value as u64,
                self.sparse_precision - self.normal_precision,
            );
        }

        // If the sparse rhoW' was encoded, this tells us that the last sp-p bits of the
        // sparse index where all zero. The normal rhoW is therefore rhoW' + sp - p.
        return ((sparse_value & Self::RHOW_MASK) + self.sparse_precision - self.normal_precision)
            as u8;
    }
}

/// Returns the number of leading zeros + 1 in the lower n {@code bits} of the value.
fn compute_rho_w(value: u64, bits: i32) -> u8 {
    // Strip of the index and move the rhoW to a higher order.
    let w = value << (64 - bits);

    // If the rhoW consists only of zeros, return the maximum length of bits + 1.
    return if w == 0 {
        bits as u8 + 1
    } else {
        w.leading_zeros() as u8 + 1
    };
}
