use std::marker::PhantomData;

use datafusion::arrow::array::{new_null_array, Array, ArrayRef, DictionaryArray};
use datafusion::arrow::datatypes::{
    ArrowDictionaryKeyType, ArrowNativeType, DataType, Int16Type, Int32Type, Int64Type, Int8Type,
    UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use datafusion::dfschema::not_impl_err;
use datafusion::error::Result as DFResult;
use datafusion::physical_expr::binary_map::OutputType;
use datafusion::physical_plan::aggregates::group_values::multi_group_by::{
    ByteGroupValueBuilder, GroupColumn,
};

/// A [`GroupColumn`] for dictionary-encoded columns that stores the group values in their
/// decoded form (delegating to an inner byte-array builder) while accepting dictionary input.
///
/// Group storage operations (`append_val`/`equal_to`) only happen on group boundaries, so they
/// resolve the dictionary value on demand: a non-null row delegates to the inner builder using
/// `(dict.values(), dict.key(row))`, and a null row delegates against a cached single-null array.
/// The per-row hot path stays in `DictionaryComparator`, which never touches this builder.
pub struct DictionaryGroupColumn<K: ArrowDictionaryKeyType> {
    inner: Box<dyn GroupColumn>,
    /// One-element null array of the dictionary's value type, used to append/compare null keys.
    null_row: ArrayRef,
    _k: PhantomData<fn() -> K>,
}

impl<K: ArrowDictionaryKeyType> DictionaryGroupColumn<K> {
    fn new(inner: Box<dyn GroupColumn>, null_row: ArrayRef) -> Self {
        Self {
            inner,
            null_row,
            _k: PhantomData,
        }
    }

    #[inline]
    fn dict(column: &ArrayRef) -> &DictionaryArray<K> {
        column
            .as_any()
            .downcast_ref::<DictionaryArray<K>>()
            .expect("DictionaryGroupColumn got non-dictionary array")
    }
}

impl<K: ArrowDictionaryKeyType> GroupColumn for DictionaryGroupColumn<K> {
    fn equal_to(&self, lhs_row: usize, column: &ArrayRef, rhs_row: usize) -> bool {
        let dict = Self::dict(column);
        if dict.is_null(rhs_row) {
            self.inner.equal_to(lhs_row, &self.null_row, 0)
        } else {
            let key = dict.keys().value(rhs_row).as_usize();
            self.inner.equal_to(lhs_row, dict.values(), key)
        }
    }

    fn append_val(&mut self, column: &ArrayRef, row: usize) {
        let dict = Self::dict(column);
        if dict.is_null(row) {
            self.inner.append_val(&self.null_row, 0);
        } else {
            let key = dict.keys().value(row).as_usize();
            self.inner.append_val(dict.values(), key);
        }
    }

    fn vectorized_equal_to(
        &self,
        lhs_rows: &[usize],
        array: &ArrayRef,
        rhs_rows: &[usize],
        equal_to_results: &mut [bool],
    ) {
        for i in 0..lhs_rows.len() {
            if equal_to_results[i] {
                equal_to_results[i] = self.equal_to(lhs_rows[i], array, rhs_rows[i]);
            }
        }
    }

    fn vectorized_append(&mut self, array: &ArrayRef, rows: &[usize]) {
        for &row in rows {
            self.append_val(array, row);
        }
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn size(&self) -> usize {
        self.inner.size() + self.null_row.get_array_memory_size()
    }

    fn build(self: Box<Self>) -> ArrayRef {
        (*self).inner.build()
    }

    fn take_n(&mut self, n: usize) -> ArrayRef {
        self.inner.take_n(n)
    }
}

/// Builds a [`DictionaryGroupColumn`] for the given dictionary key/value types.
///
/// The inner builder stores the decoded value (Utf8/Binary); the wrapper is generic over the
/// key type so it can read keys without decoding the whole batch.
pub fn new_dictionary_group_column(
    key_type: &DataType,
    value_type: &DataType,
) -> DFResult<Box<dyn GroupColumn>> {
    let inner: Box<dyn GroupColumn> = match value_type {
        DataType::Utf8 => Box::new(ByteGroupValueBuilder::<i32>::new(OutputType::Utf8)),
        DataType::LargeUtf8 => Box::new(ByteGroupValueBuilder::<i64>::new(OutputType::Utf8)),
        DataType::Binary => Box::new(ByteGroupValueBuilder::<i32>::new(OutputType::Binary)),
        DataType::LargeBinary => Box::new(ByteGroupValueBuilder::<i64>::new(OutputType::Binary)),
        other => {
            return not_impl_err!(
                "dictionary value type {other} not supported in SortedGroupValues"
            )
        }
    };
    let null_row = new_null_array(value_type, 1);

    Ok(match key_type {
        DataType::Int8 => Box::new(DictionaryGroupColumn::<Int8Type>::new(inner, null_row)),
        DataType::Int16 => Box::new(DictionaryGroupColumn::<Int16Type>::new(inner, null_row)),
        DataType::Int32 => Box::new(DictionaryGroupColumn::<Int32Type>::new(inner, null_row)),
        DataType::Int64 => Box::new(DictionaryGroupColumn::<Int64Type>::new(inner, null_row)),
        DataType::UInt8 => Box::new(DictionaryGroupColumn::<UInt8Type>::new(inner, null_row)),
        DataType::UInt16 => Box::new(DictionaryGroupColumn::<UInt16Type>::new(inner, null_row)),
        DataType::UInt32 => Box::new(DictionaryGroupColumn::<UInt32Type>::new(inner, null_row)),
        DataType::UInt64 => Box::new(DictionaryGroupColumn::<UInt64Type>::new(inner, null_row)),
        other => {
            return not_impl_err!("dictionary key type {other} not supported in SortedGroupValues")
        }
    })
}
