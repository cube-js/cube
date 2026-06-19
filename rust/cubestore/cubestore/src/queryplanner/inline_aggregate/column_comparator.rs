use datafusion::arrow::array::*;
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::*;
use std::cmp::Ordering;
use std::marker::PhantomData;

/// Trait for comparing adjacent rows in an array to detect group boundaries.
/// Used in sorted group-by operations to efficiently find where groups change.
pub trait ColumnComparator: Send + Sync {
    /// Compare adjacent rows in the column, updating `equal_results`.
    ///
    /// For each index i in 0..equal_results.len():
    /// - If equal_results[i] is true, compares row[i] with row[i+1]
    /// - Sets equal_results[i] to false if rows differ (group boundary)
    /// - Leaves equal_results[i] unchanged if already false (short-circuit)
    fn compare_adjacent(&self, col: &ArrayRef, equal_results: &mut [bool]);
}

/// Comparator for primitive types (integers, floats, decimals, dates, timestamps).
///
/// Uses const generic NULLABLE parameter to eliminate null-checking overhead
/// for NOT NULL columns at compile time.
pub struct PrimitiveComparator<T: ArrowPrimitiveType, const NULLABLE: bool>
where
    T::Native: PartialEq,
    T: Send + Sync,
{
    _phantom: PhantomData<T>,
}

impl<T: ArrowPrimitiveType, const NULLABLE: bool> PrimitiveComparator<T, NULLABLE>
where
    T::Native: PartialEq,
    T: Send + Sync,
{
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<T: ArrowPrimitiveType, const NULLABLE: bool> ColumnComparator
    for PrimitiveComparator<T, NULLABLE>
where
    T::Native: PartialEq,
    T: Send + Sync,
{
    #[inline]
    fn compare_adjacent(&self, col: &ArrayRef, equal_results: &mut [bool]) {
        let array = col.as_primitive::<T>();

        let values = array.values();

        if NULLABLE {
            // Nullable column - check if there are actually any nulls
            if array.null_count() == 0 {
                // Fast path: column is nullable but this batch has no nulls
                for i in 0..equal_results.len() {
                    if equal_results[i] {
                        equal_results[i] = values[i] == values[i + 1];
                    }
                }
            } else {
                // Slow path: need to check null bitmap
                let nulls = array.nulls().expect("null_count > 0 but no nulls bitmap");
                for i in 0..equal_results.len() {
                    if equal_results[i] {
                        let null1 = nulls.is_null(i);
                        let null2 = nulls.is_null(i + 1);

                        // Both must be null or both must be non-null with equal values
                        equal_results[i] =
                            (null1 == null2) && (null1 || values[i] == values[i + 1]);
                    }
                }
            }
        } else {
            // NOT NULL column - no null checks needed, compiler will optimize this aggressively
            for i in 0..equal_results.len() {
                if equal_results[i] {
                    equal_results[i] = values[i] == values[i + 1];
                }
            }
        }
    }
}

/// Comparator for byte array types (Utf8, LargeUtf8, Binary, LargeBinary).
///
/// Uses generic over ByteArrayType to handle both i32 and i64 offset variants.
pub struct ByteArrayComparator<T: ByteArrayType + Send + Sync, const NULLABLE: bool> {
    _phantom: PhantomData<T>,
}

impl<T: ByteArrayType + Send + Sync, const NULLABLE: bool> ByteArrayComparator<T, NULLABLE> {
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<T: ByteArrayType + Send + Sync, const NULLABLE: bool> ColumnComparator
    for ByteArrayComparator<T, NULLABLE>
where
    T::Native: PartialEq,
{
    #[inline]
    fn compare_adjacent(&self, col: &ArrayRef, equal_results: &mut [bool]) {
        let array = col.as_bytes::<T>();

        if NULLABLE {
            if array.null_count() == 0 {
                // Fast path: no nulls in this batch
                for i in 0..equal_results.len() {
                    if equal_results[i] {
                        equal_results[i] = array.value(i) == array.value(i + 1);
                    }
                }
            } else {
                // Use iterator which handles nulls efficiently
                let iter1 = array.iter();
                let iter2 = array.iter().skip(1);

                for (i, (v1, v2)) in iter1.zip(iter2).enumerate() {
                    if equal_results[i] {
                        equal_results[i] = v1 == v2;
                    }
                }
            }
        } else {
            // NOT NULL column - direct value comparison
            for i in 0..equal_results.len() {
                if equal_results[i] {
                    equal_results[i] = array.value(i) == array.value(i + 1);
                }
            }
        }
    }
}

/// Comparator for ByteView types (Utf8View, BinaryView).
///
/// ByteView arrays store short strings (<=12 bytes) inline, allowing fast comparison
/// of the view value before comparing full string data.
pub struct ByteViewComparator<T: ByteViewType + Send + Sync, const NULLABLE: bool> {
    _phantom: PhantomData<T>,
}

impl<T: ByteViewType + Send + Sync, const NULLABLE: bool> ByteViewComparator<T, NULLABLE> {
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<T: ByteViewType + Send + Sync, const NULLABLE: bool> ColumnComparator
    for ByteViewComparator<T, NULLABLE>
where
    T::Native: PartialEq,
{
    #[inline]
    fn compare_adjacent(&self, col: &ArrayRef, equal_results: &mut [bool]) {
        let array = col.as_byte_view::<T>();

        if NULLABLE {
            if array.null_count() == 0 {
                // Fast path: no nulls
                for i in 0..equal_results.len() {
                    if equal_results[i] {
                        equal_results[i] = array.value(i) == array.value(i + 1);
                    }
                }
            } else {
                // Handle nulls via iterator
                let iter1 = array.iter();
                let iter2 = array.iter().skip(1);

                for (i, (v1, v2)) in iter1.zip(iter2).enumerate() {
                    if equal_results[i] {
                        equal_results[i] = v1 == v2;
                    }
                }
            }
        } else {
            // NOT NULL column
            for i in 0..equal_results.len() {
                if equal_results[i] {
                    equal_results[i] = array.value(i) == array.value(i + 1);
                }
            }
        }
    }
}

/// Comparator for dictionary-encoded columns (e.g. `Dictionary(Int32, Utf8)`).
///
/// The hot path compares dictionary keys (small integers) instead of the underlying
/// values. Within a single batch all rows share one dictionary, so key equality implies
/// value equality. The reverse does not hold when a dictionary carries duplicate values
/// (e.g. after a merge unions several local dictionaries), so when adjacent keys differ we
/// fall back to comparing the actual values to avoid splitting a group incorrectly. That
/// fallback only fires on group boundaries, which are rare in a sorted stream.
pub struct DictionaryComparator<K: ArrowDictionaryKeyType, const NULLABLE: bool> {
    _phantom: PhantomData<fn() -> K>,
}

impl<K: ArrowDictionaryKeyType, const NULLABLE: bool> DictionaryComparator<K, NULLABLE> {
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<K: ArrowDictionaryKeyType, const NULLABLE: bool> ColumnComparator
    for DictionaryComparator<K, NULLABLE>
{
    #[inline]
    fn compare_adjacent(&self, col: &ArrayRef, equal_results: &mut [bool]) {
        let array = col
            .as_any()
            .downcast_ref::<DictionaryArray<K>>()
            .expect("DictionaryComparator got non-dictionary array");
        let keys = array.keys();
        let values = array.values();

        if !NULLABLE {
            // A non-nullable field must not carry null keys; the loop below skips null checks.
            debug_assert_eq!(
                keys.null_count(),
                0,
                "DictionaryComparator<_, false> received null keys"
            );
        }

        // Built lazily, only when adjacent keys actually differ. The values array is always one
        // of the types accepted by `new_dictionary_group_column`, all of which `make_comparator`
        // supports.
        let mut value_cmp: Option<DynComparator> = None;

        for i in 0..equal_results.len() {
            if !equal_results[i] {
                continue;
            }

            if NULLABLE {
                let null1 = keys.is_null(i);
                let null2 = keys.is_null(i + 1);
                if null1 || null2 {
                    // Both null => same group; one null => boundary.
                    equal_results[i] = null1 && null2;
                    continue;
                }
            }

            let k1 = keys.value(i).as_usize();
            let k2 = keys.value(i + 1).as_usize();
            if k1 == k2 {
                continue;
            }

            let cmp = value_cmp.get_or_insert_with(|| {
                make_comparator(values.as_ref(), values.as_ref(), SortOptions::default())
                    .expect("make_comparator for dictionary values")
            });
            equal_results[i] = cmp(k1, k2) == Ordering::Equal;
        }
    }
}

/// Instantiate a primitive comparator and push it into the vector.
///
/// Handles const generic NULLABLE parameter based on field nullability.
#[macro_export]
macro_rules! instantiate_primitive_comparator {
    ($v:expr, $nullable:expr, $t:ty) => {
        if $nullable {
            $v.push(Box::new(
                $crate::queryplanner::inline_aggregate::column_comparator::PrimitiveComparator::<
                    $t,
                    true,
                >::new(),
            ) as _)
        } else {
            $v.push(Box::new(
                $crate::queryplanner::inline_aggregate::column_comparator::PrimitiveComparator::<
                    $t,
                    false,
                >::new(),
            ) as _)
        }
    };
}

/// Instantiate a byte array comparator and push it into the vector.
#[macro_export]
macro_rules! instantiate_byte_array_comparator {
    ($v:expr, $nullable:expr, $t:ty) => {
        if $nullable {
            $v.push(Box::new(
                $crate::queryplanner::inline_aggregate::column_comparator::ByteArrayComparator::<
                    $t,
                    true,
                >::new(),
            ) as _)
        } else {
            $v.push(Box::new(
                $crate::queryplanner::inline_aggregate::column_comparator::ByteArrayComparator::<
                    $t,
                    false,
                >::new(),
            ) as _)
        }
    };
}

/// Instantiate a byte view comparator and push it into the vector.
#[macro_export]
macro_rules! instantiate_byte_view_comparator {
    ($v:expr, $nullable:expr, $t:ty) => {
        if $nullable {
            $v.push(Box::new(
                $crate::queryplanner::inline_aggregate::column_comparator::ByteViewComparator::<
                    $t,
                    true,
                >::new(),
            ) as _)
        } else {
            $v.push(Box::new(
                $crate::queryplanner::inline_aggregate::column_comparator::ByteViewComparator::<
                    $t,
                    false,
                >::new(),
            ) as _)
        }
    };
}

/// Instantiate a dictionary comparator and push it into the vector.
#[macro_export]
macro_rules! instantiate_dictionary_comparator {
    ($v:expr, $nullable:expr, $k:ty) => {
        if $nullable {
            $v.push(Box::new(
                $crate::queryplanner::inline_aggregate::column_comparator::DictionaryComparator::<
                    $k,
                    true,
                >::new(),
            ) as _)
        } else {
            $v.push(Box::new(
                $crate::queryplanner::inline_aggregate::column_comparator::DictionaryComparator::<
                    $k,
                    false,
                >::new(),
            ) as _)
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn run(comparator: &dyn ColumnComparator, col: &ArrayRef) -> Vec<bool> {
        let n = col.len();
        let mut eq = vec![true; n - 1];
        comparator.compare_adjacent(col, &mut eq);
        eq
    }

    #[test]
    fn dict_compare_same_dictionary_sorted() {
        // values [a,b,c], keys [0,0,1,1,2] => rows a,a,b,b,c
        let dict: DictionaryArray<Int32Type> = vec!["a", "a", "b", "b", "c"].into_iter().collect();
        let col: ArrayRef = Arc::new(dict);
        let cmp = DictionaryComparator::<Int32Type, false>::new();
        assert_eq!(run(&cmp, &col), vec![true, false, true, false]);
    }

    #[test]
    fn dict_compare_duplicate_values_fallback() {
        // Dictionary with duplicate values: keys 0 and 1 both map to "a".
        // Adjacent keys differ but values are equal -> must NOT be a boundary.
        let keys = Int32Array::from(vec![0, 1, 2]);
        let values = Arc::new(StringArray::from(vec!["a", "a", "b"]));
        let dict = DictionaryArray::<Int32Type>::new(keys, values);
        let col: ArrayRef = Arc::new(dict);
        let cmp = DictionaryComparator::<Int32Type, false>::new();
        // rows: a, a, b => (a,a) equal via fallback, (a,b) boundary
        assert_eq!(run(&cmp, &col), vec![true, false]);
    }

    #[test]
    fn dict_compare_nulls() {
        // rows: null, null, "a", "a", null
        let dict: DictionaryArray<Int32Type> = vec![None, None, Some("a"), Some("a"), None]
            .into_iter()
            .collect();
        let col: ArrayRef = Arc::new(dict);
        let cmp = DictionaryComparator::<Int32Type, true>::new();
        // (null,null) equal, (null,a) boundary, (a,a) equal, (a,null) boundary
        assert_eq!(run(&cmp, &col), vec![true, false, true, false]);
    }

    #[test]
    fn dict_compare_respects_short_circuit() {
        // values [a,b], keys [0,0,1]; pre-mark first pair as already-false.
        let dict: DictionaryArray<Int32Type> = vec!["a", "a", "b"].into_iter().collect();
        let col: ArrayRef = Arc::new(dict);
        let cmp = DictionaryComparator::<Int32Type, false>::new();
        let mut eq = vec![false, true];
        cmp.compare_adjacent(&col, &mut eq);
        // first stays false (short-circuit), second is a real boundary a->b
        assert_eq!(eq, vec![false, false]);
    }
}
