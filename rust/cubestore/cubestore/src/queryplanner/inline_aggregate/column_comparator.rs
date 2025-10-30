use datafusion::arrow::array::*;
use datafusion::arrow::buffer::BooleanBuffer;
use datafusion::arrow::datatypes::*;
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
