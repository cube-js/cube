use datafusion::logical_expr::EmitTo;
use datafusion::physical_plan::aggregates::group_values::multi_group_by::GroupColumn;

use std::mem::{self};

use datafusion::arrow::array::{Array, ArrayRef, RecordBatch};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{
    BinaryType, BinaryViewType, DataType, Date32Type, Date64Type, Decimal128Type, Float32Type,
    Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, LargeBinaryType, LargeUtf8Type,
    SchemaRef, StringViewType, Time32MillisecondType, Time32SecondType, Time64MicrosecondType,
    Time64NanosecondType, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
    Utf8Type,
};
use datafusion::dfschema::not_impl_err;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::physical_expr::binary_map::OutputType;
use datafusion::physical_plan::aggregates::group_values::multi_group_by::{
    ByteGroupValueBuilder, ByteViewGroupValueBuilder, PrimitiveGroupValueBuilder,
};
use datafusion::physical_plan::aggregates::group_values::GroupValues;

use crate::queryplanner::inline_aggregate::column_comparator::ColumnComparator;
use crate::queryplanner::inline_aggregate::dictionary_group_column::new_dictionary_group_column;
use crate::{
    instantiate_byte_array_comparator, instantiate_byte_view_comparator,
    instantiate_dictionary_comparator, instantiate_primitive_comparator,
};

pub struct SortedGroupValues {
    /// The output schema
    schema: SchemaRef,
    /// Group value builders for each grouping column
    group_values: Vec<Box<dyn GroupColumn>>,
    /// Column comparators for detecting group boundaries
    comparators: Vec<Box<dyn ColumnComparator>>,
    /// Reusable buffer for row indices (not currently used)
    rows_inds: Vec<usize>,
    /// Reusable buffer for equality comparison results
    equal_to_results: Vec<bool>,
}

/// instantiates a [`PrimitiveGroupValueBuilder`] and pushes it into $v
///
/// Arguments:
/// `$v`: the vector to push the new builder into
/// `$nullable`: whether the input can contains nulls
/// `$t`: the primitive type of the builder
///
macro_rules! instantiate_primitive {
    ($v:expr, $nullable:expr, $t:ty, $data_type:ident) => {
        if $nullable {
            let b = PrimitiveGroupValueBuilder::<$t, true>::new($data_type.to_owned());
            $v.push(Box::new(b) as _)
        } else {
            let b = PrimitiveGroupValueBuilder::<$t, false>::new($data_type.to_owned());
            $v.push(Box::new(b) as _)
        }
    };
}

impl SortedGroupValues {
    pub fn try_new(schema: SchemaRef) -> DFResult<Self> {
        Ok(Self {
            schema,
            group_values: vec![],
            comparators: vec![],
            rows_inds: vec![],
            equal_to_results: vec![],
        })
    }

    fn intern_impl(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> DFResult<()> {
        let n_rows = cols[0].len();
        groups.clear();

        if n_rows == 0 {
            return Ok(());
        }

        // Handle first row - compare with last group or create new group
        let first_group_idx = self.make_new_group_if_needed(cols, 0);
        groups.push(first_group_idx);

        if n_rows == 1 {
            return Ok(());
        }

        // Prepare buffer for vectorized comparison
        self.equal_to_results.resize(n_rows - 1, true);
        self.equal_to_results[..n_rows - 1].fill(true);

        // Vectorized comparison: compare row[i] with row[i+1] for all columns
        for (col, comparator) in cols.iter().zip(&self.comparators) {
            comparator.compare_adjacent(col, &mut self.equal_to_results[..n_rows - 1]);
        }

        // Build groups based on comparison results
        let mut current_group_idx = first_group_idx;
        for i in 0..n_rows - 1 {
            if !self.equal_to_results[i] {
                // Group boundary detected - add new group
                for (col_idx, group_value) in self.group_values.iter_mut().enumerate() {
                    group_value.append_val(&cols[col_idx], i + 1);
                }
                current_group_idx = self.group_values[0].len() - 1;
            }
            groups.push(current_group_idx);
        }

        Ok(())
    }

    /// Compare the specified row with the last group and create a new group if different.
    ///
    /// This is used to handle the first row of a batch, which needs to be compared
    /// with the last group from the previous batch to detect group boundaries across batches.
    ///
    /// Returns the group index for this row.
    fn make_new_group_if_needed(&mut self, cols: &[ArrayRef], row: usize) -> usize {
        let new_group_needed = if self.group_values[0].len() == 0 {
            // No groups yet - always create first group
            true
        } else {
            // Compare with last group - if any column differs, need new group
            self.group_values.iter().enumerate().any(|(i, group_val)| {
                !group_val.equal_to(self.group_values[0].len() - 1, &cols[i], row)
            })
        };

        if new_group_needed {
            // Add new group with values from this row
            for (i, group_value) in self.group_values.iter_mut().enumerate() {
                group_value.append_val(&cols[i], row);
            }
        }

        // Return index of the group (either newly created or existing last group)
        self.group_values[0].len() - 1
    }
}

impl GroupValues for SortedGroupValues {
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> DFResult<()> {
        if self.group_values.is_empty() {
            let mut v = Vec::with_capacity(cols.len());
            let mut comparators = Vec::with_capacity(cols.len());

            for f in self.schema.fields().iter() {
                let nullable = f.is_nullable();
                let data_type = f.data_type();
                match data_type {
                    &DataType::Int8 => {
                        instantiate_primitive!(v, nullable, Int8Type, data_type);
                        instantiate_primitive_comparator!(comparators, nullable, Int8Type);
                    }
                    &DataType::Int16 => {
                        instantiate_primitive!(v, nullable, Int16Type, data_type);
                        instantiate_primitive_comparator!(comparators, nullable, Int16Type);
                    }
                    &DataType::Int32 => {
                        instantiate_primitive!(v, nullable, Int32Type, data_type);
                        instantiate_primitive_comparator!(comparators, nullable, Int32Type);
                    }
                    &DataType::Int64 => {
                        instantiate_primitive!(v, nullable, Int64Type, data_type);
                        instantiate_primitive_comparator!(comparators, nullable, Int64Type);
                    }
                    &DataType::UInt8 => {
                        instantiate_primitive!(v, nullable, UInt8Type, data_type);
                        instantiate_primitive_comparator!(comparators, nullable, UInt8Type);
                    }
                    &DataType::UInt16 => {
                        instantiate_primitive!(v, nullable, UInt16Type, data_type);
                        instantiate_primitive_comparator!(comparators, nullable, UInt16Type);
                    }
                    &DataType::UInt32 => {
                        instantiate_primitive!(v, nullable, UInt32Type, data_type);
                        instantiate_primitive_comparator!(comparators, nullable, UInt32Type);
                    }
                    &DataType::UInt64 => {
                        instantiate_primitive!(v, nullable, UInt64Type, data_type);
                        instantiate_primitive_comparator!(comparators, nullable, UInt64Type);
                    }
                    &DataType::Float32 => {
                        instantiate_primitive!(v, nullable, Float32Type, data_type);
                        instantiate_primitive_comparator!(comparators, nullable, Float32Type);
                    }
                    &DataType::Float64 => {
                        instantiate_primitive!(v, nullable, Float64Type, data_type);
                        instantiate_primitive_comparator!(comparators, nullable, Float64Type);
                    }
                    &DataType::Date32 => {
                        instantiate_primitive!(v, nullable, Date32Type, data_type);
                        instantiate_primitive_comparator!(comparators, nullable, Date32Type);
                    }
                    &DataType::Date64 => {
                        instantiate_primitive!(v, nullable, Date64Type, data_type);
                        instantiate_primitive_comparator!(comparators, nullable, Date64Type);
                    }
                    &DataType::Time32(t) => match t {
                        TimeUnit::Second => {
                            instantiate_primitive!(v, nullable, Time32SecondType, data_type);
                            instantiate_primitive_comparator!(
                                comparators,
                                nullable,
                                Time32SecondType
                            );
                        }
                        TimeUnit::Millisecond => {
                            instantiate_primitive!(v, nullable, Time32MillisecondType, data_type);
                            instantiate_primitive_comparator!(
                                comparators,
                                nullable,
                                Time32MillisecondType
                            );
                        }
                        _ => {}
                    },
                    &DataType::Time64(t) => match t {
                        TimeUnit::Microsecond => {
                            instantiate_primitive!(v, nullable, Time64MicrosecondType, data_type);
                            instantiate_primitive_comparator!(
                                comparators,
                                nullable,
                                Time64MicrosecondType
                            );
                        }
                        TimeUnit::Nanosecond => {
                            instantiate_primitive!(v, nullable, Time64NanosecondType, data_type);
                            instantiate_primitive_comparator!(
                                comparators,
                                nullable,
                                Time64NanosecondType
                            );
                        }
                        _ => {}
                    },
                    &DataType::Timestamp(t, _) => match t {
                        TimeUnit::Second => {
                            instantiate_primitive!(v, nullable, TimestampSecondType, data_type);
                            instantiate_primitive_comparator!(
                                comparators,
                                nullable,
                                TimestampSecondType
                            );
                        }
                        TimeUnit::Millisecond => {
                            instantiate_primitive!(
                                v,
                                nullable,
                                TimestampMillisecondType,
                                data_type
                            );
                            instantiate_primitive_comparator!(
                                comparators,
                                nullable,
                                TimestampMillisecondType
                            );
                        }
                        TimeUnit::Microsecond => {
                            instantiate_primitive!(
                                v,
                                nullable,
                                TimestampMicrosecondType,
                                data_type
                            );
                            instantiate_primitive_comparator!(
                                comparators,
                                nullable,
                                TimestampMicrosecondType
                            );
                        }
                        TimeUnit::Nanosecond => {
                            instantiate_primitive!(v, nullable, TimestampNanosecondType, data_type);
                            instantiate_primitive_comparator!(
                                comparators,
                                nullable,
                                TimestampNanosecondType
                            );
                        }
                    },
                    &DataType::Decimal128(_, _) => {
                        instantiate_primitive! {
                            v,
                            nullable,
                            Decimal128Type,
                            data_type
                        }
                        instantiate_primitive_comparator!(comparators, nullable, Decimal128Type);
                    }
                    &DataType::Utf8 => {
                        let b = ByteGroupValueBuilder::<i32>::new(OutputType::Utf8);
                        v.push(Box::new(b) as _);
                        instantiate_byte_array_comparator!(comparators, nullable, Utf8Type);
                    }
                    &DataType::LargeUtf8 => {
                        let b = ByteGroupValueBuilder::<i64>::new(OutputType::Utf8);
                        v.push(Box::new(b) as _);
                        instantiate_byte_array_comparator!(comparators, nullable, LargeUtf8Type);
                    }
                    &DataType::Binary => {
                        let b = ByteGroupValueBuilder::<i32>::new(OutputType::Binary);
                        v.push(Box::new(b) as _);
                        instantiate_byte_array_comparator!(comparators, nullable, BinaryType);
                    }
                    &DataType::LargeBinary => {
                        let b = ByteGroupValueBuilder::<i64>::new(OutputType::Binary);
                        v.push(Box::new(b) as _);
                        instantiate_byte_array_comparator!(comparators, nullable, LargeBinaryType);
                    }
                    &DataType::Utf8View => {
                        let b = ByteViewGroupValueBuilder::<StringViewType>::new();
                        v.push(Box::new(b) as _);
                        instantiate_byte_view_comparator!(comparators, nullable, StringViewType);
                    }
                    &DataType::BinaryView => {
                        let b = ByteViewGroupValueBuilder::<BinaryViewType>::new();
                        v.push(Box::new(b) as _);
                        instantiate_byte_view_comparator!(comparators, nullable, BinaryViewType);
                    }
                    &DataType::Dictionary(ref key_type, ref value_type) => {
                        v.push(new_dictionary_group_column(key_type, value_type)?);
                        match key_type.as_ref() {
                            DataType::Int8 => {
                                instantiate_dictionary_comparator!(comparators, nullable, Int8Type)
                            }
                            DataType::Int16 => {
                                instantiate_dictionary_comparator!(comparators, nullable, Int16Type)
                            }
                            DataType::Int32 => {
                                instantiate_dictionary_comparator!(comparators, nullable, Int32Type)
                            }
                            DataType::Int64 => {
                                instantiate_dictionary_comparator!(comparators, nullable, Int64Type)
                            }
                            DataType::UInt8 => {
                                instantiate_dictionary_comparator!(comparators, nullable, UInt8Type)
                            }
                            DataType::UInt16 => {
                                instantiate_dictionary_comparator!(
                                    comparators,
                                    nullable,
                                    UInt16Type
                                )
                            }
                            DataType::UInt32 => {
                                instantiate_dictionary_comparator!(
                                    comparators,
                                    nullable,
                                    UInt32Type
                                )
                            }
                            DataType::UInt64 => {
                                instantiate_dictionary_comparator!(
                                    comparators,
                                    nullable,
                                    UInt64Type
                                )
                            }
                            dt => {
                                return not_impl_err!(
                                    "dictionary key type {dt} not supported in SortedGroupValues"
                                )
                            }
                        }
                    }
                    dt => return not_impl_err!("{dt} not supported in SortedGroupValues"),
                }
            }
            self.group_values = v;
            self.comparators = comparators;
        }
        self.intern_impl(cols, groups)
    }

    fn size(&self) -> usize {
        let group_values_size: usize = self.group_values.iter().map(|v| v.size()).sum();
        group_values_size
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn len(&self) -> usize {
        if self.group_values.is_empty() {
            return 0;
        }

        self.group_values[0].len()
    }

    fn emit(&mut self, emit_to: EmitTo) -> DFResult<Vec<ArrayRef>> {
        let mut output = match emit_to {
            EmitTo::All => {
                let group_values = mem::take(&mut self.group_values);
                debug_assert!(self.group_values.is_empty());

                group_values
                    .into_iter()
                    .map(|v| v.build())
                    .collect::<Vec<_>>()
            }
            EmitTo::First(n) => {
                let output = self
                    .group_values
                    .iter_mut()
                    .map(|v| v.take_n(n))
                    .collect::<Vec<_>>();

                output
            }
        };

        for (field, array) in self.schema.fields.iter().zip(&mut output) {
            let expected = field.data_type();
            if let DataType::Dictionary(_, v) = expected {
                let actual = array.data_type();
                if v.as_ref() != actual {
                    return Err(DataFusionError::Internal(format!(
                        "Converted group rows expected dictionary of {v} got {actual}"
                    )));
                }
                *array = cast(array.as_ref(), expected)?;
            }
        }

        Ok(output)
    }

    fn clear_shrink(&mut self, _batch: &RecordBatch) {
        self.group_values.clear();
        self.comparators.clear();
        self.rows_inds.clear();
        self.equal_to_results.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    fn dict_schema(nullable: bool) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "g",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            nullable,
        )]))
    }

    fn decode(dict: &ArrayRef) -> Vec<Option<String>> {
        let dict = dict
            .as_any()
            .downcast_ref::<datafusion::arrow::array::DictionaryArray<Int32Type>>()
            .unwrap();
        let values = dict
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        (0..dict.len())
            .map(|i| {
                if dict.is_null(i) {
                    None
                } else {
                    Some(values.value(dict.keys().value(i) as usize).to_string())
                }
            })
            .collect()
    }

    /// Groups must continue across a batch boundary even when the two batches carry different
    /// local dictionaries (the same string is encoded with different keys per batch).
    #[test]
    fn sorted_group_values_dictionary_cross_batch() {
        let mut gv = SortedGroupValues::try_new(dict_schema(false)).unwrap();

        // Batch 1: a, a, b  (values [a,b], keys [0,0,1])
        let b1 = datafusion::arrow::array::DictionaryArray::<Int32Type>::new(
            Int32Array::from(vec![0, 0, 1]),
            Arc::new(StringArray::from(vec!["a", "b"])),
        );
        let mut groups = vec![];
        gv.intern(&[Arc::new(b1) as ArrayRef], &mut groups).unwrap();
        assert_eq!(groups, vec![0, 0, 1]);

        // Batch 2: b, c  with a DIFFERENT local dictionary (values [b,c], keys [0,1]).
        let b2 = datafusion::arrow::array::DictionaryArray::<Int32Type>::new(
            Int32Array::from(vec![0, 1]),
            Arc::new(StringArray::from(vec!["b", "c"])),
        );
        gv.intern(&[Arc::new(b2) as ArrayRef], &mut groups).unwrap();
        // "b" continues the last group (idx 1), "c" opens group 2.
        assert_eq!(groups, vec![1, 2]);

        assert_eq!(gv.len(), 3);
        let out = gv.emit(EmitTo::All).unwrap();
        assert_eq!(
            decode(&out[0]),
            vec![
                Some("a".to_string()),
                Some("b".to_string()),
                Some("c".to_string())
            ]
        );
    }

    /// Isolated timing: dictionary vs Utf8 group keys over a sorted 10-column stream.
    /// Run with: cargo test -p cubestore --lib sorted_group_values_dict_vs_utf8_bench -- --ignored --nocapture
    #[test]
    #[ignore]
    fn sorted_group_values_dict_vs_utf8_bench() {
        use std::time::Instant;
        const NCOLS: usize = 10;
        const ROWS: usize = 2_000_000;
        const BATCH: usize = 8192;
        const ROWS_PER_GROUP: usize = 20; // ~100k groups, low per-column cardinality

        // tuple value for column j of group g, c0 most significant -> stream is sorted ascending
        let val = |g: usize, j: usize| -> String {
            let digit = (g / 4usize.pow((NCOLS - 1 - j) as u32)) % 4;
            format!("c{j}_{digit}")
        };

        // Build Utf8 batches and Dictionary batches for the same sorted data.
        let mut utf8_batches: Vec<Vec<ArrayRef>> = vec![];
        let mut dict_batches: Vec<Vec<ArrayRef>> = vec![];
        let mut row = 0usize;
        while row < ROWS {
            let n = BATCH.min(ROWS - row);
            let mut utf8_cols: Vec<ArrayRef> = Vec::with_capacity(NCOLS);
            let mut dict_cols: Vec<ArrayRef> = Vec::with_capacity(NCOLS);
            for j in 0..NCOLS {
                let vals: Vec<String> =
                    (0..n).map(|i| val((row + i) / ROWS_PER_GROUP, j)).collect();
                let strs: Vec<&str> = vals.iter().map(|s| s.as_str()).collect();
                utf8_cols.push(Arc::new(StringArray::from(strs.clone())) as ArrayRef);
                let dict: datafusion::arrow::array::DictionaryArray<Int32Type> =
                    strs.into_iter().collect();
                dict_cols.push(Arc::new(dict) as ArrayRef);
            }
            utf8_batches.push(utf8_cols);
            dict_batches.push(dict_cols);
            row += n;
        }

        let utf8_schema = Arc::new(Schema::new(
            (0..NCOLS)
                .map(|j| Field::new(format!("c{j}"), DataType::Utf8, false))
                .collect::<Vec<_>>(),
        ));
        let dict_schema = Arc::new(Schema::new(
            (0..NCOLS)
                .map(|j| {
                    Field::new(
                        format!("c{j}"),
                        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                        false,
                    )
                })
                .collect::<Vec<_>>(),
        ));

        let run = |schema: SchemaRef, batches: &Vec<Vec<ArrayRef>>| -> (u128, usize) {
            let mut gv = SortedGroupValues::try_new(schema).unwrap();
            let mut groups = vec![];
            let t0 = Instant::now();
            for cols in batches {
                gv.intern(cols, &mut groups).unwrap();
            }
            (t0.elapsed().as_micros(), gv.len())
        };

        // warm + measure (best of 3)
        let mut utf8_us = u128::MAX;
        let mut dict_us = u128::MAX;
        let mut ngroups = 0;
        for _ in 0..3 {
            let (u, gu) = run(utf8_schema.clone(), &utf8_batches);
            let (d, gd) = run(dict_schema.clone(), &dict_batches);
            assert_eq!(gu, gd, "group counts must match");
            ngroups = gu;
            utf8_us = utf8_us.min(u);
            dict_us = dict_us.min(d);
        }
        println!(
            "intern over {ROWS} rows x {NCOLS} cols, {ngroups} groups:\n  Utf8: {:.1} ms\n  Dict: {:.1} ms\n  speedup: {:.2}x",
            utf8_us as f64 / 1000.0,
            dict_us as f64 / 1000.0,
            utf8_us as f64 / dict_us as f64,
        );
    }

    /// Null keys form their own group and continue across batches.
    #[test]
    fn sorted_group_values_dictionary_nulls() {
        let mut gv = SortedGroupValues::try_new(dict_schema(true)).unwrap();

        // rows: null, null, a
        let b1: datafusion::arrow::array::DictionaryArray<Int32Type> =
            vec![None, None, Some("a")].into_iter().collect();
        let mut groups = vec![];
        gv.intern(&[Arc::new(b1) as ArrayRef], &mut groups).unwrap();
        assert_eq!(groups, vec![0, 0, 1]);

        // rows: a, b -> "a" continues group 1, "b" new
        let b2: datafusion::arrow::array::DictionaryArray<Int32Type> =
            vec![Some("a"), Some("b")].into_iter().collect();
        gv.intern(&[Arc::new(b2) as ArrayRef], &mut groups).unwrap();
        assert_eq!(groups, vec![1, 2]);

        let out = gv.emit(EmitTo::All).unwrap();
        assert_eq!(
            decode(&out[0]),
            vec![None, Some("a".to_string()), Some("b".to_string())]
        );
    }
}
