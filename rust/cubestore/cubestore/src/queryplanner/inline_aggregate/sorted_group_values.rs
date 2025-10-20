use datafusion::physical_plan::aggregates::group_values::multi_group_by::GroupColumn;

use std::mem::{self, size_of};

use datafusion::arrow::array::{Array, ArrayRef, RecordBatch};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{
    BinaryType, BinaryViewType, DataType, Date32Type, Date64Type, Decimal128Type, Float32Type,
    Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, LargeBinaryType, LargeUtf8Type,
    Schema, SchemaRef, StringViewType, Time32MillisecondType, Time32SecondType,
    Time64MicrosecondType, Time64NanosecondType, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt16Type,
    UInt32Type, UInt64Type, UInt8Type, Utf8Type,
};
use datafusion::dfschema::internal_err;
use datafusion::dfschema::not_impl_err;
use datafusion::error::Result as DFResult;
use datafusion::physical_expr::binary_map::OutputType;
use datafusion::physical_plan::aggregates::group_values::multi_group_by::{
    ByteGroupValueBuilder, ByteViewGroupValueBuilder, PrimitiveGroupValueBuilder,
};

use crate::queryplanner::inline_aggregate::column_comparator::ColumnComparator;
use crate::{
    instantiate_byte_array_comparator, instantiate_byte_view_comparator,
    instantiate_primitive_comparator,
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

    pub fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> DFResult<()> {
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
                            instantiate_primitive_comparator!(comparators, nullable, Time32SecondType);
                        }
                        TimeUnit::Millisecond => {
                            instantiate_primitive!(v, nullable, Time32MillisecondType, data_type);
                            instantiate_primitive_comparator!(comparators, nullable, Time32MillisecondType);
                        }
                        _ => {}
                    },
                    &DataType::Time64(t) => match t {
                        TimeUnit::Microsecond => {
                            instantiate_primitive!(v, nullable, Time64MicrosecondType, data_type);
                            instantiate_primitive_comparator!(comparators, nullable, Time64MicrosecondType);
                        }
                        TimeUnit::Nanosecond => {
                            instantiate_primitive!(v, nullable, Time64NanosecondType, data_type);
                            instantiate_primitive_comparator!(comparators, nullable, Time64NanosecondType);
                        }
                        _ => {}
                    },
                    &DataType::Timestamp(t, _) => match t {
                        TimeUnit::Second => {
                            instantiate_primitive!(v, nullable, TimestampSecondType, data_type);
                            instantiate_primitive_comparator!(comparators, nullable, TimestampSecondType);
                        }
                        TimeUnit::Millisecond => {
                            instantiate_primitive!(v, nullable, TimestampMillisecondType, data_type);
                            instantiate_primitive_comparator!(comparators, nullable, TimestampMillisecondType);
                        }
                        TimeUnit::Microsecond => {
                            instantiate_primitive!(v, nullable, TimestampMicrosecondType, data_type);
                            instantiate_primitive_comparator!(comparators, nullable, TimestampMicrosecondType);
                        }
                        TimeUnit::Nanosecond => {
                            instantiate_primitive!(v, nullable, TimestampNanosecondType, data_type);
                            instantiate_primitive_comparator!(comparators, nullable, TimestampNanosecondType);
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
                    dt => return not_impl_err!("{dt} not supported in SortedGroupValues"),
                }
            }
            self.group_values = v;
            self.comparators = comparators;
        }
        self.intern_impl(cols, groups)
    }

    pub fn size(&self) -> usize {
        let group_values_size: usize = self.group_values.iter().map(|v| v.size()).sum();
        group_values_size
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        if self.group_values.is_empty() {
            return 0;
        }

        self.group_values[0].len()
    }

    pub fn emit(&mut self) -> DFResult<Vec<ArrayRef>> {
        /* let mut output = match emit_to {
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

        // TODO: Materialize dictionaries in group keys (#7647)
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

        Ok(output) */
        todo!()
    }

    fn clear_shrink(&mut self, batch: &RecordBatch) {
        self.group_values.clear();
        self.rows_inds.clear();
        self.equal_to_results.clear();
    }

    fn intern_impl(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> DFResult<()> {
        /* let n_rows = cols[0].len();
        groups.clear();

        if n_rows == 0 {
            return Ok(());
        }

        let first_group_idx = self.make_new_group_if_needed(cols, 0);
        groups.push(first_group_idx);

        if n_rows == 1 {
            return Ok(());
        }

        if self.rows_inds.len() < n_rows {
            let old_len = self.rows_inds.len();
            self.rows_inds.extend(old_len..n_rows);
        }

        self.equal_to_results.fill(true);
        self.equal_to_results.resize(n_rows - 1, true);

        let lhs_rows = &self.rows_inds[0..n_rows - 1];
        let rhs_rows = &self.rows_inds[1..n_rows];
        for (col_idx, group_col) in self.group_values.iter().enumerate() {
            cols[col_idx].vectorized_equal_to(
                lhs_rows,
                &cols[col_idx],
                rhs_rows,
                &mut self.equal_to_results,
            );
        }
        println!("!!!!! AAAAAAAAAA");
        let mut current_group_idx = first_group_idx;
        for i in 0..n_rows - 1 {
            if !self.equal_to_results[i] {
                for (col_idx, group_value) in self.group_values.iter_mut().enumerate() {
                    group_value.append_val(&cols[col_idx], i + 1);
                }
                current_group_idx = self.group_values[0].len() - 1;
            }
            groups.push(current_group_idx);
        }
        println!("!!!!! BBBBBBB");
        Ok(()) */
        Ok(())
    }

    fn make_new_group_if_needed(&mut self, cols: &[ArrayRef], row: usize) -> usize {
        let new_group_needed = if self.group_values[0].len() == 0 {
            true
        } else {
            self.group_values.iter().enumerate().any(|(i, group_val)| {
                !group_val.equal_to(self.group_values[0].len() - 1, &cols[i], row)
            })
        };
        if new_group_needed {
            for (i, group_value) in self.group_values.iter_mut().enumerate() {
                group_value.append_val(&cols[i], row);
            }
        }
        self.group_values[0].len() - 1
    }
}
