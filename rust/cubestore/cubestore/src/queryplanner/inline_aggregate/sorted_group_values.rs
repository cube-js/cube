use datafusion::physical_plan::aggregates::group_values::multi_group_by::GroupColumn;

use std::mem::{self, size_of};

use datafusion::arrow::array::{Array, ArrayRef, RecordBatch};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{
    BinaryViewType, DataType, Date32Type, Date64Type, Decimal128Type, Float32Type, Float64Type,
    Int16Type, Int32Type, Int64Type, Int8Type, Schema, SchemaRef, StringViewType,
    Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType, TimeUnit,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use datafusion::dfschema::internal_err;
use datafusion::dfschema::not_impl_err;
use datafusion::error::Result as DFResult;
use datafusion::physical_expr::binary_map::OutputType;
use datafusion::physical_plan::aggregates::group_values::multi_group_by::{
    ByteGroupValueBuilder, ByteViewGroupValueBuilder, PrimitiveGroupValueBuilder,
};

pub struct SortedGroupValues {
    /// The output schema
    schema: SchemaRef,
    group_values: Vec<Box<dyn GroupColumn>>,
    rows_inds: Vec<usize>,
    equal_to_results: Vec<bool>,
}

impl SortedGroupValues {
    pub fn try_new(schema: SchemaRef) -> DFResult<Self> {
        Ok(Self {
            schema,
            group_values: vec![],
            rows_inds: vec![],
            equal_to_results: vec![],
        })
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

/* impl GroupValues for SortedGroupValues {
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        if self.group_values.is_empty() {
            let mut v = Vec::with_capacity(cols.len());

            for f in self.schema.fields().iter() {
                let nullable = f.is_nullable();
                let data_type = f.data_type();
                match data_type {
                    &DataType::Int8 => {
                        instantiate_primitive!(v, nullable, Int8Type, data_type)
                    }
                    &DataType::Int16 => {
                        instantiate_primitive!(v, nullable, Int16Type, data_type)
                    }
                    &DataType::Int32 => {
                        instantiate_primitive!(v, nullable, Int32Type, data_type)
                    }
                    &DataType::Int64 => {
                        instantiate_primitive!(v, nullable, Int64Type, data_type)
                    }
                    &DataType::UInt8 => {
                        instantiate_primitive!(v, nullable, UInt8Type, data_type)
                    }
                    &DataType::UInt16 => {
                        instantiate_primitive!(v, nullable, UInt16Type, data_type)
                    }
                    &DataType::UInt32 => {
                        instantiate_primitive!(v, nullable, UInt32Type, data_type)
                    }
                    &DataType::UInt64 => {
                        instantiate_primitive!(v, nullable, UInt64Type, data_type)
                    }
                    &DataType::Float32 => {
                        instantiate_primitive!(v, nullable, Float32Type, data_type)
                    }
                    &DataType::Float64 => {
                        instantiate_primitive!(v, nullable, Float64Type, data_type)
                    }
                    &DataType::Date32 => {
                        instantiate_primitive!(v, nullable, Date32Type, data_type)
                    }
                    &DataType::Date64 => {
                        instantiate_primitive!(v, nullable, Date64Type, data_type)
                    }
                    &DataType::Time32(t) => match t {
                        TimeUnit::Second => {
                            instantiate_primitive!(v, nullable, Time32SecondType, data_type)
                        }
                        TimeUnit::Millisecond => {
                            instantiate_primitive!(v, nullable, Time32MillisecondType, data_type)
                        }
                        _ => {}
                    },
                    &DataType::Time64(t) => match t {
                        TimeUnit::Microsecond => {
                            instantiate_primitive!(v, nullable, Time64MicrosecondType, data_type)
                        }
                        TimeUnit::Nanosecond => {
                            instantiate_primitive!(v, nullable, Time64NanosecondType, data_type)
                        }
                        _ => {}
                    },
                    &DataType::Timestamp(t, _) => match t {
                        TimeUnit::Second => {
                            instantiate_primitive!(v, nullable, TimestampSecondType, data_type)
                        }
                        TimeUnit::Millisecond => {
                            instantiate_primitive!(v, nullable, TimestampMillisecondType, data_type)
                        }
                        TimeUnit::Microsecond => {
                            instantiate_primitive!(v, nullable, TimestampMicrosecondType, data_type)
                        }
                        TimeUnit::Nanosecond => {
                            instantiate_primitive!(v, nullable, TimestampNanosecondType, data_type)
                        }
                    },
                    &DataType::Decimal128(_, _) => {
                        instantiate_primitive! {
                            v,
                            nullable,
                            Decimal128Type,
                            data_type
                        }
                    }
                    &DataType::Utf8 => {
                        let b = ByteGroupValueBuilder::<i32>::new(OutputType::Utf8);
                        v.push(Box::new(b) as _)
                    }
                    &DataType::LargeUtf8 => {
                        let b = ByteGroupValueBuilder::<i64>::new(OutputType::Utf8);
                        v.push(Box::new(b) as _)
                    }
                    &DataType::Binary => {
                        let b = ByteGroupValueBuilder::<i32>::new(OutputType::Binary);
                        v.push(Box::new(b) as _)
                    }
                    &DataType::LargeBinary => {
                        let b = ByteGroupValueBuilder::<i64>::new(OutputType::Binary);
                        v.push(Box::new(b) as _)
                    }
                    &DataType::Utf8View => {
                        let b = ByteViewGroupValueBuilder::<StringViewType>::new();
                        v.push(Box::new(b) as _)
                    }
                    &DataType::BinaryView => {
                        let b = ByteViewGroupValueBuilder::<BinaryViewType>::new();
                        v.push(Box::new(b) as _)
                    }
                    dt => return not_impl_err!("{dt} not supported in GroupValuesColumn"),
                }
            }
            self.group_values = v;
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

    fn emit(&mut self) -> Result<Vec<ArrayRef>> {
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
} */
