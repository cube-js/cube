use datafusion::logical_expr::EmitTo;

use datafusion::arrow::array::{Array, ArrayRef, ListArray, RecordBatch, StructArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::row::{RowConverter, Rows, SortField};
use datafusion::error::Result as DFResult;
use datafusion::physical_plan::aggregates::group_values::GroupValues;

use std::sync::Arc;

/// A [`GroupValues`] implementation optimized for sorted input data
///
/// This is a specialized implementation for sorted data that:
/// - Does not use a hash table (unlike `GroupValuesRows`)
/// - Detects group boundaries by comparing adjacent rows
/// - Works for any data type including Boolean, Struct, List, etc.
///
/// It uses the arrow-rs [`Rows`] format for efficient row-wise storage and comparison.
pub struct SortedGroupValuesRows {
    /// The output schema
    schema: SchemaRef,

    /// Converter for the group values
    row_converter: RowConverter,

    /// The actual group by values, stored in arrow [`Row`] format.
    /// `group_values[i]` holds the group value for group_index `i`.
    ///
    /// The row format is used to compare group keys quickly and store
    /// them efficiently in memory. Quick comparison is especially
    /// important for multi-column group keys.
    ///
    /// [`Row`]: arrow::row::Row
    group_values: Option<Rows>,

    /// Reused buffer to store rows
    rows_buffer: Rows,
}

impl SortedGroupValuesRows {
    pub fn try_new(schema: SchemaRef) -> DFResult<Self> {
        let row_converter = RowConverter::new(
            schema
                .fields()
                .iter()
                .map(|f| SortField::new(f.data_type().clone()))
                .collect(),
        )?;

        let starting_rows_capacity = 1000;
        let starting_data_capacity = 64 * starting_rows_capacity;
        let rows_buffer = row_converter.empty_rows(starting_rows_capacity, starting_data_capacity);

        Ok(Self {
            schema,
            row_converter,
            group_values: None,
            rows_buffer,
        })
    }

    fn intern_impl(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> DFResult<()> {
        // Convert the group keys into the row format
        self.rows_buffer.clear();
        self.row_converter.append(&mut self.rows_buffer, cols)?;
        let n_rows = self.rows_buffer.num_rows();

        groups.clear();

        if n_rows == 0 {
            return Ok(());
        }

        let mut group_values = match self.group_values.take() {
            Some(group_values) => group_values,
            None => self.row_converter.empty_rows(0, 0),
        };

        // Handle first row - compare with last group or create new group
        let new_group_needed = if group_values.num_rows() == 0 {
            // No groups yet - always create first group
            true
        } else {
            // Compare with last group - if differs, need new group
            let last_group_idx = group_values.num_rows() - 1;
            group_values.row(last_group_idx) != self.rows_buffer.row(0)
        };

        if new_group_needed {
            // Add new group with values from first row
            group_values.push(self.rows_buffer.row(0));
        }

        let first_group_idx = group_values.num_rows() - 1;
        groups.push(first_group_idx);

        if n_rows == 1 {
            self.group_values = Some(group_values);
            return Ok(());
        }

        // Build groups based on comparison of adjacent rows
        let mut current_group_idx = first_group_idx;
        for i in 0..n_rows - 1 {
            // Compare row[i] with row[i+1]
            if self.rows_buffer.row(i) != self.rows_buffer.row(i + 1) {
                // Group boundary detected - add new group
                group_values.push(self.rows_buffer.row(i + 1));
                current_group_idx = group_values.num_rows() - 1;
            }
            groups.push(current_group_idx);
        }

        self.group_values = Some(group_values);
        Ok(())
    }
}

impl GroupValues for SortedGroupValuesRows {
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> DFResult<()> {
        self.intern_impl(cols, groups)
    }

    fn size(&self) -> usize {
        let group_values_size = self.group_values.as_ref().map(|v| v.size()).unwrap_or(0);
        self.row_converter.size() + group_values_size + self.rows_buffer.size()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn len(&self) -> usize {
        self.group_values
            .as_ref()
            .map(|group_values| group_values.num_rows())
            .unwrap_or(0)
    }

    fn emit(&mut self, emit_to: EmitTo) -> DFResult<Vec<ArrayRef>> {
        let mut group_values = self
            .group_values
            .take()
            .expect("Can not emit from empty rows");

        let mut output = match emit_to {
            EmitTo::All => {
                let output = self.row_converter.convert_rows(&group_values)?;
                group_values.clear();
                output
            }
            EmitTo::First(n) => {
                let groups_rows = group_values.iter().take(n);
                let output = self.row_converter.convert_rows(groups_rows)?;
                // Clear out first n group keys by copying them to a new Rows.
                let mut new_group_values = self.row_converter.empty_rows(0, 0);
                for row in group_values.iter().skip(n) {
                    new_group_values.push(row);
                }
                std::mem::swap(&mut new_group_values, &mut group_values);
                output
            }
        };

        // Handle dictionary encoding for output
        for (field, array) in self.schema.fields.iter().zip(&mut output) {
            let expected = field.data_type();
            *array = dictionary_encode_if_necessary(Arc::<dyn Array>::clone(array), expected)?;
        }

        self.group_values = Some(group_values);
        Ok(output)
    }

    fn clear_shrink(&mut self, _batch: &RecordBatch) {
        self.group_values = self.group_values.take().map(|mut rows| {
            rows.clear();
            rows
        });
    }
}

fn dictionary_encode_if_necessary(array: ArrayRef, expected: &DataType) -> DFResult<ArrayRef> {
    match (expected, array.data_type()) {
        (DataType::Struct(expected_fields), _) => {
            let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();
            let arrays = expected_fields
                .iter()
                .zip(struct_array.columns())
                .map(|(expected_field, column)| {
                    dictionary_encode_if_necessary(
                        Arc::<dyn Array>::clone(column),
                        expected_field.data_type(),
                    )
                })
                .collect::<DFResult<Vec<_>>>()?;

            Ok(Arc::new(StructArray::try_new(
                expected_fields.clone(),
                arrays,
                struct_array.nulls().cloned(),
            )?))
        }
        (DataType::List(expected_field), &DataType::List(_)) => {
            let list = array.as_any().downcast_ref::<ListArray>().unwrap();

            Ok(Arc::new(ListArray::try_new(
                Arc::<datafusion::arrow::datatypes::Field>::clone(expected_field),
                list.offsets().clone(),
                dictionary_encode_if_necessary(
                    Arc::<dyn Array>::clone(list.values()),
                    expected_field.data_type(),
                )?,
                list.nulls().cloned(),
            )?))
        }
        (DataType::Dictionary(_, _), _) => Ok(cast(array.as_ref(), expected)?),
        (_, _) => Ok(Arc::<dyn Array>::clone(&array)),
    }
}
