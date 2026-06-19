use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, DictionaryArray, Int32Array, Int32Builder, StringArray,
};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{DataType, Int32Type};
use datafusion::error::{DataFusionError, Result as DFResult};

/// True for the only dictionary layout CubeStore produces for string group keys.
pub(crate) fn is_int32_utf8_dict(dt: &DataType) -> bool {
    matches!(dt, DataType::Dictionary(k, v)
        if k.as_ref() == &DataType::Int32 && v.as_ref() == &DataType::Utf8)
}

/// Accumulates a global `String -> id` mapping across batches so a dictionary-encoded group column
/// can be grouped as `Int32` global ids on DataFusion's fast primitive path, instead of
/// materializing the string on every row. The per-batch string work is proportional to the batch's
/// distinct dictionary values, not its row count. Null dictionary entries and null keys stay null.
pub(crate) struct GlobalDict {
    value_to_id: HashMap<String, i32>,
    values: Vec<String>,
}

impl GlobalDict {
    pub fn new() -> Self {
        Self {
            value_to_id: HashMap::new(),
            values: Vec::new(),
        }
    }

    fn intern_value(&mut self, v: &str) -> i32 {
        if let Some(id) = self.value_to_id.get(v) {
            return *id;
        }
        let id = self.values.len() as i32;
        self.values.push(v.to_string());
        self.value_to_id.insert(v.to_string(), id);
        id
    }

    /// Remap a `Dictionary(Int32, Utf8)` array to an `Int32Array` of global ids.
    pub fn remap(&mut self, array: &ArrayRef) -> DFResult<ArrayRef> {
        let dict = array
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "GlobalDict::remap expected Dictionary(Int32)".to_string(),
                )
            })?;
        let local_values = dict
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("GlobalDict::remap expected Utf8 values".to_string())
            })?;

        // local id -> global id, interning each distinct value once; a null dictionary entry is a
        // null in this map. Built once per batch (O(distinct values)).
        let mut builder = Int32Builder::with_capacity(local_values.len());
        for i in 0..local_values.len() {
            if local_values.is_null(i) {
                builder.append_null();
            } else {
                builder.append_value(self.intern_value(local_values.value(i)));
            }
        }
        let local_to_global = builder.finish();

        // Gather the global id per row via a vectorized take: null keys and null dictionary entries
        // both propagate to null, matching how the string path groups nulls.
        Ok(take(&local_to_global, dict.keys(), None)?)
    }

    /// Rebuild a `Dictionary(Int32, Utf8)` array from an `Int32Array` of global ids emitted by the
    /// group table; the values are the full accumulated global dictionary.
    pub fn rebuild(&self, ids: &ArrayRef) -> DFResult<ArrayRef> {
        let ids = ids.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
            DataFusionError::Internal("GlobalDict::rebuild expected Int32 ids".to_string())
        })?;
        let values = StringArray::from_iter_values(self.values.iter());
        let dict = DictionaryArray::<Int32Type>::try_new(ids.clone(), Arc::new(values))?;
        Ok(Arc::new(dict))
    }
}
