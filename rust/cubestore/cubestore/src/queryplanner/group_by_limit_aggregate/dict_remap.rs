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
    value_to_id: HashMap<Arc<str>, i32>,
    values: Vec<Arc<str>>,
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
        // One allocation shared between the map key and the values vec.
        let key: Arc<str> = Arc::from(v);
        self.values.push(key.clone());
        self.value_to_id.insert(key, id);
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

#[cfg(test)]
mod tests {
    use super::*;

    fn dict(values: Vec<Option<&str>>, keys: Vec<Option<i32>>) -> ArrayRef {
        let values = StringArray::from(values);
        let keys = Int32Array::from(keys);
        Arc::new(DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap())
    }

    fn ids(a: &ArrayRef) -> Int32Array {
        a.as_any().downcast_ref::<Int32Array>().unwrap().clone()
    }

    fn rebuilt_strings(a: &ArrayRef) -> Vec<Option<String>> {
        let d = a
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        let v = d.values().as_any().downcast_ref::<StringArray>().unwrap();
        d.keys()
            .iter()
            .map(|k| k.map(|k| v.value(k as usize).to_string()))
            .collect()
    }

    #[test]
    fn remaps_to_consistent_global_ids_across_batches() {
        let mut gd = GlobalDict::new();
        // batch 1: local dict ["b", "a"], rows b, a, b
        let b1 = ids(&gd
            .remap(&dict(
                vec![Some("b"), Some("a")],
                vec![Some(0), Some(1), Some(0)],
            ))
            .unwrap());
        // batch 2: a DIFFERENT local dict ["a", "c"], rows c, a -- "a" must reuse its global id
        let b2 = ids(&gd
            .remap(&dict(vec![Some("a"), Some("c")], vec![Some(1), Some(0)]))
            .unwrap());

        assert_eq!(b1.values(), &[0, 1, 0]); // b=0, a=1 (first-seen)
        assert_eq!(b2.value(1), b1.value(1)); // same string "a" -> same global id across batches
        assert_ne!(b2.value(0), b1.value(0)); // "c" is a new id

        // rebuild over the accumulated global ids yields the original strings
        let all: ArrayRef = Arc::new(Int32Array::from(vec![
            b1.value(0),
            b1.value(1),
            b2.value(0),
        ]));
        assert_eq!(
            rebuilt_strings(&gd.rebuild(&all).unwrap()),
            vec![
                Some("b".to_string()),
                Some("a".to_string()),
                Some("c".to_string())
            ]
        );
    }

    #[test]
    fn null_keys_and_null_entries_stay_null() {
        let mut gd = GlobalDict::new();
        // local dict ["x", null]; rows: x, null-key, points-to-null-entry
        let r = gd
            .remap(&dict(vec![Some("x"), None], vec![Some(0), None, Some(1)]))
            .unwrap();
        let r = ids(&r);
        assert!(r.is_valid(0));
        assert!(r.is_null(1));
        assert!(r.is_null(2));
        assert_eq!(
            rebuilt_strings(&gd.rebuild(&(Arc::new(r) as ArrayRef)).unwrap()),
            vec![Some("x".to_string()), None, None]
        );
    }
}
