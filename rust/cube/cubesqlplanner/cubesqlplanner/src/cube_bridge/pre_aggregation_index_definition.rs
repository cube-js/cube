use cubenativeutils::wrappers::serializer::{NativeDeserialize, NativeSerialize};
use cubenativeutils::wrappers::NativeContextHolder;
use cubenativeutils::wrappers::NativeObjectHandle;
use cubenativeutils::CubeError;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::rc::Rc;

#[derive(Serialize, Deserialize, Debug)]
pub struct PreAggregationIndexDefinitionStatic {
    /// Stamped by `SchemaSource.cubes()` from the Record key.
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub columns: Vec<String>,
    /// `regular` | `aggregate`. Optional in the schema — defaults to
    /// `regular` if missing.
    #[serde(rename = "type")]
    pub index_type: Option<String>,
}

#[nativebridge::native_bridge(PreAggregationIndexDefinitionStatic)]
pub trait PreAggregationIndexDefinition {}
