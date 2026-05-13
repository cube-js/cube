use cubenativeutils::wrappers::serializer::{NativeDeserialize, NativeSerialize};
use cubenativeutils::wrappers::NativeContextHolder;
use cubenativeutils::wrappers::NativeObjectHandle;
use cubenativeutils::CubeError;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::rc::Rc;

// `values_references` mirrors the contract of `FilterItem.values` from
// `base_query_options.rs` — query filters arriving from the API are already
// stringified there, so the Tesseract planner treats filter values as
// `Option<Vec<Option<String>>>` everywhere. The JS evaluator coerces with
// `String(v)` before populating this field.
#[derive(Serialize, Deserialize, Debug, Clone, nativebridge::NativeBridgeStatic)]
pub struct ViewFilterDefinitionStatic {
    pub operator: String,
    #[serde(rename = "memberReference")]
    pub member_reference: String,
    #[serde(rename = "valuesReferences")]
    pub values_references: Option<Vec<Option<String>>>,
    #[serde(rename = "unlessReferences")]
    pub unless_references: Option<Vec<String>>,
}

#[nativebridge::native_bridge(ViewFilterDefinitionStatic, with_static_meta)]
pub trait ViewFilterDefinition {}
