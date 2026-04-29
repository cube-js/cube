use super::base_query_options::FilterItem as NativeFilterItem;
use cubenativeutils::wrappers::serializer::{NativeDeserialize, NativeSerialize};
use cubenativeutils::wrappers::NativeContextHolder;
use cubenativeutils::wrappers::NativeObjectHandle;
use cubenativeutils::CubeError;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::rc::Rc;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MultiStageFilterReferencesStatic {
    pub mode: Option<String>,
    #[serde(rename = "excludeReferences")]
    pub exclude: Option<Vec<String>>,
    #[serde(rename = "keepOnlyReferences")]
    pub keep_only: Option<Vec<String>>,
    pub include: Option<Vec<NativeFilterItem>>,
}

#[nativebridge::native_bridge(MultiStageFilterReferencesStatic)]
pub trait MultiStageFilterReferences {}
