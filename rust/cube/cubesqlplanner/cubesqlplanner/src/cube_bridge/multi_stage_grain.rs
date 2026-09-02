use cubenativeutils::wrappers::serializer::{NativeDeserialize, NativeSerialize};
use cubenativeutils::wrappers::NativeContextHolder;
use cubenativeutils::wrappers::NativeObjectHandle;
use cubenativeutils::CubeError;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::rc::Rc;

#[derive(Serialize, Deserialize, Debug, Clone, nativebridge::NativeBridgeStatic)]
pub struct MultiStageGrainReferencesStatic {
    #[serde(rename = "excludeReferences")]
    pub exclude: Option<Vec<String>>,
    #[serde(rename = "keepOnlyReferences")]
    pub keep_only: Option<Vec<String>>,
    #[serde(rename = "includeReferences")]
    pub include: Option<Vec<String>>,
}

#[nativebridge::native_bridge(MultiStageGrainReferencesStatic, with_static_meta)]
pub trait MultiStageGrainReferences {}
