use cubenativeutils::wrappers::serializer::{NativeDeserialize, NativeSerialize};
use cubenativeutils::wrappers::NativeContextHolder;
use cubenativeutils::wrappers::NativeObjectHandle;
use cubenativeutils::CubeError;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::rc::Rc;

/// Shared shape for `memberLevel` and `memberMasking` on an access
/// policy. After `prepareAccessPolicy` includes / excludes are resolved
/// into qualified-name lists.
#[derive(Serialize, Deserialize, Debug)]
pub struct MemberLevelAccessDefinitionStatic {
    #[serde(rename = "includesMembers", default)]
    pub includes_members: Vec<String>,
    #[serde(rename = "excludesMembers", default)]
    pub excludes_members: Vec<String>,
}

#[nativebridge::native_bridge(MemberLevelAccessDefinitionStatic)]
pub trait MemberLevelAccessDefinition {}
