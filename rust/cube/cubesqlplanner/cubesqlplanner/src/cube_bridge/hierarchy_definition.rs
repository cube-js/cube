use cubenativeutils::wrappers::serializer::{NativeDeserialize, NativeSerialize};
use cubenativeutils::wrappers::NativeContextHolder;
use cubenativeutils::wrappers::NativeObjectHandle;
use cubenativeutils::CubeError;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::rc::Rc;

#[derive(Serialize, Deserialize, Debug)]
pub struct HierarchyDefinitionStatic {
    pub name: String,
    #[serde(default)]
    pub levels: Vec<String>,
    pub title: Option<String>,
    pub public: Option<bool>,
    #[serde(rename = "aliasMember")]
    pub alias_member: Option<String>,
}

#[nativebridge::native_bridge(HierarchyDefinitionStatic)]
pub trait HierarchyDefinition {}
