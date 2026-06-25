use cubenativeutils::wrappers::serializer::{NativeDeserialize, NativeSerialize};
use cubenativeutils::wrappers::NativeContextHolder;
use cubenativeutils::wrappers::NativeObjectHandle;
use cubenativeutils::CubeError;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::rc::Rc;

#[derive(Serialize, Deserialize, Debug)]
pub struct ViewIncludedMemberStatic {
    /// `measures` | `dimensions` | `segments` | `hierarchies`.
    #[serde(rename = "type")]
    pub member_kind: String,
    /// Path to the source member on the underlying cube
    /// (`"Cube.member"`).
    #[serde(rename = "memberPath")]
    pub member_path: String,
    /// Local name surfaced by the view.
    pub name: String,
}

#[nativebridge::native_bridge(ViewIncludedMemberStatic)]
pub trait ViewIncludedMember {}
