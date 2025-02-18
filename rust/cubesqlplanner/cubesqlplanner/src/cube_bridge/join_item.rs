use super::join_item_definition::{JoinItemDefinition, NativeJoinItemDefinition};
use cubenativeutils::wrappers::serializer::{
    NativeDeserialize, NativeDeserializer, NativeSerialize,
};
use cubenativeutils::wrappers::NativeContextHolder;
use cubenativeutils::wrappers::NativeObjectHandle;
use cubenativeutils::CubeError;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::rc::Rc;

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Hash)]
pub struct JoinItemStatic {
    pub from: String,
    pub to: String,
    #[serde(rename = "originalFrom")]
    pub original_from: String,
    #[serde(rename = "originalTo")]
    pub original_to: String,
}

#[nativebridge::native_bridge(JoinItemStatic)]
pub trait JoinItem {
    #[field]
    fn join(&self) -> Result<Rc<dyn JoinItemDefinition>, CubeError>;
}
