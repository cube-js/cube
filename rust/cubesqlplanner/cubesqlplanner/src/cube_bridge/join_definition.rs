use super::join_item::{JoinItemsVec, NativeJoinItemsVec};
use cubenativeutils::wrappers::serializer::{
    NativeDeserialize, NativeDeserializer, NativeSerialize,
};
use cubenativeutils::wrappers::NativeContextHolder;
use cubenativeutils::wrappers::NativeObjectHandle;
use cubenativeutils::CubeError;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::HashMap;
use std::rc::Rc;

#[derive(Serialize, Deserialize, Debug)]
pub struct JoinDefinitionStatic {
    pub root: String,
    #[serde(rename = "multiplicationFactor")]
    pub multiplication_factor: HashMap<String, bool>,
}

#[nativebridge::native_bridge(JoinDefinitionStatic)]
pub trait JoinDefinition {
    #[field]
    fn joins(&self) -> Result<Rc<dyn JoinItemsVec>, CubeError>;
}
