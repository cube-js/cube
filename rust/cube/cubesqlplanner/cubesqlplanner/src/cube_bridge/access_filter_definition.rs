use cubenativeutils::wrappers::serializer::{
    NativeDeserialize, NativeDeserializer, NativeSerialize,
};
use cubenativeutils::wrappers::NativeArray;
use cubenativeutils::wrappers::NativeContextHolder;
use cubenativeutils::wrappers::NativeObjectHandle;
use cubenativeutils::CubeError;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::rc::Rc;

/// `accessPolicy[].rowLevel.filters[]` shape. Either a leaf (member +
/// operator + values) or an `and`/`or` recursive group.
#[derive(Serialize, Deserialize, Debug)]
pub struct AccessFilterDefinitionStatic {
    /// Resolved by `prepareAccessPolicy` from the source `member` field.
    #[serde(rename = "memberReference")]
    pub member_reference: Option<String>,
    pub operator: Option<String>,
    #[serde(default)]
    pub values: Vec<String>,
}

#[nativebridge::native_bridge(AccessFilterDefinitionStatic)]
pub trait AccessFilterDefinition {
    #[nbridge(field, vec, optional)]
    fn and(&self) -> Result<Option<Vec<Rc<dyn AccessFilterDefinition>>>, CubeError>;
    #[nbridge(field, vec, optional)]
    fn or(&self) -> Result<Option<Vec<Rc<dyn AccessFilterDefinition>>>, CubeError>;
}
