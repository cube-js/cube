use super::access_condition_definition::{
    AccessConditionDefinition, NativeAccessConditionDefinition,
};
use super::member_level_access_definition::{
    MemberLevelAccessDefinition, NativeMemberLevelAccessDefinition,
};
use super::row_level_access_definition::{
    NativeRowLevelAccessDefinition, RowLevelAccessDefinition,
};
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

#[derive(Serialize, Deserialize, Debug)]
pub struct AccessPolicyDefinitionStatic {
    pub role: Option<String>,
    pub group: Option<String>,
    #[serde(default)]
    pub groups: Vec<String>,
}

/// Access policy declared on a cube.
#[nativebridge::native_bridge(AccessPolicyDefinitionStatic)]
pub trait AccessPolicyDefinition {
    #[nbridge(field, optional, rename = "memberLevel")]
    fn member_level(&self) -> Result<Option<Rc<dyn MemberLevelAccessDefinition>>, CubeError>;
    #[nbridge(field, optional, rename = "memberMasking")]
    fn member_masking(&self) -> Result<Option<Rc<dyn MemberLevelAccessDefinition>>, CubeError>;
    #[nbridge(field, optional, rename = "rowLevel")]
    fn row_level(&self) -> Result<Option<Rc<dyn RowLevelAccessDefinition>>, CubeError>;
    #[nbridge(field, vec, optional)]
    fn conditions(&self) -> Result<Option<Vec<Rc<dyn AccessConditionDefinition>>>, CubeError>;
}
