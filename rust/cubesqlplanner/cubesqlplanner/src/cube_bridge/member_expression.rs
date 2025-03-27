use super::member_sql::{MemberSql, NativeMemberSql};
use cubenativeutils::wrappers::serializer::{
    NativeDeserialize, NativeDeserializer, NativeSerialize,
};
use cubenativeutils::wrappers::{NativeContextHolder, NativeObjectHandle};
use cubenativeutils::CubeError;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::rc::Rc;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MemberExpressionDefinitionStatic {
    #[serde(rename = "expressionName")]
    pub expression_name: Option<String>,
    #[serde(rename = "cubeName")]
    pub cube_name: Option<String>,
    pub definition: Option<String>,
}

#[nativebridge::native_bridge(MemberExpressionDefinitionStatic)]
pub trait MemberExpressionDefinition {
    #[nbridge(field)]
    fn expression(&self) -> Result<Rc<dyn MemberSql>, CubeError>;
}
