use super::member_sql::{MemberSql, NativeMemberSql};
use cubenativeutils::wrappers::serializer::{
    NativeDeserialize, NativeDeserializer, NativeSerialize,
};
use cubenativeutils::wrappers::NativeContextHolder;
use cubenativeutils::wrappers::NativeObjectHandle;
use cubenativeutils::CubeError;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::rc::Rc;

#[derive(Serialize, Deserialize, Debug)]
pub struct CubeDefinitionStatic {
    pub name: String,
    #[serde(rename = "sqlAlias")]
    pub sql_alias: Option<String>,
}

#[nativebridge::native_bridge(CubeDefinitionStatic)]
pub trait CubeDefinition {
    #[field]
    #[optional]
    fn sql_table(&self) -> Result<Option<Rc<dyn MemberSql>>, CubeError>;
    #[field]
    #[optional]
    fn sql(&self) -> Result<Option<Rc<dyn MemberSql>>, CubeError>;
}
