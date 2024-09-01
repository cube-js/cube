use super::memeber_sql::{MemberSql, NativeMemberSql};
use cubenativeutils::wrappers::serializer::{
    NativeDeserialize, NativeDeserializer, NativeSerialize,
};
use cubenativeutils::wrappers::NativeContextHolder;
use cubenativeutils::wrappers::NativeObjectHandle;
use cubenativeutils::CubeError;
use serde::{Deserialize, Serialize};
use std::rc::Rc;

#[derive(Serialize, Deserialize, Debug)]
pub struct CubeDefinitionStatic {
    pub name: String,
}

#[nativebridge::native_bridge(CubeDefinitionStatic)]
pub trait CubeDefinition {
    #[field]
    fn sql_table(&self) -> Result<Rc<dyn MemberSql>, CubeError>;
}
