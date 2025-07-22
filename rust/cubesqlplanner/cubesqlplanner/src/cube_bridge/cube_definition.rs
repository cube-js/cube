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
    #[serde(rename = "isView")]
    pub is_view: Option<bool>,
    #[serde(rename = "calendar")]
    pub is_calendar: Option<bool>,
}

impl CubeDefinitionStatic {
    pub fn resolved_alias(&self) -> &String {
        if let Some(alias) = &self.sql_alias {
            alias
        } else {
            &self.name
        }
    }
}

#[nativebridge::native_bridge(CubeDefinitionStatic)]
pub trait CubeDefinition {
    #[nbridge(field, optional)]
    fn sql_table(&self) -> Result<Option<Rc<dyn MemberSql>>, CubeError>;
    #[nbridge(field, optional)]
    fn sql(&self) -> Result<Option<Rc<dyn MemberSql>>, CubeError>;
}
