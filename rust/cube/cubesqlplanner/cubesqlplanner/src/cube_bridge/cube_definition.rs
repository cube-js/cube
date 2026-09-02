use super::member_sql::{MemberSql, NativeMemberSql};
use super::view_filter_definition::{NativeViewFilterDefinition, ViewFilterDefinition};
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

#[derive(Serialize, Deserialize, Debug, nativebridge::NativeBridgeStatic)]
pub struct CubeDefinitionStatic {
    pub name: String,
    #[serde(rename = "sqlAlias")]
    pub sql_alias: Option<String>,
    #[serde(rename = "isView")]
    pub is_view: Option<bool>,
    #[serde(rename = "calendar")]
    pub is_calendar: Option<bool>,
    #[serde(rename = "joinMap")]
    pub join_map: Option<Vec<Vec<String>>>,
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

#[nativebridge::native_bridge(CubeDefinitionStatic, with_static_meta)]
pub trait CubeDefinition {
    #[nbridge(field, optional)]
    fn sql_table(&self) -> Result<Option<Rc<dyn MemberSql>>, CubeError>;
    #[nbridge(field, optional)]
    fn sql(&self) -> Result<Option<Rc<dyn MemberSql>>, CubeError>;
    #[nbridge(field, optional, vec)]
    fn default_filters(&self) -> Result<Option<Vec<Rc<dyn ViewFilterDefinition>>>, CubeError>;
}
