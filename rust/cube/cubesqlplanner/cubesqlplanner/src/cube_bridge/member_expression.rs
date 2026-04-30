use super::member_sql::{MemberSql, NativeMemberSql};
use super::struct_with_sql_member::{NativeStructWithSqlMember, StructWithSqlMember};
use cubenativeutils::wrappers::serializer::{
    NativeDeserialize, NativeDeserializer, NativeSerialize,
};
use cubenativeutils::wrappers::NativeArray;
use cubenativeutils::wrappers::{NativeContextHolder, NativeObjectHandle};
use cubenativeutils::CubeError;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::rc::Rc;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ExpressionStructStatic {
    #[serde(rename = "type")]
    pub expression_type: String,
    #[serde(rename = "sourceMeasure")]
    pub source_measure: Option<String>,
    #[serde(rename = "replaceAggregationType")]
    pub replace_aggregation_type: Option<String>,
}

#[nativebridge::native_bridge(ExpressionStructStatic)]
pub trait ExpressionStruct {
    #[nbridge(field, optional, vec)]
    fn add_filters(&self) -> Result<Option<Vec<Rc<dyn StructWithSqlMember>>>, CubeError>;
}

pub enum MemberExpressionExpressionDef {
    Sql(Rc<dyn MemberSql>),
    Struct(Rc<dyn ExpressionStruct>),
}

impl<IT: InnerTypes> NativeDeserialize<IT> for MemberExpressionExpressionDef {
    fn from_native(native_object: NativeObjectHandle<IT>) -> Result<Self, CubeError> {
        match NativeMemberSql::from_native(native_object.clone()) {
            Ok(sql) => Ok(Self::Sql(Rc::new(sql))),
            Err(_) => match NativeExpressionStruct::from_native(native_object) {
                Ok(expr) => Ok(Self::Struct(Rc::new(expr))),
                Err(_) => Err(CubeError::user(format!(
                    "Member sql or expression struct expected for member expression expression field"
                ))),
            },
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MemberExpressionDefinitionStatic {
    #[serde(rename = "expressionName")]
    pub expression_name: Option<String>,
    pub name: Option<String>,
    #[serde(rename = "cubeName")]
    pub cube_name: Option<String>,
    pub definition: Option<String>,
}

#[nativebridge::native_bridge(MemberExpressionDefinitionStatic, without_imports)]
pub trait MemberExpressionDefinition {
    #[nbridge(field)]
    fn expression(&self) -> Result<MemberExpressionExpressionDef, CubeError>;
}
