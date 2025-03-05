use super::cube_definition::{CubeDefinition, NativeCubeDefinition};
use super::member_order_by::{MemberOrderBy, NativeMemberOrderBy};
use super::member_sql::{MemberSql, NativeMemberSql};
use super::struct_with_sql_member::{NativeStructWithSqlMember, StructWithSqlMember};
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

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct TimeShiftReference {
    pub interval: String,
    #[serde(rename = "type")]
    pub shift_type: Option<String>,
    #[serde(rename = "timeDimension")]
    pub time_dimension: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct RollingWindow {
    pub trailing: Option<String>,
    pub leading: Option<String>,
    pub offset: Option<String>,
    #[serde(rename = "type")]
    pub rolling_type: Option<String>,
    pub granularity: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MeasureDefinitionStatic {
    #[serde(rename = "type")]
    pub measure_type: String,
    #[serde(rename = "ownedByCube")]
    pub owned_by_cube: Option<bool>,
    #[serde(rename = "multiStage")]
    pub multi_stage: Option<bool>,
    #[serde(rename = "reduceByReferences")]
    pub reduce_by_references: Option<Vec<String>>,
    #[serde(rename = "addGroupByReferences")]
    pub add_group_by_references: Option<Vec<String>>,
    #[serde(rename = "groupByReferences")]
    pub group_by_references: Option<Vec<String>>,
    #[serde(rename = "timeShiftReferences")]
    pub time_shift_references: Option<Vec<TimeShiftReference>>,
    #[serde(rename = "rollingWindow")]
    pub rolling_window: Option<RollingWindow>,
}

#[nativebridge::native_bridge(MeasureDefinitionStatic)]
pub trait MeasureDefinition {
    #[nbridge(field, optional)]
    fn sql(&self) -> Result<Option<Rc<dyn MemberSql>>, CubeError>;

    fn cube(&self) -> Result<Rc<dyn CubeDefinition>, CubeError>;

    #[nbridge(field, optional, vec)]
    fn filters(&self) -> Result<Option<Vec<Rc<dyn StructWithSqlMember>>>, CubeError>;

    #[nbridge(field, optional, vec)]
    fn drill_filters(&self) -> Result<Option<Vec<Rc<dyn StructWithSqlMember>>>, CubeError>;

    #[nbridge(field, optional, vec)]
    fn order_by(&self) -> Result<Option<Vec<Rc<dyn MemberOrderBy>>>, CubeError>;
}
