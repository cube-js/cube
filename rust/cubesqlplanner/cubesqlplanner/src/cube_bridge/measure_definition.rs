use super::cube_definition::{CubeDefinition, NativeCubeDefinition};
use super::measure_filter::{MeasureFiltersVec, NativeMeasureFiltersVec};
use super::member_order_by::{MemberOrderByVec, NativeMemberOrderByVec};
use super::memeber_sql::{MemberSql, NativeMemberSql};
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
pub struct MeasureDefinitionStatic {
    #[serde(rename = "type")]
    pub measure_type: String,
    pub owned_by_cube: Option<bool>,
    #[serde(rename = "multiStage")]
    pub multi_stage: Option<bool>,
    #[serde(rename = "reduceByReferences")]
    pub reduce_by_references: Option<Vec<String>>,
    #[serde(rename = "addGroupByReferences")]
    pub add_group_by_references: Option<Vec<String>>,
    #[serde(rename = "groupByReferences")]
    pub group_by_references: Option<Vec<String>>,
}

#[nativebridge::native_bridge(MeasureDefinitionStatic)]
pub trait MeasureDefinition {
    #[optional]
    #[field]
    fn sql(&self) -> Result<Option<Rc<dyn MemberSql>>, CubeError>;

    fn cube(&self) -> Result<Rc<dyn CubeDefinition>, CubeError>;

    #[optional]
    #[field]
    fn filters(&self) -> Result<Option<Rc<dyn MeasureFiltersVec>>, CubeError>;

    #[optional]
    #[field]
    fn order_by(&self) -> Result<Option<Rc<dyn MemberOrderByVec>>, CubeError>;
}
