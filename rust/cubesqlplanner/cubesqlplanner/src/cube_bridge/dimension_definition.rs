use super::case_definition::{CaseDefinition, NativeCaseDefinition};
use super::geo_item::{GeoItem, NativeGeoItem};
use super::member_sql::{MemberSql, NativeMemberSql};
use crate::cube_bridge::timeshift_definition::{NativeTimeShiftDefinition, TimeShiftDefinition};
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
pub struct DimensionDefinitionStatic {
    #[serde(rename = "type")]
    pub dimension_type: String,
    #[serde(rename = "ownedByCube")]
    pub owned_by_cube: Option<bool>,
    #[serde(rename = "multiStage")]
    pub multi_stage: Option<bool>,
    #[serde(rename = "subQuery")]
    pub sub_query: Option<bool>,
    #[serde(rename = "propagateFiltersToSubQuery")]
    pub propagate_filters_to_sub_query: Option<bool>,
}

#[nativebridge::native_bridge(DimensionDefinitionStatic)]
pub trait DimensionDefinition {
    #[nbridge(field, optional)]
    fn sql(&self) -> Result<Option<Rc<dyn MemberSql>>, CubeError>;

    #[nbridge(field, optional)]
    fn case(&self) -> Result<Option<Rc<dyn CaseDefinition>>, CubeError>;

    #[nbridge(field, optional)]
    fn latitude(&self) -> Result<Option<Rc<dyn GeoItem>>, CubeError>;

    #[nbridge(field, optional)]
    fn longitude(&self) -> Result<Option<Rc<dyn GeoItem>>, CubeError>;

    #[nbridge(field, vec, optional)]
    fn time_shift(&self) -> Result<Option<Vec<Rc<dyn TimeShiftDefinition>>>, CubeError>;
}
