use super::geo_item::{GeoItem, NativeGeoItem};
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
pub struct DimenstionDefinitionStatic {
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

#[nativebridge::native_bridge(DimenstionDefinitionStatic)]
pub trait DimensionDefinition {
    #[optional]
    #[field]
    fn sql(&self) -> Result<Option<Rc<dyn MemberSql>>, CubeError>;

    #[optional]
    #[field]
    fn latitude(&self) -> Result<Option<Rc<dyn GeoItem>>, CubeError>;

    #[optional]
    #[field]
    fn longitude(&self) -> Result<Option<Rc<dyn GeoItem>>, CubeError>;
}
