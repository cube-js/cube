use super::access_policy_definition::{AccessPolicyDefinition, NativeAccessPolicyDefinition};
use super::cube_join_definition::{CubeJoinDefinition, NativeCubeJoinDefinition};
use super::dimension_definition::{DimensionDefinition, NativeDimensionDefinition};
use super::measure_definition::{MeasureDefinition, NativeMeasureDefinition};
use super::member_sql::{MemberSql, NativeMemberSql};
use super::pre_aggregation_description::{
    NativePreAggregationDescription, PreAggregationDescription,
};
use super::segment_definition::{NativeSegmentDefinition, SegmentDefinition};
use super::view_filter_definition::{NativeViewFilterDefinition, ViewFilterDefinition};
use super::view_included_member::{NativeViewIncludedMember, ViewIncludedMember};
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
    #[nbridge(field, vec)]
    fn measures(&self) -> Result<Vec<Rc<dyn MeasureDefinition>>, CubeError>;
    #[nbridge(field, vec)]
    fn dimensions(&self) -> Result<Vec<Rc<dyn DimensionDefinition>>, CubeError>;
    #[nbridge(field, vec)]
    fn segments(&self) -> Result<Vec<Rc<dyn SegmentDefinition>>, CubeError>;
    #[nbridge(field, vec, optional)]
    fn joins(&self) -> Result<Option<Vec<Rc<dyn CubeJoinDefinition>>>, CubeError>;
    #[nbridge(field, vec, optional, rename = "preAggregations")]
    fn pre_aggregations(&self)
        -> Result<Option<Vec<Rc<dyn PreAggregationDescription>>>, CubeError>;
    #[nbridge(field, vec, optional, rename = "accessPolicy")]
    fn access_policies(&self) -> Result<Option<Vec<Rc<dyn AccessPolicyDefinition>>>, CubeError>;
    #[nbridge(field, vec, optional, rename = "includedMembers")]
    fn included_members(&self) -> Result<Option<Vec<Rc<dyn ViewIncludedMember>>>, CubeError>;
}
