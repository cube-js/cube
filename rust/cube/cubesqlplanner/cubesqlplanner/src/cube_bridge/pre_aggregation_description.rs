use super::member_sql::{MemberSql, NativeMemberSql};
use super::pre_aggregation_index_definition::{
    NativePreAggregationIndexDefinition, PreAggregationIndexDefinition,
};
use super::pre_aggregation_time_dimension::{
    NativePreAggregationTimeDimension, PreAggregationTimeDimension,
};
use super::refresh_key_definition::{NativeRefreshKeyDefinition, RefreshKeyDefinition};
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
pub struct PreAggregationDescriptionStatic {
    pub name: String,
    #[serde(rename = "type")]
    pub pre_aggregation_type: String,
    pub granularity: Option<String>,
    #[serde(rename = "sqlAlias")]
    pub sql_alias: Option<String>,
    pub external: Option<bool>,
    #[serde(rename = "allowNonStrictDateRangeMatch")]
    pub allow_non_strict_date_range_match: Option<bool>,
    #[serde(rename = "scheduledRefresh")]
    pub scheduled_refresh: Option<bool>,
    #[serde(rename = "useOriginalSqlPreAggregations")]
    pub use_original_sql_pre_aggregations: Option<bool>,
    #[serde(rename = "partitionGranularity")]
    pub partition_granularity: Option<String>,
    #[serde(rename = "ownedByCube")]
    pub owned_by_cube: Option<bool>,
}

#[nativebridge::native_bridge(PreAggregationDescriptionStatic, with_static_meta)]
pub trait PreAggregationDescription {
    #[nbridge(field, optional)]
    fn measure_references(&self) -> Result<Option<Rc<dyn MemberSql>>, CubeError>;

    #[nbridge(field, optional)]
    fn dimension_references(&self) -> Result<Option<Rc<dyn MemberSql>>, CubeError>;

    #[nbridge(field, optional)]
    fn time_dimension_reference(&self) -> Result<Option<Rc<dyn MemberSql>>, CubeError>;

    #[nbridge(field, optional)]
    fn segment_references(&self) -> Result<Option<Rc<dyn MemberSql>>, CubeError>;

    #[nbridge(field, optional)]
    fn rollup_references(&self) -> Result<Option<Rc<dyn MemberSql>>, CubeError>;

    #[nbridge(field, vec, optional)]
    fn time_dimension_references(
        &self,
    ) -> Result<Option<Vec<Rc<dyn PreAggregationTimeDimension>>>, CubeError>;
    #[nbridge(field, optional, rename = "refreshRangeStart")]
    fn build_range_start(&self) -> Result<Option<Rc<dyn MemberSql>>, CubeError>;
    #[nbridge(field, optional, rename = "refreshRangeEnd")]
    fn build_range_end(&self) -> Result<Option<Rc<dyn MemberSql>>, CubeError>;
    #[nbridge(field, vec, optional)]
    fn indexes(&self) -> Result<Option<Vec<Rc<dyn PreAggregationIndexDefinition>>>, CubeError>;
    #[nbridge(field, optional, rename = "refreshKey")]
    fn refresh_key(&self) -> Result<Option<Rc<dyn RefreshKeyDefinition>>, CubeError>;
}
