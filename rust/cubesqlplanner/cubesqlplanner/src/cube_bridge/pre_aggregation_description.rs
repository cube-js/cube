use super::member_sql::{MemberSql, NativeMemberSql};
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
pub struct PreAggregationDescriptionStatic {
    pub name: String,
    #[serde(rename = "type")]
    pub pre_aggregation_type: String,
    pub granularity: Option<String>,
    pub external: Option<bool>,
}

#[nativebridge::native_bridge(PreAggregationDescriptionStatic)]
pub trait PreAggregationDescription {
    #[nbridge(field, optional)]
    fn measure_references(&self) -> Result<Option<Rc<dyn MemberSql>>, CubeError>;

    #[nbridge(field, optional)]
    fn dimension_references(&self) -> Result<Option<Rc<dyn MemberSql>>, CubeError>;

    #[nbridge(field, optional)]
    fn time_dimension_reference(&self) -> Result<Option<Rc<dyn MemberSql>>, CubeError>;
}
