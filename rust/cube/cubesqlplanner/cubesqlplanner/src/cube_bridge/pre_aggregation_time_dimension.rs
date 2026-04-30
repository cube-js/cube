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

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct PreAggregationTimeDimensionStatic {
    pub granularity: String,
}

#[nativebridge::native_bridge(PreAggregationTimeDimensionStatic)]
pub trait PreAggregationTimeDimension {
    #[nbridge(field)]
    fn dimension(&self) -> Result<Rc<dyn MemberSql>, CubeError>;
}
