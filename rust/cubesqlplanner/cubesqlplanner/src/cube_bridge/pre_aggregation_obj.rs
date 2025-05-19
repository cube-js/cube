use cubenativeutils::wrappers::serializer::{NativeDeserialize, NativeSerialize};
use cubenativeutils::wrappers::NativeContextHolder;
use cubenativeutils::wrappers::NativeObjectHandle;
use cubenativeutils::CubeError;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::rc::Rc;

#[derive(Serialize, Deserialize, Debug)]
pub struct PreAggregationObjStatic {
    #[serde(rename = "tableName")]
    pub table_name: Option<String>,
    #[serde(rename = "preAggregationName")]
    pub pre_aggregation_name: Option<String>,
    pub cube: Option<String>,
    #[serde(rename = "preAggregationId")]
    pub pre_aggregation_id: Option<String>,
}

#[nativebridge::native_bridge(PreAggregationObjStatic)]
pub trait PreAggregationObj {}
