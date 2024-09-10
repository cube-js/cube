use crate::cube_bridge::base_tools::{BaseTools, NativeBaseTools};
use crate::cube_bridge::evaluator::{CubeEvaluator, NativeCubeEvaluator};
use cubenativeutils::wrappers::serializer::{
    NativeDeserialize, NativeDeserializer, NativeSerialize,
};
use cubenativeutils::wrappers::NativeContextHolder;
use cubenativeutils::wrappers::NativeObjectHandle;
use cubenativeutils::CubeError;
use serde::{Deserialize, Serialize};
use std::rc::Rc;

#[derive(Serialize, Deserialize, Debug)]
pub struct TimeDimension {
    pub dimension: String,
    pub granularity: Option<String>,
    #[serde(rename = "dateRange")]
    pub date_range: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BaseQueryOptionsStatic {
    pub measures: Option<Vec<String>>,
    pub dimensions: Option<Vec<String>>,
    #[serde(rename = "timeDimensions")]
    pub time_dimensions: Option<Vec<TimeDimension>>,
    pub timezone: Option<String>,
    #[serde(rename = "joinRoot")]
    pub join_root: Option<String>, //TODO temporaty. join graph should be rewrited in rust or taked
                                   //from Js CubeCompiller
}

#[nativebridge::native_bridge(BaseQueryOptionsStatic)]
pub trait BaseQueryOptions {
    #[field]
    fn measures(&self) -> Result<Option<Vec<String>>, CubeError>;
    #[field]
    fn dimensions(&self) -> Result<Option<Vec<String>>, CubeError>;
    #[field]
    fn cube_evaluator(&self) -> Result<Rc<dyn CubeEvaluator>, CubeError>;
    #[field]
    fn base_tools(&self) -> Result<Rc<dyn BaseTools>, CubeError>;
}
