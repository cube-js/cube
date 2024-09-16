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
pub struct BaseQueryOptionsStatic {
    pub measures: Option<Vec<String>>,
    pub dimensions: Option<Vec<String>>,
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
}
