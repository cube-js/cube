use super::join_graph::{JoinGraph, NativeJoinGraph};
use super::options_member::OptionsMember;
use crate::cube_bridge::base_tools::{BaseTools, NativeBaseTools};
use crate::cube_bridge::evaluator::{CubeEvaluator, NativeCubeEvaluator};
use cubenativeutils::wrappers::serializer::{
    NativeDeserialize, NativeDeserializer, NativeSerialize,
};
use cubenativeutils::wrappers::{NativeArray, NativeContextHolder, NativeObjectHandle};
use cubenativeutils::CubeError;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::rc::Rc;

#[derive(Serialize, Deserialize, Debug)]
pub struct TimeDimension {
    pub dimension: String,
    pub granularity: Option<String>,
    #[serde(rename = "dateRange")]
    pub date_range: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FilterItem {
    pub or: Option<Vec<FilterItem>>,
    pub and: Option<Vec<FilterItem>>,
    pub member: Option<String>,
    pub dimension: Option<String>,
    pub operator: Option<String>,
    pub values: Option<Vec<Option<String>>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct OrderByItem {
    pub id: String,
    pub desc: Option<bool>,
}

impl OrderByItem {
    pub fn is_desc(&self) -> bool {
        self.desc.unwrap_or(false)
    }
}

impl FilterItem {
    pub fn member(&self) -> Option<&String> {
        self.member.as_ref().or(self.dimension.as_ref())
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BaseQueryOptionsStatic {
    #[serde(rename = "timeDimensions")]
    pub time_dimensions: Option<Vec<TimeDimension>>,
    pub timezone: Option<String>,
    pub filters: Option<Vec<FilterItem>>,
    pub order: Option<Vec<OrderByItem>>,
    pub limit: Option<String>,
    #[serde(rename = "rowLimit")]
    pub row_limit: Option<String>,
    pub offset: Option<String>,
    pub ungrouped: Option<bool>,
}

#[nativebridge::native_bridge(BaseQueryOptionsStatic)]
pub trait BaseQueryOptions {
    #[nbridge(field, optional, vec)]
    fn measures(&self) -> Result<Option<Vec<OptionsMember>>, CubeError>;
    #[nbridge(field, optional, vec)]
    fn dimensions(&self) -> Result<Option<Vec<OptionsMember>>, CubeError>;
    #[nbridge(field)]
    fn cube_evaluator(&self) -> Result<Rc<dyn CubeEvaluator>, CubeError>;
    #[nbridge(field)]
    fn base_tools(&self) -> Result<Rc<dyn BaseTools>, CubeError>;
    #[nbridge(field)]
    fn join_graph(&self) -> Result<Rc<dyn JoinGraph>, CubeError>;
}
