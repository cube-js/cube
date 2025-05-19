use super::cube_definition::{CubeDefinition, NativeCubeDefinition};
use super::dimension_definition::{
    DimensionDefinition, GranularityDefinition, NativeDimensionDefinition,
};
use super::measure_definition::{MeasureDefinition, NativeMeasureDefinition};
use super::member_sql::{MemberSql, NativeMemberSql};
use super::pre_aggregation_description::{
    NativePreAggregationDescription, PreAggregationDescription,
};
use super::segment_definition::{NativeSegmentDefinition, SegmentDefinition};
use cubenativeutils::wrappers::serializer::{
    NativeDeserialize, NativeDeserializer, NativeSerialize,
};
use cubenativeutils::wrappers::NativeArray;
use cubenativeutils::wrappers::NativeContextHolder;
use cubenativeutils::wrappers::NativeObjectHandle;
use cubenativeutils::CubeError;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::HashMap;
use std::rc::Rc;

#[derive(Serialize, Deserialize, Debug)]
pub struct CubeEvaluatorStatic {
    #[serde(rename = "primaryKeys")]
    pub primary_keys: HashMap<String, Vec<String>>,
}

#[derive(Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
pub struct CallDep {
    pub name: String,
    pub parent: Option<usize>,
}

#[nativebridge::native_bridge(CubeEvaluatorStatic)]
pub trait CubeEvaluator {
    #[nbridge(field)]
    fn primary_keys(&self) -> Result<HashMap<String, String>, CubeError>;
    fn parse_path(&self, path_type: String, path: String) -> Result<Vec<String>, CubeError>;
    fn measure_by_path(&self, measure_path: String)
        -> Result<Rc<dyn MeasureDefinition>, CubeError>;
    fn dimension_by_path(
        &self,
        dimension_path: String,
    ) -> Result<Rc<dyn DimensionDefinition>, CubeError>;
    fn segment_by_path(&self, segment_path: String)
        -> Result<Rc<dyn SegmentDefinition>, CubeError>;
    fn cube_from_path(&self, cube_path: String) -> Result<Rc<dyn CubeDefinition>, CubeError>;
    fn is_measure(&self, path: Vec<String>) -> Result<bool, CubeError>;
    fn is_dimension(&self, path: Vec<String>) -> Result<bool, CubeError>;
    fn cube_exists(&self, name: String) -> Result<bool, CubeError>;
    fn resolve_symbols_call_deps(
        &self,
        cube_name: String,
        sql: Rc<dyn MemberSql>,
    ) -> Result<Vec<CallDep>, CubeError>;
    fn resolve_granularity(&self, path: Vec<String>) -> Result<GranularityDefinition, CubeError>;
    #[nbridge(vec)]
    fn pre_aggregations_for_cube_as_array(
        &self,
        cube_name: String,
    ) -> Result<Vec<Rc<dyn PreAggregationDescription>>, CubeError>;
}
