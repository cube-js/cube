use super::cube_definition::{CubeDefinition, NativeCubeDefinition};
use super::dimension_definition::{DimensionDefinition, NativeDimensionDefinition};
use super::measure_definition::{MeasureDefinition, NativeMeasureDefinition};
use super::member_sql::{MemberSql, NativeMemberSql};
use cubenativeutils::wrappers::serializer::{
    NativeDeserialize, NativeDeserializer, NativeSerialize,
};
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

#[derive(Deserialize, Debug)]
pub struct CallDep {
    pub name: String,
    pub parent: Option<usize>,
}

#[nativebridge::native_bridge(CubeEvaluatorStatic)]
pub trait CubeEvaluator {
    #[field]
    fn primary_keys(&self) -> Result<HashMap<String, String>, CubeError>;
    fn parse_path(&self, path_type: String, path: String) -> Result<Vec<String>, CubeError>;
    fn measure_by_path(&self, measure_path: String)
        -> Result<Rc<dyn MeasureDefinition>, CubeError>;
    fn dimension_by_path(
        &self,
        measure_path: String,
    ) -> Result<Rc<dyn DimensionDefinition>, CubeError>;
    fn cube_from_path(&self, cube_path: String) -> Result<Rc<dyn CubeDefinition>, CubeError>;
    fn is_measure(&self, path: Vec<String>) -> Result<bool, CubeError>;
    fn is_dimension(&self, path: Vec<String>) -> Result<bool, CubeError>;
    fn cube_exists(&self, name: String) -> Result<bool, CubeError>;
    fn resolve_symbols_call_deps(
        &self,
        cube_name: String,
        sql: Rc<dyn MemberSql>,
    ) -> Result<Vec<CallDep>, CubeError>;
}
