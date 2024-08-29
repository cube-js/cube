use super::cube_definition::{CubeDefinition, NativeCubeDefinition};
use super::dimension_definition::{DimensionDefinition, NativeDimensionDefinition};
use super::join_definition::{JoinDefinition, NativeJoinDefinition};
use super::measure_definition::{MeasureDefinition, NativeMeasureDefinition};
use super::memeber_sql::{MemberSql, NativeMemberSql};
use cubenativeutils::wrappers::serializer::{
    NativeDeserialize, NativeDeserializer, NativeSerialize,
};
use cubenativeutils::wrappers::NativeContextHolder;
use cubenativeutils::wrappers::NativeObjectHandle;
use cubenativeutils::CubeError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::rc::Rc;

#[nativebridge::native_bridge]
pub trait JoinGraph {
    fn build_join(&self, cubes_to_join: Vec<String>) -> Result<Rc<dyn JoinDefinition>, CubeError>;
}
