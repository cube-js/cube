use super::cube_definition::{CubeDefinition, NativeCubeDefinition};
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
pub struct SchemaSourceStatic {
    #[serde(rename = "primaryKeys")]
    pub primary_keys: HashMap<String, Vec<String>>,
}

/// Build-phase bridge: feeds `model::ModelBuilder` from JS.
///
/// Separate from `CubeEvaluator` on purpose. `CubeEvaluator` is the
/// runtime bridge (path lookups, per-query helpers); `SchemaSource`
/// exists only for one-shot schema enumeration during model build.
#[nativebridge::native_bridge(SchemaSourceStatic)]
pub trait SchemaSource {
    #[nbridge(vec)]
    fn cubes(&self) -> Result<Vec<Rc<dyn CubeDefinition>>, CubeError>;
}
