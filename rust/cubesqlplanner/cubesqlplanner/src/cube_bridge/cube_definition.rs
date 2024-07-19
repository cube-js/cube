use crate::cube_bridge::evaluator::{CubeEvaluator, NativeCubeEvaluator};
use cubenativeutils::wrappers::serializer::{
    NativeDeserialize, NativeDeserializer, NativeSerialize,
};
use cubenativeutils::wrappers::NativeContextHolder;
use cubenativeutils::wrappers::NativeObjectHandle;
use cubenativeutils::CubeError;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct CubeDefinitionStatic {
    pub name: String,
}

#[nativebridge::native_bridge(CubeDefinitionStatic)]
pub trait CubeDefinition {
    fn sql_table(&self) -> Result<String, CubeError>;
}

/*
console.log
      !!! -- from path {
        allDefinitions: [Function: allDefinitions],
        measures: [Getter/Setter],
        dimensions: [Getter/Setter],
        segments: [Getter/Setter],
        name: 'cards',
        sqlTable: [Function: sqlTable],
        preAggregations: {},
        joins: {},
        fileName: 'main.yml'
      }
 */
