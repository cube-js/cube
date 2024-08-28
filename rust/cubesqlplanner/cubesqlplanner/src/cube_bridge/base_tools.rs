use super::cube_definition::{CubeDefinition, NativeCubeDefinition};
use super::dimension_definition::{DimensionDefinition, NativeDimensionDefinition};
use super::measure_definition::{MeasureDefinition, NativeMeasureDefinition};
use super::sql_templates_render::{NativeSqlTemplatesRender, SqlTemplatesRender};
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
pub trait BaseTools {
    fn convert_tz(&self, field: String) -> Result<String, CubeError>;
    fn time_grouped_column(
        &self,
        granularity: String,
        dimension: String,
    ) -> Result<String, CubeError>;
    fn sql_templates(&self) -> Result<Rc<dyn SqlTemplatesRender>, CubeError>;
}
