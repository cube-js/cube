use super::cube_definition::{CubeDefinition, NativeCubeDefinition};
use super::dimension_definition::{DimensionDefinition, NativeDimensionDefinition};
use super::filter_group::{FilterGroup, NativeFilterGroup};
use super::filter_params::{FilterParams, NativeFilterParams};
use super::measure_definition::{MeasureDefinition, NativeMeasureDefinition};
use super::memeber_sql::{MemberSql, NativeMemberSql};
use super::security_context::{NativeSecurityContext, SecurityContext};
use super::sql_templates_render::{NativeSqlTemplatesRender, SqlTemplatesRender};
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

#[derive(Deserialize, Debug)]
pub struct CallDep {
    pub name: String,
    pub parent: Option<usize>,
}

#[nativebridge::native_bridge]
pub trait BaseTools {
    fn convert_tz(&self, field: String) -> Result<String, CubeError>;
    fn time_grouped_column(
        &self,
        granularity: String,
        dimension: String,
    ) -> Result<String, CubeError>;
    fn sql_templates(&self) -> Result<Rc<dyn SqlTemplatesRender>, CubeError>;
    fn resolve_symbols_call_deps(
        &self,
        cube_name: String,
        sql: Rc<dyn MemberSql>,
    ) -> Result<Vec<CallDep>, CubeError>;
    fn security_context_for_rust(&self) -> Result<Rc<dyn SecurityContext>, CubeError>;
    fn filters_proxy(&self) -> Result<Rc<dyn FilterParams>, CubeError>;
    fn filter_group_function(&self) -> Result<Rc<dyn FilterGroup>, CubeError>;
    fn timestamp_precision(&self) -> Result<u32, CubeError>;
    fn in_db_time_zone(&self, date: String) -> Result<String, CubeError>;
}
