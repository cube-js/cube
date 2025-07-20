use super::base_query_options::FilterItem;
use super::driver_tools::{DriverTools, NativeDriverTools};
use super::filter_group::{FilterGroup, NativeFilterGroup};
use super::filter_params::{FilterParams, NativeFilterParams};
use super::pre_aggregation_obj::{NativePreAggregationObj, PreAggregationObj};
use super::security_context::{NativeSecurityContext, SecurityContext};
use super::sql_templates_render::{NativeSqlTemplatesRender, SqlTemplatesRender};
use super::sql_utils::{NativeSqlUtils, SqlUtils};
use cubenativeutils::wrappers::serializer::{
    NativeDeserialize, NativeDeserializer, NativeSerialize,
};
use cubenativeutils::wrappers::NativeContextHolder;
use cubenativeutils::wrappers::NativeObjectHandle;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

#[nativebridge::native_bridge]
pub trait BaseTools {
    fn driver_tools(&self, external: bool) -> Result<Rc<dyn DriverTools>, CubeError>;
    fn sql_templates(&self) -> Result<Rc<dyn SqlTemplatesRender>, CubeError>;
    fn security_context_for_rust(&self) -> Result<Rc<dyn SecurityContext>, CubeError>;
    fn sql_utils_for_rust(&self) -> Result<Rc<dyn SqlUtils>, CubeError>;
    fn filters_proxy_for_rust(
        &self,
        used_filters: Option<Vec<FilterItem>>,
    ) -> Result<Rc<dyn FilterParams>, CubeError>;
    fn filter_group_function_for_rust(
        &self,
        used_filters: Option<Vec<FilterItem>>,
    ) -> Result<Rc<dyn FilterGroup>, CubeError>;
    fn generate_time_series(
        &self,
        granularity: String,
        date_range: Vec<String>,
    ) -> Result<Vec<Vec<String>>, CubeError>;
    fn generate_custom_time_series(
        &self,
        granularity: String,
        date_range: Vec<String>,
        origin: String,
    ) -> Result<Vec<Vec<String>>, CubeError>;
    fn get_allocated_params(&self) -> Result<Vec<String>, CubeError>;
    fn all_cube_members(&self, path: String) -> Result<Vec<String>, CubeError>;
    fn interval_and_minimal_time_unit(&self, interval: String) -> Result<Vec<String>, CubeError>;
    fn get_pre_aggregation_by_name(
        &self,
        cube_name: String,
        name: String,
    ) -> Result<Rc<dyn PreAggregationObj>, CubeError>;
    fn pre_aggregation_table_name(
        &self,
        cube_name: String,
        name: String,
    ) -> Result<String, CubeError>; //TODO move to rust
}
