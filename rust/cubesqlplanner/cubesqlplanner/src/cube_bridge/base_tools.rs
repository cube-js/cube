use super::base_query_options::FilterItem;
use super::filter_group::{FilterGroup, NativeFilterGroup};
use super::filter_params::{FilterParams, NativeFilterParams};
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
    fn convert_tz(&self, field: String) -> Result<String, CubeError>;
    fn time_grouped_column(
        &self,
        granularity: String,
        dimension: String,
    ) -> Result<String, CubeError>;
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
    fn timestamp_precision(&self) -> Result<u32, CubeError>;
    fn in_db_time_zone(&self, date: String) -> Result<String, CubeError>;
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
    //===== TODO Move to templates
    fn hll_init(&self, sql: String) -> Result<String, CubeError>;
    fn hll_merge(&self, sql: String) -> Result<String, CubeError>;
    fn hll_cardinality_merge(&self, sql: String) -> Result<String, CubeError>;
    fn count_distinct_approx(&self, sql: String) -> Result<String, CubeError>;
    fn date_bin(
        &self,
        interval: String,
        source: String,
        origin: String,
    ) -> Result<String, CubeError>;
}
