use super::sql_templates_render::{NativeSqlTemplatesRender, SqlTemplatesRender};
use cubenativeutils::wrappers::serializer::{
    NativeDeserialize, NativeDeserializer, NativeSerialize,
};
use cubenativeutils::wrappers::NativeContextHolder;
use cubenativeutils::wrappers::NativeObjectHandle;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

#[nativebridge::native_bridge]
pub trait DriverTools {
    fn convert_tz(&self, field: String) -> Result<String, CubeError>;
    fn time_grouped_column(
        &self,
        granularity: String,
        dimension: String,
    ) -> Result<String, CubeError>;
    fn sql_templates(&self) -> Result<Rc<dyn SqlTemplatesRender>, CubeError>;
    fn timestamp_precision(&self) -> Result<u32, CubeError>;
    fn time_stamp_cast(&self, field: String) -> Result<String, CubeError>; //TODO move to templates
    fn date_time_cast(&self, field: String) -> Result<String, CubeError>; //TODO move to templates
    fn in_db_time_zone(&self, date: String) -> Result<String, CubeError>;
    fn get_allocated_params(&self) -> Result<Vec<String>, CubeError>;
    fn subtract_interval(&self, date: String, interval: String) -> Result<String, CubeError>;
    fn add_interval(&self, date: String, interval: String) -> Result<String, CubeError>;
    fn interval_string(&self, interval: String) -> Result<String, CubeError>;
    fn add_timestamp_interval(&self, date: String, interval: String) -> Result<String, CubeError>;
    fn interval_and_minimal_time_unit(&self, interval: String) -> Result<Vec<String>, CubeError>;
    fn hll_init(&self, sql: String) -> Result<String, CubeError>;
    fn hll_merge(&self, sql: String) -> Result<String, CubeError>;
    fn hll_cardinality_merge(&self, sql: String) -> Result<String, CubeError>;
    fn count_distinct_approx(&self, sql: String) -> Result<String, CubeError>;
    fn support_generated_series_for_custom_td(&self) -> Result<bool, CubeError>;
    fn date_bin(
        &self,
        interval: String,
        source: String,
        origin: String,
    ) -> Result<String, CubeError>;
}
