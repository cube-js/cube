use crate::cube_bridge::member_sql::FilterParamsColumn;
use std::collections::HashMap;

#[derive(Default)]
pub struct FiltersContext {
    pub use_local_tz: bool,
    pub filter_params_columns: HashMap<String, FilterParamsColumn>,
}
