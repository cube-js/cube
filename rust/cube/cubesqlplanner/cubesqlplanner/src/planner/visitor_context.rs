use crate::cube_bridge::member_sql::FilterParamsColumn;
use std::collections::HashMap;

#[derive(Default)]
pub struct FiltersContext {
    pub use_local_tz: bool,
    pub filter_params_columns: HashMap<String, FilterParamsColumn>,
    /// True when members resolve to pre-aggregation columns (a rollup read). A
    /// segment is then a stored boolean column, which some dialects can't use
    /// as a bare predicate (e.g. MSSQL `BIT` needs `= 1`).
    pub reading_pre_aggregation: bool,
}
