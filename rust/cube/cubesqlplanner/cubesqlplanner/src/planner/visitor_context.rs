use crate::planner::sql_call::SqlCallFilterParamsItem;
use std::collections::HashMap;

#[derive(Default)]
pub struct FiltersContext {
    pub use_local_tz: bool,
    /// Per member, every `FILTER_PARAMS` binding the rendered group declares
    /// for it. A member carries more than one when the model addresses its time
    /// shifts separately; which of them renders depends on the stage.
    pub filter_params_columns: HashMap<String, Vec<SqlCallFilterParamsItem>>,
    /// True when members resolve to pre-aggregation columns (a rollup read). A
    /// segment is then a stored boolean column, which some dialects can't use
    /// as a bare predicate (e.g. MSSQL `BIT` needs `= 1`).
    pub reading_pre_aggregation: bool,
}
