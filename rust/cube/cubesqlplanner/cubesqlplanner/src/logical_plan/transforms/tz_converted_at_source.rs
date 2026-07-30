use super::super::LogicalSchema;
use crate::planner::symbols::transforms;
use cubenativeutils::CubeError;
use std::collections::HashSet;
use std::rc::Rc;

/// Copy of the schema with every time dimension marked as
/// timezone-converted at the source (a pre-aggregation rollup or an
/// input CTE). The mark is applied recursively inside each time
/// dimension's tree.
pub fn mark_tz_converted_at_source_in_schema(
    schema: &LogicalSchema,
) -> Result<Rc<LogicalSchema>, CubeError> {
    let names = schema
        .time_dimensions
        .iter()
        .map(|d| d.full_name())
        .collect::<HashSet<_>>();
    let mut new = schema.clone();
    new.time_dimensions = new
        .time_dimensions
        .iter()
        .map(|d| transforms::mark_tz_converted_at_source(d, &names))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Rc::new(new))
}
