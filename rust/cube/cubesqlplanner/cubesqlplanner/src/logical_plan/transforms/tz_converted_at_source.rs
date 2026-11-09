use super::super::LogicalSchema;
use crate::planner::symbols::transforms;
use cubenativeutils::CubeError;
use std::collections::HashSet;
use std::rc::Rc;

/// Copy of the schema with every time dimension marked as
/// timezone-converted at the source (a pre-aggregation rollup or an
/// input subquery). Every schema member is rewritten, so occurrences of
/// the marked time dimensions embedded in other members' expression
/// trees carry the mark too.
pub fn mark_tz_converted_at_source_in_schema(
    schema: &LogicalSchema,
) -> Result<Rc<LogicalSchema>, CubeError> {
    let ids = schema
        .time_dimensions
        .iter()
        .map(|d| d.id().clone())
        .collect::<HashSet<_>>();
    let schema = schema.map_symbols(&|m| transforms::mark_tz_converted_at_source(m, &ids))?;
    Ok(Rc::new(schema))
}
