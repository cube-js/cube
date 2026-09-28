use super::super::LogicalSchema;
use crate::planner::symbols::transforms;
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;
use std::collections::HashSet;
use std::rc::Rc;

/// Copy of the schema with every time dimension marked as
/// timezone-converted at the source (a pre-aggregation rollup or an
/// input CTE). Every schema member is rewritten, so occurrences of
/// the marked time dimensions embedded in other members' expression
/// trees carry the mark too.
pub fn mark_tz_converted_at_source_in_schema(
    schema: &LogicalSchema,
) -> Result<Rc<LogicalSchema>, CubeError> {
    let names = schema
        .time_dimensions
        .iter()
        .map(|d| d.full_name())
        .collect::<HashSet<_>>();
    let mark = |members: &Vec<Rc<MemberSymbol>>| {
        members
            .iter()
            .map(|m| transforms::mark_tz_converted_at_source(m, &names))
            .collect::<Result<Vec<_>, _>>()
    };
    let mut new = schema.clone();
    new.time_dimensions = mark(&new.time_dimensions)?;
    new.dimensions = mark(&new.dimensions)?;
    new.measures = mark(&new.measures)?;
    Ok(Rc::new(new))
}
