use super::super::LogicalSchema;
use crate::planner::filter::{map_filter, Filter};
use crate::planner::symbols::transforms;
use crate::planner::{MemberId, MemberSymbol};
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

/// Copy of the schema with every member named in `substitutions` replaced
/// by its mapped symbol, both at top level and inside other members' trees.
pub fn substitute_symbols_in_schema(
    schema: &LogicalSchema,
    substitutions: &HashMap<MemberId, Rc<MemberSymbol>>,
) -> Result<Rc<LogicalSchema>, CubeError> {
    if substitutions.is_empty() {
        return Ok(Rc::new(schema.clone()));
    }
    let schema = schema.map_symbols(&|m| transforms::substitute_by_name(m, substitutions))?;
    Ok(Rc::new(schema))
}

/// Filter counterpart of `substitute_symbols_in_schema`.
pub fn substitute_symbols_in_filter(
    filter: Option<Filter>,
    substitutions: &HashMap<MemberId, Rc<MemberSymbol>>,
) -> Result<Option<Filter>, CubeError> {
    if substitutions.is_empty() {
        return Ok(filter);
    }
    map_filter(filter, &|symbol| {
        transforms::substitute_filter_member_by_name(symbol, substitutions)
    })
}
