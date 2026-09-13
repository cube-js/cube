use super::super::LogicalSchema;
use crate::planner::filter::Filter;
use crate::planner::symbols::transforms;
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

/// Copy of the schema with every member named in `substitutions`
/// replaced by the symbol mapped to it. Every schema member is
/// rewritten, so a substituted member is replaced both where it is
/// exposed on its own and where it appears inside another member's
/// expression tree.
pub fn substitute_symbols_in_schema(
    schema: &LogicalSchema,
    substitutions: &HashMap<String, Rc<MemberSymbol>>,
) -> Result<Rc<LogicalSchema>, CubeError> {
    let substitute = |members: &Vec<Rc<MemberSymbol>>| {
        members
            .iter()
            .map(|m| transforms::substitute_by_name(m, substitutions))
            .collect::<Result<Vec<_>, _>>()
    };
    let mut new = schema.clone();
    new.time_dimensions = substitute(&new.time_dimensions)?;
    new.dimensions = substitute(&new.dimensions)?;
    new.measures = substitute(&new.measures)?;
    Ok(Rc::new(new))
}

/// Copy of the filter with every member named in `substitutions`
/// replaced by the symbol mapped to it.
pub fn substitute_symbols_in_filter(
    filter: Option<Filter>,
    substitutions: &HashMap<String, Rc<MemberSymbol>>,
) -> Result<Option<Filter>, CubeError> {
    transforms::map_filter_symbols(filter, &|symbol| {
        transforms::substitute_filter_member_by_name(symbol, substitutions)
    })
}
