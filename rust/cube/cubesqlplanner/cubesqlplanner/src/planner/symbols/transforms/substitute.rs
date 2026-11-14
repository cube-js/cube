use super::super::deps;
use super::super::MemberSymbol;
use crate::planner::{MemberId, SqlCall};
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

// A terminal reference is left alone: it carries the id of the member it
// stands for, so a substitute that reads its member through a reference
// would otherwise be rebuilt around it over and over.
fn replacement<'a>(
    replacements: &'a HashMap<MemberId, Rc<MemberSymbol>>,
) -> impl Fn(&Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError> + 'a {
    move |node: &Rc<MemberSymbol>| {
        if node.is_terminal_ref() {
            return Ok(node.clone());
        }
        Ok(replacements
            .get(node.id())
            .cloned()
            .unwrap_or_else(|| node.clone()))
    }
}

/// Rebuilds the symbol tree with every node whose id appears
/// in `replacements` substituted by the mapped symbol; the walk then
/// descends into the substitute's dependencies, not the original's.
pub fn substitute_by_name(
    symbol: &Rc<MemberSymbol>,
    replacements: &HashMap<MemberId, Rc<MemberSymbol>>,
) -> Result<Rc<MemberSymbol>, CubeError> {
    if replacements.is_empty() {
        return Ok(symbol.clone());
    }
    symbol.apply_recursive(&replacement(replacements))
}

/// Like `substitute_by_name`, but a time-dimension wrapper is never replaced
/// itself — a filter renders the dimension behind it (see
/// `BaseFilter::member_evaluator`), so only its dependencies are substituted.
pub fn substitute_filter_member_by_name(
    symbol: &Rc<MemberSymbol>,
    replacements: &HashMap<MemberId, Rc<MemberSymbol>>,
) -> Result<Rc<MemberSymbol>, CubeError> {
    if replacements.is_empty() {
        return Ok(symbol.clone());
    }
    if matches!(symbol.as_ref(), MemberSymbol::TimeDimension(_)) {
        return deps::apply_to_deps(symbol, &replacement(replacements));
    }
    substitute_by_name(symbol, replacements)
}

/// `substitute_by_name` over the dependencies of a call.
pub fn substitute_in_sql_call(
    call: &Rc<SqlCall>,
    replacements: &HashMap<MemberId, Rc<MemberSymbol>>,
) -> Result<Rc<SqlCall>, CubeError> {
    if replacements.is_empty() {
        return Ok(call.clone());
    }
    deps::apply_to_node_deps(call, &replacement(replacements))
}
