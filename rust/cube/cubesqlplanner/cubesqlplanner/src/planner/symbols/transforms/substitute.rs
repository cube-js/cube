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
    symbol.apply_recursive(&replacement(replacements))
}

/// Substitution for the member a filter names.
///
/// A filter renders the dimension behind a time-dimension wrapper
/// rather than the wrapper itself (see `BaseFilter::member_evaluator`),
/// so a granularity wrapper is never the symbol read from a source —
/// the substitution applies inside it. Anything else is substituted as
/// usual.
pub fn substitute_filter_member_by_name(
    symbol: &Rc<MemberSymbol>,
    replacements: &HashMap<MemberId, Rc<MemberSymbol>>,
) -> Result<Rc<MemberSymbol>, CubeError> {
    if matches!(symbol.as_ref(), MemberSymbol::TimeDimension(_)) {
        return deps::apply_to_deps(symbol, &replacement(replacements));
    }
    substitute_by_name(symbol, replacements)
}

/// Rebuilds the call with every member named in `replacements` among its
/// dependencies substituted, the same way `substitute_by_name` does.
pub fn substitute_in_sql_call(
    call: &Rc<SqlCall>,
    replacements: &HashMap<MemberId, Rc<MemberSymbol>>,
) -> Result<Rc<SqlCall>, CubeError> {
    deps::apply_to_node_deps(call, &replacement(replacements))
}
