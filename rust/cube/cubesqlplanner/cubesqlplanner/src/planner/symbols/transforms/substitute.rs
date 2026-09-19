use super::super::deps;
use super::super::MemberSymbol;
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

/// The per-node replacement: a member named in `replacements` becomes
/// the symbol mapped to it.
///
/// A `ColumnRef` node is left alone. It already names what it reads, and
/// it carries the name of the member it stands for — so a substitute
/// that reads its member from a reference stays put instead of being
/// rebuilt around that reference over and over.
fn replacement<'a>(
    replacements: &'a HashMap<String, Rc<MemberSymbol>>,
) -> impl Fn(&Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError> + 'a {
    move |node: &Rc<MemberSymbol>| {
        if matches!(node.as_ref(), MemberSymbol::ColumnRef(_)) {
            return Ok(node.clone());
        }
        Ok(replacements
            .get(&node.full_name())
            .cloned()
            .unwrap_or_else(|| node.clone()))
    }
}

/// Rebuilds the symbol tree with every node whose `full_name` appears
/// in `replacements` substituted by the mapped symbol; the walk then
/// descends into the substitute's dependencies, not the original's.
pub fn substitute_by_name(
    symbol: &Rc<MemberSymbol>,
    replacements: &HashMap<String, Rc<MemberSymbol>>,
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
    replacements: &HashMap<String, Rc<MemberSymbol>>,
) -> Result<Rc<MemberSymbol>, CubeError> {
    if matches!(symbol.as_ref(), MemberSymbol::TimeDimension(_)) {
        return deps::apply_to_deps(symbol, &replacement(replacements));
    }
    substitute_by_name(symbol, replacements)
}
