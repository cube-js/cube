use super::super::MemberSymbol;
use crate::planner::MemberId;
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

/// Rebuilds the symbol tree with every node whose id appears
/// in `replacements` substituted by the mapped symbol; the walk then
/// descends into the substitute's dependencies, not the original's.
pub fn substitute_by_name(
    symbol: &Rc<MemberSymbol>,
    replacements: &HashMap<MemberId, Rc<MemberSymbol>>,
) -> Result<Rc<MemberSymbol>, CubeError> {
    symbol.apply_recursive(&|node| {
        Ok(replacements
            .get(node.id())
            .cloned()
            .unwrap_or_else(|| node.clone()))
    })
}
