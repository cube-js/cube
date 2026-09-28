use super::super::MemberSymbol;
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

/// Rebuilds the symbol tree with every node whose `full_name` appears
/// in `replacements` substituted by the mapped symbol; the walk then
/// descends into the substitute's dependencies, not the original's.
pub fn substitute_by_name(
    symbol: &Rc<MemberSymbol>,
    replacements: &HashMap<String, Rc<MemberSymbol>>,
) -> Result<Rc<MemberSymbol>, CubeError> {
    symbol.apply_recursive(&|node| {
        Ok(replacements
            .get(&node.full_name())
            .cloned()
            .unwrap_or_else(|| node.clone()))
    })
}
