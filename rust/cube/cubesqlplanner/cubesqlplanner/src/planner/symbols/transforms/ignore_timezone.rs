use super::super::MemberSymbol;
use cubenativeutils::CubeError;
use std::collections::HashSet;
use std::rc::Rc;

/// Marks the time dimensions listed in `names` as already
/// timezone-converted, everywhere in the symbol tree: their value
/// comes from a source that stores it converted (a pre-aggregation
/// rollup or an input CTE), so rendering must not apply the timezone
/// conversion again.
pub fn ignore_timezone_for(
    symbol: &Rc<MemberSymbol>,
    names: &HashSet<String>,
) -> Result<Rc<MemberSymbol>, CubeError> {
    symbol.apply_recursive(&|node| {
        if let MemberSymbol::TimeDimension(td) = node.as_ref() {
            if !td.ignore_timezone() && names.contains(&node.full_name()) {
                let mut new = (**td).clone();
                new.ignore_timezone = true;
                return Ok(Rc::new(MemberSymbol::TimeDimension(Rc::new(new))));
            }
        }
        Ok(node.clone())
    })
}
