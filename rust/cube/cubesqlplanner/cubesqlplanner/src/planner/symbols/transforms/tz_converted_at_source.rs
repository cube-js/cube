use super::super::MemberSymbol;
use cubenativeutils::CubeError;
use std::collections::HashSet;
use std::rc::Rc;

/// Marks the time dimensions listed in `names` as timezone-converted
/// at the source, everywhere in the symbol tree: their value comes
/// converted from a pre-aggregation rollup or an input CTE, so
/// rendering must not apply the timezone conversion again.
pub fn mark_tz_converted_at_source(
    symbol: &Rc<MemberSymbol>,
    names: &HashSet<String>,
) -> Result<Rc<MemberSymbol>, CubeError> {
    symbol.apply_recursive(&|node| {
        if let MemberSymbol::TimeDimension(td) = node.as_ref() {
            if !td.tz_converted_at_source() && names.contains(&node.full_name()) {
                let mut new = (**td).clone();
                new.tz_converted_at_source = true;
                return Ok(Rc::new(MemberSymbol::TimeDimension(Rc::new(new))));
            }
        }
        Ok(node.clone())
    })
}
