use super::super::{MeasureRenderModifier, MemberSymbol};
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Rebuilds the symbol tree with `modifier` set on every measure that
/// has no render modifier yet. Measures already carrying one keep it:
/// a member-level modifier is set before the query-level pass and
/// takes precedence.
pub fn measures_render_modifier(
    symbol: &Rc<MemberSymbol>,
    modifier: &MeasureRenderModifier,
) -> Result<Rc<MemberSymbol>, CubeError> {
    symbol.apply_recursive(&|node| {
        if let MemberSymbol::Measure(measure) = node.as_ref() {
            if measure.render_modifier().is_none() {
                let mut new = (**measure).clone();
                new.render_modifier = Some(modifier.clone());
                return Ok(Rc::new(MemberSymbol::Measure(Rc::new(new))));
            }
        }
        Ok(node.clone())
    })
}
