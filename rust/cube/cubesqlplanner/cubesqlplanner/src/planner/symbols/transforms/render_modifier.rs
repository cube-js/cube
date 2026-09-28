use super::super::{MeasureRenderModifier, MemberSymbol};
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Rebuilds the symbol tree with `modifier` set on every measure the
/// form applies to that has no render modifier yet. A measure already
/// carrying a form keeps it, so stamping a tree twice is a no-op
/// rather than a silent overwrite — each select decides the form of
/// the measures it renders exactly once.
pub fn measures_render_modifier(
    symbol: &Rc<MemberSymbol>,
    modifier: &MeasureRenderModifier,
) -> Result<Rc<MemberSymbol>, CubeError> {
    symbol.apply_recursive(&|node| {
        if let MemberSymbol::Measure(measure) = node.as_ref() {
            if measure.render_modifier().is_none() && modifier.applies_to(measure) {
                let mut new = (**measure).clone();
                new.render_modifier = Some(modifier.clone());
                return Ok(Rc::new(MemberSymbol::Measure(Rc::new(new))));
            }
        }
        Ok(node.clone())
    })
}
