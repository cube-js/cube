use super::super::{MeasureRenderModifier, MemberSymbol};
use super::map_input_origin;
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Sets `modifier` on every measure of the tree it applies to; stamping twice is a no-op.
/// A reference to plan inputs carries the modifier on its origin, so inlining it keeps the form.
pub fn measures_render_modifier(
    symbol: &Rc<MemberSymbol>,
    modifier: &MeasureRenderModifier,
) -> Result<Rc<MemberSymbol>, CubeError> {
    symbol.apply_recursive(&|node| {
        if let Some(reference) =
            map_input_origin(node, &|origin| measures_render_modifier(origin, modifier))?
        {
            return Ok(reference);
        }
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
