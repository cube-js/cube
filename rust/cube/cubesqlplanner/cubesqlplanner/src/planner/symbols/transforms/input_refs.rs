use super::super::{MemberSymbol, RefTarget};
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Replaces every reference to a plan input with the member it reads, for a
/// node that no longer reads its inputs.
pub fn inline_input_refs(symbol: &Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError> {
    symbol.apply_recursive(&|node: &Rc<MemberSymbol>| match node.input_origin() {
        Some(origin) => Ok(origin.clone()),
        None => Ok(node.clone()),
    })
}

/// `node` with `f` applied to the member it reads, when it is a reference to
/// plan inputs.
pub fn map_input_origin(
    node: &Rc<MemberSymbol>,
    f: &impl Fn(&Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError>,
) -> Result<Option<Rc<MemberSymbol>>, CubeError> {
    let MemberSymbol::Ref(reference) = node.as_ref() else {
        return Ok(None);
    };
    Ok(match reference.target() {
        RefTarget::Input { input, origin } => {
            Some(MemberSymbol::new_input_ref(&f(origin)?, *input))
        }
        RefTarget::Key { inputs, origin } => {
            Some(MemberSymbol::new_key_ref(&f(origin)?, inputs.clone()))
        }
        _ => None,
    })
}
