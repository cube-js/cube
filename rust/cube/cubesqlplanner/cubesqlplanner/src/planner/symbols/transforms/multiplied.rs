use super::super::measure_kinds::MeasureKind;
use super::super::{MeasureSymbol, MemberSymbol};
use std::rc::Rc;

/// Render form of a measure when it sits under a row-multiplying
/// join: a `count` switches to a distinct `MultipliedCount`, every
/// other kind is returned unchanged.
pub fn into_multiplied(measure: &MeasureSymbol) -> Rc<MemberSymbol> {
    with_kind(measure, measure.kind.into_multiplied())
}

/// `Some(render form)` when the measure, under a row-multiplying
/// join, can still be computed directly in the main query (it stays
/// additive there): a key-based count rolls up as a distinct
/// `MultipliedCount`, distinct aggregations are already immune.
/// `None` when it must be isolated in a multiplied subquery instead.
pub fn regular_in_multiplied(measure: &MeasureSymbol) -> Option<Rc<MemberSymbol>> {
    measure
        .kind
        .regular_in_multiplied()
        .map(|kind| with_kind(measure, kind))
}

fn with_kind(measure: &MeasureSymbol, kind: MeasureKind) -> Rc<MemberSymbol> {
    let mut new = measure.clone();
    new.kind = kind;
    MemberSymbol::new_measure(Rc::new(new))
}
