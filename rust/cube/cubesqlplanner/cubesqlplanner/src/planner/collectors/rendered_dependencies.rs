use crate::planner::symbols::deps::{collect_deps, SymbolDeps};
use crate::planner::MemberSymbol;
use std::rc::Rc;

/// The slots of `member` that reach its rendered SQL: `drill_filters` never do,
/// and a `mask` only for the members `is_masked` accepts.
pub fn visit_rendered_slots(
    member: &Rc<MemberSymbol>,
    is_masked: &dyn Fn(&Rc<MemberSymbol>) -> bool,
    visit: &mut dyn FnMut(&dyn SymbolDeps),
) {
    // Not `iter_sql_calls`: on the measure side it misses `measure_filters` and
    // `measure_order_by`.
    // A masked member's own slots stay required: the same model renders them
    // for every member the mask is not applied to.
    if is_masked(member) {
        if let Some(mask) = member.mask_sql() {
            visit(mask);
        }
    }
    if let Ok(measure) = member.as_measure() {
        visit(measure.kind());
        for filter in measure.measure_filters() {
            visit(filter);
        }
        for order_by in measure.measure_order_by() {
            visit(order_by);
        }
        if let Some(case) = measure.case() {
            visit(case);
        }
    } else if let Ok(dimension) = member.as_dimension() {
        visit(dimension.kind());
    } else if let Ok(time_dimension) = member.as_time_dimension() {
        // A time dimension is a view of its base: the granularity renders around
        // whatever the base renders, so both contribute.
        visit(time_dimension.granularity_obj());
        visit_rendered_slots(time_dimension.base_symbol(), is_masked, visit);
    } else if let Ok(ref_symbol) = member.as_ref_symbol() {
        visit(ref_symbol.target());
    } else {
        visit(member.as_ref());
    }
}

/// The members the rendered SQL of `member` reads.
pub fn rendered_dependencies(
    member: &Rc<MemberSymbol>,
    is_masked: &dyn Fn(&Rc<MemberSymbol>) -> bool,
) -> Vec<Rc<MemberSymbol>> {
    let mut result = vec![];
    visit_rendered_slots(member, is_masked, &mut |slot| {
        result.extend(collect_deps(slot))
    });
    result
}
