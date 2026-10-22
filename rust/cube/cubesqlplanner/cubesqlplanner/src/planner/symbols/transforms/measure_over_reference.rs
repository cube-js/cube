use super::super::{MeasureSymbol, MemberSymbol};
use crate::planner::SqlCall;
use std::rc::Rc;

/// Returns a copy of the measure that aggregates `reference` instead of the
/// value it declares.
///
/// The `case` body and the measure filters are dropped: they shaped the rows
/// the source below already aggregated, so re-applying them here would double
/// the condition and read members this select cannot reach.
pub fn measure_over_reference(
    measure: &MeasureSymbol,
    reference: Rc<MemberSymbol>,
) -> Rc<MeasureSymbol> {
    let kind = measure
        .kind()
        .over_input_sql(SqlCall::new_direct_reference(reference));
    Rc::new(MeasureSymbol {
        compiled_path: measure.compiled_path.clone(),
        kind,
        rolling_window: measure.rolling_window.clone(),
        multi_stage: measure.multi_stage.clone(),
        is_reference: measure.is_reference,
        is_view: measure.is_view,
        case: None,
        measure_filters: vec![],
        measure_drill_filters: vec![],
        measure_order_by: measure.measure_order_by.clone(),
        mask_sql: measure.mask_sql.clone(),
        render_modifier: measure.render_modifier.clone(),
    })
}
