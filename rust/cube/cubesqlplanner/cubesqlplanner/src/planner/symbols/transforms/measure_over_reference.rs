use super::super::{MeasureSymbol, MemberSymbol};
use crate::planner::SqlCall;
use std::rc::Rc;

/// The measure aggregating `reference` instead of its declared value. `case`
/// and measure filters are dropped: the source below already applied them.
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
