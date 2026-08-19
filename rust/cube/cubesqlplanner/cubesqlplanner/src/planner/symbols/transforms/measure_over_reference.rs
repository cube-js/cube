use super::super::{MeasureSymbol, MemberSymbol};
use crate::planner::SqlCall;
use std::rc::Rc;

/// Returns a copy of the measure that aggregates `reference` instead of
/// the value it declares — the form a measure takes in a select that
/// reads its row-level input from a source below.
///
/// The measure keeps everything that describes how the aggregated
/// result is emitted: its aggregation, its mask, the `order_by` a rank
/// window uses, its multi-stage and rolling context, and any render
/// modifier stamped on it.
///
/// It keeps nothing that describes how the input is computed: the
/// `case` body and the measure-level filters produced the rows that the
/// source below already aggregated, and re-applying them here would
/// both double the condition and read members this select has no access
/// to.
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
