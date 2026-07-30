use super::super::measure_kinds::{CalculatedMeasure, CalculatedMeasureType, MeasureKind};
use super::super::MeasureSymbol;
use std::rc::Rc;

/// Returns a non-rolling copy of the measure. A rolling-window
/// measure carries both the windowing context and the SQL of the
/// inner value it operates on; unrolling drops the window and
/// yields that inner value. Multi-stage rolling measures collapse
/// to a `Calculated` kind so they can be rendered without window-
/// function machinery.
pub fn unroll_rolling(measure: &MeasureSymbol) -> Rc<MeasureSymbol> {
    if !measure.is_rolling_window() {
        return Rc::new(measure.clone());
    }
    let kind = if measure.is_multi_stage() {
        if let Some(sql) = measure.kind.member_sql() {
            MeasureKind::Calculated(CalculatedMeasure::new(
                CalculatedMeasureType::Number,
                sql.clone(),
            ))
        } else {
            MeasureKind::Calculated(CalculatedMeasure::new_without_sql(
                CalculatedMeasureType::Number,
            ))
        }
    } else {
        measure.kind.clone()
    };
    Rc::new(MeasureSymbol {
        compiled_path: measure.compiled_path.clone(),
        kind,
        rolling_window: None,
        multi_stage: None,
        is_reference: false,
        is_view: measure.is_view,
        case: measure.case.clone(),
        measure_filters: measure.measure_filters.clone(),
        measure_drill_filters: measure.measure_drill_filters.clone(),
        measure_order_by: measure.measure_order_by.clone(),
        mask_sql: measure.mask_sql.clone(),
        render_modifier: measure.render_modifier.clone(),
    })
}
