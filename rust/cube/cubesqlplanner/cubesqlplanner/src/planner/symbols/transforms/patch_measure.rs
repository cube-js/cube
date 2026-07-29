use super::super::MeasureSymbol;
use crate::planner::SqlCall;
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Returns a copy of the measure with the measure type optionally
/// replaced (subject to per-kind compatibility checks) and
/// additional measure filters merged in.
pub fn patch_measure(
    measure: &MeasureSymbol,
    new_measure_type: Option<String>,
    add_filters: Vec<Rc<SqlCall>>,
) -> Result<Rc<MeasureSymbol>, CubeError> {
    let result_kind = if let Some(new_measure_type) = new_measure_type {
        if !measure.kind.can_replace_type_with(&new_measure_type) {
            return Err(CubeError::user(format!(
                "Unsupported measure type replacement for {}: {} => {}",
                measure.compiled_path.name(),
                measure.kind.measure_type_str(),
                new_measure_type
            )));
        }
        measure.kind.with_new_type(&new_measure_type)?
    } else {
        measure.kind.clone()
    };

    let mut measure_filters = measure.measure_filters.clone();
    if !add_filters.is_empty() {
        if !result_kind.supports_additional_filters() {
            return Err(CubeError::user(format!(
                "Unsupported additional filters for measure {} type {}",
                measure.compiled_path.name(),
                result_kind.measure_type_str()
            )));
        }
        measure_filters.extend(add_filters);
    }
    Ok(Rc::new(MeasureSymbol {
        compiled_path: measure.compiled_path.clone(),
        kind: result_kind,
        rolling_window: measure.rolling_window.clone(),
        multi_stage: measure.multi_stage.clone(),
        is_reference: measure.is_reference,
        is_view: measure.is_view,
        case: measure.case.clone(),
        measure_filters,
        measure_drill_filters: measure.measure_drill_filters.clone(),
        measure_order_by: measure.measure_order_by.clone(),
        is_splitted_source: measure.is_splitted_source,
        mask_sql: measure.mask_sql.clone(),
    }))
}
