use crate::{plan::FilterItem, planner::sql_evaluator::MemberSymbol};
use std::rc::Rc;

pub fn find_single_value_restriction(
    filters: &Vec<FilterItem>,
    symbol: &Rc<MemberSymbol>,
) -> Option<String> {
    let mut candidate: Option<String> = None;

    for child in filters {
        if let Some(v) = child.find_single_value_restriction(symbol) {
            if let Some(prev) = &candidate {
                if prev != &v {
                    return None;
                }
            }
            candidate = Some(v);
        }
    }

    candidate
}

pub fn apply_static_filter(
    symbol: &Rc<MemberSymbol>,
    filters: &Vec<FilterItem>,
) -> Rc<MemberSymbol> {
    match symbol.as_ref() {
        MemberSymbol::Dimension(dim) => {
            if dim.dimension_type() == "switch" {
                if let Some(value) = find_single_value_restriction(filters, symbol) {
                    if dim.values().iter().any(|v| v == &value) {
                        return MemberSymbol::new_dimension(dim.replace_values(vec![value]));
                    }
                }
            }

            if let Some(case) = dim.case() {
                if let Some(case_replacement) = case.apply_static_filter(filters) {
                    return MemberSymbol::new_dimension(
                        dim.replace_case_with_sql_call(case_replacement),
                    );
                }
            }
        }
        MemberSymbol::Measure(meas) => {
            if let Some(case) = meas.case() {
                if let Some(case_replacement) = case.apply_static_filter(filters) {
                    return MemberSymbol::new_measure(
                        meas.replace_case_with_sql_call(case_replacement),
                    );
                }
            }
        }
        _ => {}
    }
    symbol.clone()
}

pub fn apply_static_filter_to_vec(symbols: &mut Vec<Rc<MemberSymbol>>, filters: &Vec<FilterItem>) {
    symbols
        .iter_mut()
        .for_each(|s| *s = apply_static_filter(&s, &filters));
}
