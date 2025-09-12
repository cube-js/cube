use cubenativeutils::CubeError;

use crate::{
    plan::{Filter, FilterItem},
    planner::sql_evaluator::MemberSymbol,
};
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

pub fn get_filtered_values(symbol: &Rc<MemberSymbol>, filter: &Option<Filter>) -> Vec<String> {
    if let Ok(dim) = symbol.as_dimension() {
        if dim.dimension_type() == "switch" {
            if let Some(filter) = filter {
                if let Some(value) = find_single_value_restriction(&filter.items, symbol) {
                    if dim.values().iter().any(|v| v == &value) {
                        return vec![value];
                    }
                }
            }
        }
        return dim.values().clone();
    }

    vec![]
}

pub fn apply_static_filter_to_symbol(
    symbol: &Rc<MemberSymbol>,
    filters: &Vec<FilterItem>,
) -> Result<Rc<MemberSymbol>, CubeError> {
    symbol.apply_recursive(&|symbol: &Rc<MemberSymbol>| {
        match symbol.as_ref() {
            MemberSymbol::Dimension(dim) => {
                if let Some(case) = dim.case() {
                    if let Some(case_replacement) = case.apply_static_filter(filters) {
                        return Ok(MemberSymbol::new_dimension(
                            dim.replace_case_with_sql_call(case_replacement),
                        ));
                    }
                }
            }
            MemberSymbol::Measure(meas) => {
                if let Some(case) = meas.case() {
                    if let Some(case_replacement) = case.apply_static_filter(filters) {
                        return Ok(MemberSymbol::new_measure(
                            meas.replace_case_with_sql_call(case_replacement),
                        ));
                    }
                }
            }
            _ => {}
        }
        Ok(symbol.clone())
    })
}

pub fn apply_static_filter_to_filter_item(
    filter_item: &FilterItem,
    filters: &Vec<FilterItem>,
) -> Result<FilterItem, CubeError> {
    let mut result = filter_item.clone();
    match &mut result {
        FilterItem::Group(group) => {
            let mut new_group = group.as_ref().clone();
            for item in new_group.items.iter_mut() {
                *item = apply_static_filter_to_filter_item(item, filters)?;
            }
            *group = Rc::new(new_group);
        }
        FilterItem::Item(item) => {
            *item = item.with_member_evaluator(apply_static_filter_to_symbol(
                &item.member_evaluator(),
                filters,
            )?);
        }
        FilterItem::Segment(item) => {
            *item = item.with_member_evaluator(apply_static_filter_to_symbol(
                &item.member_evaluator(),
                filters,
            )?);
        }
    }
    Ok(result)
}
