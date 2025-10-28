use cubenativeutils::CubeError;

use crate::{
    plan::{filter::FilterGroupOperator, Filter, FilterGroup, FilterItem},
    planner::sql_evaluator::MemberSymbol,
};
use std::rc::Rc;

pub fn find_value_restriction(
    filters: &Vec<FilterItem>,
    symbol: &Rc<MemberSymbol>,
) -> Option<Vec<String>> {
    let filter = FilterItem::Group(Rc::new(FilterGroup {
        operator: FilterGroupOperator::And,
        items: filters.clone(),
    }));
    filter.find_value_restriction(symbol)
}

pub fn get_filtered_values(symbol: &Rc<MemberSymbol>, filter: &Option<Filter>) -> Vec<String> {
    if let Ok(dim) = symbol.as_dimension() {
        if dim.dimension_type() == "switch" {
            if let Some(filter) = filter {
                if let Some(values) = find_value_restriction(&filter.items, symbol) {
                    let res = dim
                        .values()
                        .iter()
                        .filter(|v| values.contains(v))
                        .cloned()
                        .collect();
                    return res;
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
                    if let Some(new_case) = case.apply_static_filter(filters) {
                        return Ok(MemberSymbol::new_dimension(dim.replace_case(new_case)));
                    }
                }
            }
            MemberSymbol::Measure(meas) => {
                if let Some(case) = meas.case() {
                    if let Some(new_case) = case.apply_static_filter(filters) {
                        return Ok(MemberSymbol::new_measure(meas.replace_case(new_case)));
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
                &item.raw_member_evaluator(),
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
