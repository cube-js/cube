use super::super::common::Case;
use super::super::dimension_kinds::DimensionKind;
use super::super::{DimensionSymbol, MeasureSymbol, MemberSymbol};
use crate::planner::filter::{Filter, FilterGroup, FilterGroupOperator, FilterItem};
use cubenativeutils::CubeError;
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
        if dim.is_switch() {
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
        return dim.values().to_vec();
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
                        return Ok(MemberSymbol::new_dimension(replace_dimension_case(
                            dim, new_case,
                        )));
                    }
                }
            }
            MemberSymbol::Measure(meas) => {
                if let Some(case) = meas.case() {
                    if let Some(new_case) = case.apply_static_filter(filters) {
                        return Ok(MemberSymbol::new_measure(replace_measure_case(
                            meas, new_case,
                        )));
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
            )?)?;
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

fn replace_measure_case(measure: &MeasureSymbol, new_case: Case) -> Rc<MeasureSymbol> {
    let mut new = measure.clone();
    new.case = Some(new_case);
    Rc::new(new)
}

fn replace_dimension_case(dimension: &DimensionSymbol, new_case: Case) -> Rc<DimensionSymbol> {
    let mut new = dimension.clone();
    if new_case.is_single_value() {
        //FIXME - Hack: we don't treat a single-element case as a multi-stage dimension
        new.multi_stage = None;
    }
    if let DimensionKind::Case(ref c) = new.kind {
        new.kind = DimensionKind::Case(c.replace_case(new_case));
    }
    Rc::new(new)
}
