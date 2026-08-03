use super::super::common::Case;
use super::super::dimension_kinds::DimensionKind;
use super::super::{DimensionSymbol, MeasureSymbol, MemberSymbol};
use crate::planner::filter::{Filter, FilterGroup, FilterGroupOperator, FilterItem};
use crate::planner::symbols::deps::{DepVisitorMut, SymbolDeps};
use crate::planner::SqlCallFilterParamsItem;
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
    super::map_filter_item_symbols(filter_item, &|symbol| {
        apply_static_filter_to_symbol(symbol, filters)
    })
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

/// Marks every `FILTER_PARAMS` binding in the symbol by whether `filters` reach
/// the members it renders from. A binding renders only where its filters do, so
/// only there do the members its column reads count among the symbol's
/// dependencies and pull their cubes into the join.
pub fn apply_filter_params_activity_to_symbol(
    symbol: &Rc<MemberSymbol>,
    filters: &[FilterItem],
) -> Result<Rc<MemberSymbol>, CubeError> {
    let group = FilterItem::Group(Rc::new(FilterGroup {
        operator: FilterGroupOperator::And,
        items: filters.to_vec(),
    }));
    let mut visitor = ActivityVisitor { filters: &group };
    let mut result = symbol.as_ref().clone();
    result.visit_deps_mut(&mut visitor)?;
    Ok(Rc::new(result))
}

struct ActivityVisitor<'a> {
    filters: &'a FilterItem,
}

impl DepVisitorMut for ActivityVisitor<'_> {
    fn symbol(&mut self, slot: &mut Rc<MemberSymbol>) -> Result<(), CubeError> {
        let mut symbol = slot.as_ref().clone();
        symbol.visit_deps_mut(self)?;
        *slot = Rc::new(symbol);
        Ok(())
    }

    fn filter_params_group(
        &mut self,
        items: &mut [SqlCallFilterParamsItem],
    ) -> Result<(), CubeError> {
        // Matched against the whole group, since that is the predicate the group
        // renders: an OR group survives only when every member of it matches.
        let members = items
            .iter()
            .map(|item| &item.filter_symbol_name)
            .collect::<Vec<_>>();
        let active = self.filters.find_subtree_for_members(&members).is_some();
        for item in items.iter_mut() {
            item.active = active;
        }
        Ok(())
    }
}

/// `apply_filter_params_activity_to_symbol` over every symbol a filter item
/// carries.
pub fn apply_filter_params_activity_to_filter_item(
    filter_item: &FilterItem,
    filters: &[FilterItem],
) -> Result<FilterItem, CubeError> {
    super::map_filter_item_symbols(filter_item, &|symbol| {
        apply_filter_params_activity_to_symbol(symbol, filters)
    })
}
