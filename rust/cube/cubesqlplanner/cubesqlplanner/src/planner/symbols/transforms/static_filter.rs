use super::super::common::Case;
use super::super::dimension_kinds::DimensionKind;
use super::super::{DimensionSymbol, MeasureSymbol, MemberSymbol};
use crate::planner::filter::{Filter, FilterGroup, FilterGroupOperator, FilterItem};
use crate::planner::symbols::deps::{self, DepVisitorMut, SymbolDeps};
use crate::planner::SqlCallFilterParamsItem;
use cubenativeutils::CubeError;
use std::ops::ControlFlow;
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
    if let Ok(dim) = symbol.peel_refs().as_dimension() {
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

// The body of a subquery dimension, and of a multi-stage member when the query
// plans those as stages, is computed by another query against its own filters.
// A view member or a time dimension only wraps a member, so it is looked through.
fn is_computed_elsewhere(symbol: &MemberSymbol, plans_multi_stage: bool) -> bool {
    match symbol {
        MemberSymbol::Dimension(d) => (plans_multi_stage && d.is_multi_stage()) || d.is_sub_query(),
        MemberSymbol::Measure(m) => plans_multi_stage && m.is_multi_stage(),
        MemberSymbol::TimeDimension(_)
        | MemberSymbol::Ref(_)
        | MemberSymbol::MemberExpression(_) => false,
    }
}

/// Prunes the `case` of every member the query computes itself against
/// `filters`, leaving the members computed by other queries as they are.
pub fn apply_static_filter_to_symbol(
    symbol: &Rc<MemberSymbol>,
    filters: &Vec<FilterItem>,
    plans_multi_stage: bool,
) -> Result<Rc<MemberSymbol>, CubeError> {
    deps::apply_recursive_until(symbol, &|node| {
        prune_case_if_computed_here(node, filters, plans_multi_stage)
    })
}

/// Prunes only the `case` of the member a multi-stage stage computes: the
/// members it reads are planned with states of their own.
pub fn apply_static_filter_to_stage_member(
    member: &Rc<MemberSymbol>,
    filters: &Vec<FilterItem>,
) -> Result<Rc<MemberSymbol>, CubeError> {
    prune_case(member, filters)
}

// A member computed elsewhere stays whole, unless pruning leaves it a single
// branch the query then computes as a regular member.
fn prune_case_if_computed_here(
    node: &Rc<MemberSymbol>,
    filters: &Vec<FilterItem>,
    plans_multi_stage: bool,
) -> Result<ControlFlow<Rc<MemberSymbol>, Rc<MemberSymbol>>, CubeError> {
    let pruned = prune_case(node, filters)?;
    Ok(if is_computed_elsewhere(&pruned, plans_multi_stage) {
        ControlFlow::Break(node.clone())
    } else {
        ControlFlow::Continue(pruned)
    })
}

fn prune_case(
    symbol: &Rc<MemberSymbol>,
    filters: &Vec<FilterItem>,
) -> Result<Rc<MemberSymbol>, CubeError> {
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
        MemberSymbol::TimeDimension(_)
        | MemberSymbol::MemberExpression(_)
        | MemberSymbol::Ref(_) => {}
    }
    Ok(symbol.clone())
}

pub fn apply_static_filter_to_filter_item(
    filter_item: &FilterItem,
    filters: &Vec<FilterItem>,
    plans_multi_stage: bool,
) -> Result<FilterItem, CubeError> {
    super::map_filter_item_symbols(filter_item, &|symbol| {
        apply_static_filter_to_symbol(symbol, filters, plans_multi_stage)
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

/// Marks every `FILTER_PARAMS` binding of the members the query computes itself
/// by whether `filters` reach the members it renders from. Only an active
/// binding renders, so only its column's members join the symbol's dependencies.
pub fn apply_filter_params_activity_to_symbol(
    symbol: &Rc<MemberSymbol>,
    filters: &FilterItem,
    plans_multi_stage: bool,
) -> Result<Rc<MemberSymbol>, CubeError> {
    if is_computed_elsewhere(symbol, plans_multi_stage) {
        return Ok(symbol.clone());
    }
    mark_filter_params_activity(symbol, filters, Some(plans_multi_stage))
}

/// Marks only the bindings of the member a multi-stage stage computes: the
/// members it reads are planned with states of their own.
pub fn apply_filter_params_activity_to_stage_member(
    member: &Rc<MemberSymbol>,
    filters: &FilterItem,
) -> Result<Rc<MemberSymbol>, CubeError> {
    mark_filter_params_activity(member, filters, None)
}

fn mark_filter_params_activity(
    symbol: &Rc<MemberSymbol>,
    filters: &FilterItem,
    plans_multi_stage: Option<bool>,
) -> Result<Rc<MemberSymbol>, CubeError> {
    let mut visitor = ActivityVisitor {
        filters,
        plans_multi_stage,
    };
    let mut result = symbol.as_ref().clone();
    result.visit_deps_mut(&mut visitor)?;
    Ok(Rc::new(result))
}

/// The filter set as the one item `apply_filter_params_activity_to_symbol`
/// matches against.
pub fn filter_params_activity_filters(filters: &[FilterItem]) -> FilterItem {
    FilterItem::Group(Rc::new(FilterGroup {
        operator: FilterGroupOperator::And,
        items: filters.to_vec(),
    }))
}

struct ActivityVisitor<'a> {
    filters: &'a FilterItem,
    // `None` leaves the members the symbol reads unmarked.
    plans_multi_stage: Option<bool>,
}

impl DepVisitorMut for ActivityVisitor<'_> {
    fn symbol(&mut self, slot: &mut Rc<MemberSymbol>) -> Result<(), CubeError> {
        let Some(plans_multi_stage) = self.plans_multi_stage else {
            return Ok(());
        };
        if is_computed_elsewhere(slot, plans_multi_stage) {
            return Ok(());
        }
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
    filters: &FilterItem,
    plans_multi_stage: bool,
) -> Result<FilterItem, CubeError> {
    super::map_filter_item_symbols(filter_item, &|symbol| {
        apply_filter_params_activity_to_symbol(symbol, filters, plans_multi_stage)
    })
}
