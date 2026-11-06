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

/// Specializes symbols against the filters of the query that computes them:
/// prunes `case` branches the filters rule out and marks which `FILTER_PARAMS`
/// bindings they reach. Only an active binding renders, so only its column's
/// members join the symbol's dependencies.
pub struct Specializer {
    case_filters: Vec<FilterItem>,
    activity_filters: FilterItem,
    plans_multi_stage: bool,
}

impl Specializer {
    // A FILTER_PARAMS binding may name any filtered member, not only a
    // dimension, so its activity is matched against the whole set.
    pub fn new(
        dimensions_filters: &[FilterItem],
        all_filters: &[FilterItem],
        plans_multi_stage: bool,
    ) -> Self {
        Self {
            case_filters: dimensions_filters.to_vec(),
            activity_filters: FilterItem::Group(Rc::new(FilterGroup {
                operator: FilterGroupOperator::And,
                items: all_filters.to_vec(),
            })),
            plans_multi_stage,
        }
    }

    /// Specializes every member the query computes itself, leaving the members
    /// computed by other queries as they are.
    pub fn specialize(&self, symbol: &Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError> {
        let pruned = prune_case(symbol, &self.case_filters)?;
        // Checked after pruning: a multi-stage case pruned to one branch is
        // computed here as a regular member.
        if is_computed_elsewhere(&pruned, self.plans_multi_stage) {
            return Ok(symbol.clone());
        }
        self.specialize_node(&pruned, true)
    }

    /// Specializes only the member a multi-stage stage computes: the members it
    /// reads are planned with states of their own.
    pub fn specialize_stage_member(
        &self,
        member: &Rc<MemberSymbol>,
    ) -> Result<Rc<MemberSymbol>, CubeError> {
        let pruned = prune_case(member, &self.case_filters)?;
        self.specialize_node(&pruned, false)
    }

    fn specialize_node(
        &self,
        symbol: &Rc<MemberSymbol>,
        deep: bool,
    ) -> Result<Rc<MemberSymbol>, CubeError> {
        let mut node = symbol.as_ref().clone();
        node.visit_deps_mut(&mut SpecializeVisitor {
            specializer: self,
            deep,
        })?;
        Ok(Rc::new(node))
    }
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

struct SpecializeVisitor<'a> {
    specializer: &'a Specializer,
    // `false` leaves the members the symbol reads as they are.
    deep: bool,
}

impl DepVisitorMut for SpecializeVisitor<'_> {
    fn symbol(&mut self, slot: &mut Rc<MemberSymbol>) -> Result<(), CubeError> {
        if self.deep {
            *slot = self.specializer.specialize(slot)?;
        }
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
        let active = self
            .specializer
            .activity_filters
            .find_subtree_for_members(&members)
            .is_some();
        for item in items.iter_mut() {
            item.active = active;
        }
        Ok(())
    }
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
