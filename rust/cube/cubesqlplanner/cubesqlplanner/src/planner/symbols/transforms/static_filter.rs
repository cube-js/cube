use super::super::common::Case;
use super::super::dimension_kinds::DimensionKind;
use super::super::{DimensionSymbol, MeasureSymbol, MemberSymbol};
use crate::cube_bridge::base_tools::BaseTools;
use crate::cube_bridge::query_context::QueryContextData;
use crate::planner::filter::{Filter, FilterGroup, FilterGroupOperator, FilterItem};
use crate::planner::symbols::deps::{DepVisitorMut, SymbolDeps};
use crate::planner::{QueryProperties, SqlCall, SqlCallFilterParamsItem};
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

/// Specializes symbols against the query node that computes them: prunes
/// `case` branches the node's filters rule out, picks the branch of a
/// QUERY_CONTEXT case and marks which `FILTER_PARAMS` bindings they reach.
pub struct Specializer {
    case_filters: Vec<FilterItem>,
    activity_filters: FilterItem,
    reach: Reach,
    query_context: QueryContextData,
    base_tools: Rc<dyn BaseTools>,
}

enum Reach {
    // A multi-stage stage's member: the members it reads are planned with
    // states of their own.
    OwnNode,
    // Every member the query computes itself, stopping at the ones another
    // query computes.
    ComputedHere { plans_multi_stage: bool },
}

impl Specializer {
    pub fn for_query(query: &QueryProperties, plans_multi_stage: bool) -> Self {
        Self::new(query, query, Reach::ComputedHere { plans_multi_stage })
    }

    pub fn for_stage_member(state: &QueryProperties, query: &QueryProperties) -> Self {
        Self::new(state, query, Reach::OwnNode)
    }

    // A FILTER_PARAMS binding may name any filtered member, not only a
    // dimension, so its activity is matched against the whole set.
    fn new(state: &QueryProperties, query: &QueryProperties, reach: Reach) -> Self {
        Self {
            case_filters: state.dimensions_filters().clone(),
            activity_filters: FilterItem::Group(Rc::new(FilterGroup {
                operator: FilterGroupOperator::And,
                items: state.all_filter_items(),
            })),
            reach,
            query_context: QueryContextData::from_node(state, query),
            base_tools: state.query_tools().query_tools().base_tools().clone(),
        }
    }

    pub fn specialize(&self, symbol: &Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError> {
        let pruned = self.prune_case(symbol)?;
        // Checked after pruning: a multi-stage case pruned to one branch is
        // computed here as a regular member.
        if self.computed_elsewhere(&pruned) {
            return Ok(symbol.clone());
        }
        let mut node = pruned.as_ref().clone();
        node.visit_deps_mut(&mut SpecializeVisitor { specializer: self })?;
        Ok(Rc::new(node))
    }

    fn prune_case(&self, symbol: &Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError> {
        match symbol.as_ref() {
            MemberSymbol::Dimension(dim) => {
                if let Some(case) = dim.case() {
                    if let Some(new_case) = case.apply_static_filter(&self.case_filters) {
                        return Ok(MemberSymbol::new_dimension(replace_dimension_case(
                            dim, new_case,
                        )));
                    }
                }
            }
            MemberSymbol::Measure(meas) => match meas.case() {
                // The stage computing a multi-stage measure resolves it at its own grain.
                Some(Case::CaseContext(case)) if !self.computed_elsewhere(symbol) => {
                    let sql = case.resolve(
                        symbol.cube_id().target(),
                        &self.query_context,
                        self.base_tools.as_ref(),
                    )?;
                    return Ok(MemberSymbol::new_measure(resolve_measure_case(meas, sql)));
                }
                Some(case) => {
                    if let Some(new_case) = case.apply_static_filter(&self.case_filters) {
                        return Ok(MemberSymbol::new_measure(replace_measure_case(
                            meas, new_case,
                        )));
                    }
                }
                None => {}
            },
            MemberSymbol::TimeDimension(_)
            | MemberSymbol::MemberExpression(_)
            | MemberSymbol::Ref(_) => {}
        }
        Ok(symbol.clone())
    }

    // Whether another query computes `symbol`, so this one leaves it as is.
    fn computed_elsewhere(&self, symbol: &MemberSymbol) -> bool {
        match self.reach {
            Reach::ComputedHere { plans_multi_stage } => {
                is_computed_elsewhere(symbol, plans_multi_stage)
            }
            Reach::OwnNode => false,
        }
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
}

impl DepVisitorMut for SpecializeVisitor<'_> {
    fn symbol(&mut self, slot: &mut Rc<MemberSymbol>) -> Result<(), CubeError> {
        if let Reach::ComputedHere { .. } = self.specializer.reach {
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

// The measure computing the chosen branch: its kind aggregates that branch's
// SQL in place of the case.
fn resolve_measure_case(measure: &MeasureSymbol, sql: Rc<SqlCall>) -> Rc<MeasureSymbol> {
    let mut new = measure.clone();
    new.kind = measure.kind.over_input_sql(sql);
    new.case = None;
    Rc::new(new)
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
