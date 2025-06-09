use super::*;
use crate::planner::sql_evaluator::MemberSymbol;
use std::rc::Rc;

pub struct MultiStageSubqueryRef {
    pub name: String,
    pub symbols: Vec<Rc<MemberSymbol>>,
}

impl PrettyPrint for MultiStageSubqueryRef {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println(&format!("MultiStageSubqueryRef: {}", self.name), state);
        let state = state.new_level();
        result.println(
            &format!("symbols: {}", print_symbols(&self.symbols)),
            &state,
        );
    }
}

#[derive(Clone)]
pub enum ResolvedMultipliedMeasures {
    ResolveMultipliedMeasures(Rc<ResolveMultipliedMeasures>),
    PreAggregation(Rc<SimpleQuery>),
}

impl PrettyPrint for ResolvedMultipliedMeasures {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        match self {
            Self::ResolveMultipliedMeasures(resolve_multiplied_measures) => {
                resolve_multiplied_measures.pretty_print(result, state);
            }
            Self::PreAggregation(pre_aggregation) => {
                result.println("PreAggregation query:", state);
                pre_aggregation.pretty_print(result, state);
            }
        }
    }
}

pub struct FullKeyAggregate {
    pub join_dimensions: Vec<Rc<MemberSymbol>>,
    pub use_full_join_and_coalesce: bool,
    pub multiplied_measures_resolver: Option<ResolvedMultipliedMeasures>,
    pub multi_stage_subquery_refs: Vec<Rc<MultiStageSubqueryRef>>,
}

impl PrettyPrint for FullKeyAggregate {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println("FullKeyAggregate: ", state);
        let state = state.new_level();
        let details_state = state.new_level();
        result.println(
            &format!("join_dimensions: {}", print_symbols(&self.join_dimensions)),
            &state,
        );
        result.println(
            &format!(
                "use_full_join_and_coalesce: {}",
                self.use_full_join_and_coalesce
            ),
            &state,
        );
        if let Some(resolve_multiplied_measures) = &self.multiplied_measures_resolver {
            result.println("multiplied measures resolver:", &state);
            resolve_multiplied_measures.pretty_print(result, &details_state);
        }

        if !self.multi_stage_subquery_refs.is_empty() {
            result.println("multi_stage_subquery_refs:", &state);
            for subquery_ref in self.multi_stage_subquery_refs.iter() {
                subquery_ref.pretty_print(result, &details_state);
            }
        }
    }
}
