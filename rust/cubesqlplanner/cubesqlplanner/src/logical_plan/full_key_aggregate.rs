use super::*;
use crate::planner::sql_evaluator::MemberSymbol;
use std::rc::Rc;

pub struct MultiStageSubqueryRef {
    pub name: String,
}

impl PrettyPrint for MultiStageSubqueryRef {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println(&format!("MultiStageSubqueryRef: {}", self.name), state);
    }
}

pub enum FullKeyAggregateSource {
    ResolveMultipliedMeasures(Rc<ResolveMultipliedMeasures>),
    MultiStageSubqueryRef(Rc<MultiStageSubqueryRef>),
}

impl PrettyPrint for FullKeyAggregateSource {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        match self {
            Self::ResolveMultipliedMeasures(resolve_multiplied_measures) => {
                resolve_multiplied_measures.pretty_print(result, state);
            }
            Self::MultiStageSubqueryRef(subquery_ref) => {
                subquery_ref.pretty_print(result, state);
            }
        }
    }
}

pub struct FullKeyAggregate {
    pub join_dimensions: Vec<Rc<MemberSymbol>>,
    pub use_full_join_and_coalesce: bool,
    pub sources: Vec<FullKeyAggregateSource>,
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
        result.println("sources:", &state);
        for source in self.sources.iter() {
            source.pretty_print(result, &details_state);
        }
    }
}
