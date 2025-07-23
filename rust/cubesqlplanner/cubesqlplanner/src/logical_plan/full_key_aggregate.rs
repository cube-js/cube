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
    PreAggregation(Rc<Query>),
}

impl ResolvedMultipliedMeasures {
    pub fn schema(&self) -> Rc<LogicalSchema> {
        match self {
            ResolvedMultipliedMeasures::ResolveMultipliedMeasures(resolve_multiplied_measures) => {
                resolve_multiplied_measures.schema.clone()
            }
            ResolvedMultipliedMeasures::PreAggregation(simple_query) => simple_query.schema.clone(),
        }
    }
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
    pub schema: Rc<LogicalSchema>,
    pub use_full_join_and_coalesce: bool,
    pub multiplied_measures_resolver: Option<ResolvedMultipliedMeasures>,
    pub multi_stage_subquery_refs: Vec<Rc<MultiStageSubqueryRef>>,
}

impl LogicalNode for FullKeyAggregate {
    type InputsType = OptionNodeInput;

    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::LogicalJoin(self.clone())
    }

    fn inputs(&self) -> Self::InputsType {
        let plan_node = match &self.source {
            QuerySource::LogicalJoin(join) => SingleNodeInput::new(join.as_plan_node()),
            QuerySource::FullKeyAggregate(full_key) => {
                SingleNodeInput::new(full_key.as_plan_node())
            }
            QuerySource::PreAggregation(pre_aggregation) => {
                SingleNodeInput::new(pre_aggregation.as_plan_node())
            }
        };
        plan_node
    }

    fn with_inputs(self: Rc<Self>, inputs: Self::InputsType) -> Result<Rc<Self>, CubeError> {
        let source = match inputs.item() {
            PlanNode::LogicalJoin(item) => QuerySource::LogicalJoin(item.clone()),
            PlanNode::FullKeyAggregate(item) => QuerySource::FullKeyAggregate(item.clone()),
            PlanNode::PreAggregation(item) => QuerySource::PreAggregation(item.clone()),
            _ => {
                return Err(CubeError::internal(format!(
                    "{} is incorrect input for Query node",
                    inputs.item().node_name()
                )))
            }
        };
        Ok(Rc::new(Query {
            multistage_members: self.multistage_members.clone(),
            schema: self.schema.clone(),
            filter: self.filter.clone(),
            modifers: self.modifers.clone(),
            source,
        }))
    }

    fn node_name() -> &'static str {
        "Query"
    }
    fn try_from_plan_node(plan_node: PlanNode) -> Result<Rc<Self>, CubeError> {
        if let PlanNode::Query(query) = plan_node {
            Ok(query)
        } else {
            Err(cast_error::<Self>(&plan_node))
        }
    }
}

impl PrettyPrint for FullKeyAggregate {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println("FullKeyAggregate: ", state);
        let state = state.new_level();
        let details_state = state.new_level();
        result.println(&format!("schema:"), &state);
        self.schema.pretty_print(result, &details_state);
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
