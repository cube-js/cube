use super::*;
use crate::planner::sql_evaluator::MemberSymbol;
use cubenativeutils::CubeError;
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
        PlanNode::FullKeyAggregate(self.clone())
    }

    fn inputs(&self) -> Self::InputsType {
        let plan_node = self
            .multiplied_measures_resolver
            .as_ref()
            .map(|resolver| match resolver {
                ResolvedMultipliedMeasures::ResolveMultipliedMeasures(item) => item.as_plan_node(),
                ResolvedMultipliedMeasures::PreAggregation(item) => item.as_plan_node(),
            });
        OptionNodeInput::new(plan_node)
    }

    fn with_inputs(self: Rc<Self>, inputs: Self::InputsType) -> Result<Rc<Self>, CubeError> {
        let input = inputs.unpack();
        let multiplied_measures_resolver = if self.multiplied_measures_resolver.is_none()
            && input.is_none()
        {
            None
        } else if let (Some(self_source), Some(input_source)) =
            (&self.multiplied_measures_resolver, input)
        {
            Some(match self_source {
                ResolvedMultipliedMeasures::ResolveMultipliedMeasures(_) => {
                    ResolvedMultipliedMeasures::ResolveMultipliedMeasures(
                        input_source.into_logical_node()?,
                    )
                }
                ResolvedMultipliedMeasures::PreAggregation(_) => {
                    ResolvedMultipliedMeasures::PreAggregation(input_source.into_logical_node()?)
                }
            })
        } else {
            return Err(CubeError::internal(format!(
                "Wrong inputs for FullKeyAggregate node"
            )));
        };

        Ok(Rc::new(Self {
            schema: self.schema.clone(),
            use_full_join_and_coalesce: self.use_full_join_and_coalesce,
            multiplied_measures_resolver,
            multi_stage_subquery_refs: self.multi_stage_subquery_refs.clone(),
        }))
    }

    fn node_name() -> &'static str {
        "FullKeyAggregate"
    }
    fn try_from_plan_node(plan_node: PlanNode) -> Result<Rc<Self>, CubeError> {
        if let PlanNode::FullKeyAggregate(item) = plan_node {
            Ok(item)
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
