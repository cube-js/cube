use super::*;
use crate::planner::sql_evaluator::MemberSymbol;
use cubenativeutils::CubeError;
use std::rc::Rc;
use typed_builder::TypedBuilder;

#[derive(TypedBuilder)]
pub struct MultiStageSubqueryRef {
    name: String,
    #[builder(default)]
    symbols: Vec<Rc<MemberSymbol>>,
    schema: Rc<LogicalSchema>,
}

impl MultiStageSubqueryRef {
    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn symbols(&self) -> &Vec<Rc<MemberSymbol>> {
        &self.symbols
    }

    pub fn schema(&self) -> &Rc<LogicalSchema> {
        &self.schema
    }
}

impl PrettyPrint for MultiStageSubqueryRef {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println(&format!("MultiStageSubqueryRef: {}", self.name()), state);
        let state = state.new_level();
        result.println(
            &format!("symbols: {}", print_symbols(self.symbols())),
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
            ResolvedMultipliedMeasures::PreAggregation(simple_query) => {
                simple_query.schema().clone()
            }
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

#[derive(Clone, TypedBuilder)]
pub struct FullKeyAggregate {
    schema: Rc<LogicalSchema>,
    use_full_join_and_coalesce: bool,
    #[builder(default)]
    multiplied_measures_resolver: Option<ResolvedMultipliedMeasures>,
    #[builder(default)]
    multi_stage_subquery_refs: Vec<Rc<MultiStageSubqueryRef>>,
}

impl FullKeyAggregate {
    pub fn schema(&self) -> &Rc<LogicalSchema> {
        &self.schema
    }

    pub fn use_full_join_and_coalesce(&self) -> bool {
        self.use_full_join_and_coalesce
    }

    pub fn multiplied_measures_resolver(&self) -> &Option<ResolvedMultipliedMeasures> {
        &self.multiplied_measures_resolver
    }

    pub fn multi_stage_subquery_refs(&self) -> &Vec<Rc<MultiStageSubqueryRef>> {
        &self.multi_stage_subquery_refs
    }

    pub fn is_empty(&self) -> bool {
        self.multi_stage_subquery_refs.is_empty() && self.multiplied_measures_resolver.is_none()
    }
}

impl LogicalNode for FullKeyAggregate {
    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::FullKeyAggregate(self.clone())
    }

    fn inputs(&self) -> Vec<PlanNode> {
        if let Some(resolver) = self.multiplied_measures_resolver() {
            vec![match resolver {
                ResolvedMultipliedMeasures::ResolveMultipliedMeasures(item) => item.as_plan_node(),
                ResolvedMultipliedMeasures::PreAggregation(item) => item.as_plan_node(),
            }]
        } else {
            vec![]
        }
    }

    fn with_inputs(self: Rc<Self>, inputs: Vec<PlanNode>) -> Result<Rc<Self>, CubeError> {
        let multiplied_measures_resolver = if self.multiplied_measures_resolver().is_none() {
            check_inputs_len(&inputs, 0, self.node_name())?;
            None
        } else {
            check_inputs_len(&inputs, 1, self.node_name())?;
            let input_source = &inputs[0];

            Some(
                match self.multiplied_measures_resolver().as_ref().unwrap() {
                    ResolvedMultipliedMeasures::ResolveMultipliedMeasures(_) => {
                        ResolvedMultipliedMeasures::ResolveMultipliedMeasures(
                            input_source.clone().into_logical_node()?,
                        )
                    }
                    ResolvedMultipliedMeasures::PreAggregation(_) => {
                        ResolvedMultipliedMeasures::PreAggregation(
                            input_source.clone().into_logical_node()?,
                        )
                    }
                },
            )
        };

        Ok(Rc::new(
            Self::builder()
                .schema(self.schema().clone())
                .use_full_join_and_coalesce(self.use_full_join_and_coalesce())
                .multiplied_measures_resolver(multiplied_measures_resolver)
                .multi_stage_subquery_refs(self.multi_stage_subquery_refs().clone())
                .build(),
        ))
    }

    fn node_name(&self) -> &'static str {
        "FullKeyAggregate"
    }
    fn try_from_plan_node(plan_node: PlanNode) -> Result<Rc<Self>, CubeError> {
        if let PlanNode::FullKeyAggregate(item) = plan_node {
            Ok(item)
        } else {
            Err(cast_error(&plan_node, "FullKeyAggregate"))
        }
    }
}

impl PrettyPrint for FullKeyAggregate {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println("FullKeyAggregate: ", state);
        let state = state.new_level();
        let details_state = state.new_level();
        result.println(&format!("schema:"), &state);
        self.schema().pretty_print(result, &details_state);
        result.println(
            &format!(
                "use_full_join_and_coalesce: {}",
                self.use_full_join_and_coalesce()
            ),
            &state,
        );
        if let Some(resolve_multiplied_measures) = self.multiplied_measures_resolver() {
            result.println("multiplied measures resolver:", &state);
            resolve_multiplied_measures.pretty_print(result, &details_state);
        }

        if !self.multi_stage_subquery_refs().is_empty() {
            result.println("multi_stage_subquery_refs:", &state);
            for subquery_ref in self.multi_stage_subquery_refs().iter() {
                subquery_ref.pretty_print(result, &details_state);
            }
        }
    }
}
