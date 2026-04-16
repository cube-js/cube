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

#[derive(Clone, TypedBuilder)]
pub struct FullKeyAggregate {
    schema: Rc<LogicalSchema>,
    use_full_join_and_coalesce: bool,
    #[builder(default)]
    pre_aggregation_override: Option<Rc<Query>>,
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

    pub fn pre_aggregation_override(&self) -> &Option<Rc<Query>> {
        &self.pre_aggregation_override
    }

    pub fn multi_stage_subquery_refs(&self) -> &Vec<Rc<MultiStageSubqueryRef>> {
        &self.multi_stage_subquery_refs
    }

    pub fn is_empty(&self) -> bool {
        self.multi_stage_subquery_refs.is_empty() && self.pre_aggregation_override.is_none()
    }
}

impl LogicalNode for FullKeyAggregate {
    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::FullKeyAggregate(self.clone())
    }

    fn inputs(&self) -> Vec<PlanNode> {
        if let Some(pre_agg) = &self.pre_aggregation_override {
            vec![pre_agg.as_plan_node()]
        } else {
            vec![]
        }
    }

    fn with_inputs(self: Rc<Self>, inputs: Vec<PlanNode>) -> Result<Rc<Self>, CubeError> {
        let pre_aggregation_override = if self.pre_aggregation_override.is_none() {
            check_inputs_len(&inputs, 0, self.node_name())?;
            None
        } else {
            check_inputs_len(&inputs, 1, self.node_name())?;
            Some(inputs[0].clone().into_logical_node()?)
        };

        Ok(Rc::new(
            Self::builder()
                .schema(self.schema().clone())
                .use_full_join_and_coalesce(self.use_full_join_and_coalesce())
                .pre_aggregation_override(pre_aggregation_override)
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
        if let Some(pre_agg) = &self.pre_aggregation_override {
            result.println("pre_aggregation_override:", &state);
            pre_agg.pretty_print(result, &details_state);
        }

        if !self.multi_stage_subquery_refs().is_empty() {
            result.println("multi_stage_subquery_refs:", &state);
            for subquery_ref in self.multi_stage_subquery_refs().iter() {
                subquery_ref.pretty_print(result, &details_state);
            }
        }
    }
}
