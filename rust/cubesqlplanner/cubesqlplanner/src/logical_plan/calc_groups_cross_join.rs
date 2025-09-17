use crate::planner::sql_evaluator::MemberSymbol;

use super::*;
use cubenativeutils::CubeError;
use std::rc::Rc;
use typed_builder::TypedBuilder;

#[derive(Clone)]
pub struct CalcGroupDescription {
    pub symbol: Rc<MemberSymbol>,
    pub values: Vec<String>,
}

impl PrettyPrint for CalcGroupDescription {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        let state = state.new_level();
        result.println(
            &format!("{}:{}", self.symbol.full_name(), self.values.join(", ")),
            &state,
        );
    }
}

#[derive(Clone, TypedBuilder)]
pub struct CalcGroupsCrossJoin {
    source: BaseQuerySource,
    calc_groups: Vec<Rc<CalcGroupDescription>>,
}

impl CalcGroupsCrossJoin {
    pub fn source(&self) -> &BaseQuerySource {
        &self.source
    }

    pub fn calc_groups(&self) -> &Vec<Rc<CalcGroupDescription>> {
        &self.calc_groups
    }
}

impl LogicalNode for CalcGroupsCrossJoin {
    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::CalcGroupsCrossJoin(self.clone())
    }

    fn inputs(&self) -> Vec<PlanNode> {
        vec![self.source.as_plan_node()]
    }

    fn with_inputs(self: Rc<Self>, inputs: Vec<PlanNode>) -> Result<Rc<Self>, CubeError> {
        check_inputs_len(&inputs, 1, self.node_name())?;
        let source = &inputs[0];

        Ok(Rc::new(Self {
            calc_groups: self.calc_groups.clone(),
            source: self.source.with_plan_node(source.clone())?,
        }))
    }

    fn node_name(&self) -> &'static str {
        "CalcGroupsCrossJoin"
    }

    fn try_from_plan_node(plan_node: PlanNode) -> Result<Rc<Self>, CubeError> {
        if let PlanNode::CalcGroupsCrossJoin(item) = plan_node {
            Ok(item)
        } else {
            Err(cast_error(&plan_node, "MultiStageGetDateRange"))
        }
    }
}

impl PrettyPrint for CalcGroupsCrossJoin {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        let state = state.new_level();
        result.println("CaclGroupsCrossJoin: ", &state);
        let state = state.new_level();
        result.println("Groups: ", &state);
        let details_state = state.new_level();
        for group in &self.calc_groups {
            group.pretty_print(result, &details_state);
        }
        result.println("Source: ", &state);
        self.source.pretty_print(result, &details_state);
    }
}
