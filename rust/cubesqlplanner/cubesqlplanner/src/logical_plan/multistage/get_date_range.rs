use crate::logical_plan::*;
use crate::planner::sql_evaluator::MemberSymbol;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct MultiStageGetDateRange {
    pub time_dimension: Rc<MemberSymbol>,
    pub source: Rc<LogicalJoin>,
}

impl LogicalNode for MultiStageGetDateRange {
    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::MultiStageGetDateRange(self.clone())
    }

    fn inputs(&self) -> Vec<PlanNode> {
        vec![self.source.as_plan_node()]
    }

    fn with_inputs(self: Rc<Self>, inputs: Vec<PlanNode>) -> Result<Rc<Self>, CubeError> {
        check_inputs_len(&inputs, 1, self.node_name())?;
        let source = &inputs[0];

        Ok(Rc::new(Self {
            time_dimension: self.time_dimension.clone(),
            source: source.clone().into_logical_node()?,
        }))
    }

    fn node_name(&self) -> &'static str {
        "MultiStageGetDateRange"
    }

    fn try_from_plan_node(plan_node: PlanNode) -> Result<Rc<Self>, CubeError> {
        if let PlanNode::MultiStageGetDateRange(item) = plan_node {
            Ok(item)
        } else {
            Err(cast_error(&plan_node, "MultiStageGetDateRange"))
        }
    }
}

impl PrettyPrint for MultiStageGetDateRange {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println("Get Date Range", state);
        let state = state.new_level();
        let details_state = state.new_level();
        result.println(
            &format!("time_dimension: {}", self.time_dimension.full_name()),
            &details_state,
        );
        result.println("source:", &state);
        self.source.pretty_print(result, &details_state);
    }
}
