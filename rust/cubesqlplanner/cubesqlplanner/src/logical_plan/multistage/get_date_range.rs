use crate::logical_plan::*;
use crate::planner::sql_evaluator::MemberSymbol;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct MultiStageGetDateRange {
    pub time_dimension: Rc<MemberSymbol>,
    pub source: Rc<LogicalJoin>,
}

impl LogicalNode for MultiStageGetDateRange {
    type InputsType = SingleNodeInput;

    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::MultiStageGetDateRange(self.clone())
    }

    fn inputs(&self) -> Self::InputsType {
        SingleNodeInput::new(self.source.as_plan_node())
    }

    fn with_inputs(self: Rc<Self>, inputs: Self::InputsType) -> Result<Rc<Self>, CubeError> {
        let source = inputs.unpack();

        Ok(Rc::new(Self {
            time_dimension: self.time_dimension.clone(),
            source: source.into_logical_node()?,
        }))
    }

    fn node_name() -> &'static str {
        "MultiStageGetDateRange"
    }

    fn try_from_plan_node(plan_node: PlanNode) -> Result<Rc<Self>, CubeError> {
        if let PlanNode::MultiStageGetDateRange(item) = plan_node {
            Ok(item)
        } else {
            Err(cast_error::<Self>(&plan_node))
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
