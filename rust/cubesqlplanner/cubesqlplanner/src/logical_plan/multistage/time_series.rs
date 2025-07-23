use crate::logical_plan::*;
use crate::planner::sql_evaluator::MemberSymbol;
use cubenativeutils::CubeError;
use std::rc::Rc;
pub struct MultiStageTimeSeries {
    pub time_dimension: Rc<MemberSymbol>,
    pub date_range: Option<Vec<String>>,
    pub get_date_range_multistage_ref: Option<String>,
}

impl PrettyPrint for MultiStageTimeSeries {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println("Time Series", state);
        let state = state.new_level();
        result.println(
            &format!("time_dimension: {}", self.time_dimension.full_name()),
            &state,
        );
        if let Some(date_range) = &self.date_range {
            result.println(
                &format!("date_range: [{}, {}]", date_range[0], date_range[1]),
                &state,
            );
        }
        if let Some(get_date_range_multistage_ref) = &self.get_date_range_multistage_ref {
            result.println(
                &format!(
                    "get_date_range_multistage_ref: {}",
                    get_date_range_multistage_ref
                ),
                &state,
            );
        }
    }
}

impl LogicalNode for MultiStageTimeSeries {
    type InputsType = EmptyNodeInput;

    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::MultiStageTimeSeries(self.clone())
    }

    fn inputs(&self) -> Self::InputsType {
        EmptyNodeInput::new()
    }

    fn with_inputs(self: Rc<Self>, _inputs: Self::InputsType) -> Result<Rc<Self>, CubeError> {
        Ok(self)
    }

    fn node_name() -> &'static str {
        "MultiStageTimeSeries"
    }

    fn try_from_plan_node(plan_node: PlanNode) -> Result<Rc<Self>, CubeError> {
        if let PlanNode::MultiStageTimeSeries(item) = plan_node {
            Ok(item)
        } else {
            Err(cast_error::<Self>(&plan_node))
        }
    }
}
