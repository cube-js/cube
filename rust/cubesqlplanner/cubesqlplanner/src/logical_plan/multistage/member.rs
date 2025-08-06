use crate::logical_plan::*;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub enum MultiStageMemberLogicalType {
    LeafMeasure(Rc<MultiStageLeafMeasure>),
    MeasureCalculation(Rc<MultiStageMeasureCalculation>),
    GetDateRange(Rc<MultiStageGetDateRange>),
    TimeSeries(Rc<MultiStageTimeSeries>),
    RollingWindow(Rc<MultiStageRollingWindow>),
}

impl MultiStageMemberLogicalType {
    fn as_plan_node(&self) -> PlanNode {
        match self {
            Self::LeafMeasure(item) => item.as_plan_node(),
            Self::MeasureCalculation(item) => item.as_plan_node(),
            Self::GetDateRange(item) => item.as_plan_node(),
            Self::TimeSeries(item) => item.as_plan_node(),
            Self::RollingWindow(item) => item.as_plan_node(),
        }
    }

    fn with_plan_node(&self, plan_node: PlanNode) -> Result<Self, CubeError> {
        Ok(match self {
            Self::LeafMeasure(_) => Self::LeafMeasure(plan_node.into_logical_node()?),
            Self::MeasureCalculation(_) => Self::MeasureCalculation(plan_node.into_logical_node()?),
            Self::GetDateRange(_) => Self::GetDateRange(plan_node.into_logical_node()?),
            Self::TimeSeries(_) => Self::TimeSeries(plan_node.into_logical_node()?),
            Self::RollingWindow(_) => Self::RollingWindow(plan_node.into_logical_node()?),
        })
    }
}

impl PrettyPrint for MultiStageMemberLogicalType {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        match self {
            Self::LeafMeasure(measure) => measure.pretty_print(result, state),
            Self::MeasureCalculation(calculation) => calculation.pretty_print(result, state),
            Self::GetDateRange(get_date_range) => get_date_range.pretty_print(result, state),
            Self::TimeSeries(time_series) => time_series.pretty_print(result, state),
            Self::RollingWindow(rolling_window) => rolling_window.pretty_print(result, state),
        }
    }
}

pub struct LogicalMultiStageMember {
    pub name: String,
    pub member_type: MultiStageMemberLogicalType,
}

impl LogicalNode for LogicalMultiStageMember {
    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::LogicalMultiStageMember(self.clone())
    }

    fn inputs(&self) -> Vec<PlanNode> {
        vec![self.member_type.as_plan_node()]
    }

    fn with_inputs(self: Rc<Self>, inputs: Vec<PlanNode>) -> Result<Rc<Self>, CubeError> {
        check_inputs_len(&inputs, 1, self.node_name())?;
        let input = inputs[0].clone();

        Ok(Rc::new(Self {
            name: self.name.clone(),
            member_type: self.member_type.with_plan_node(input)?,
        }))
    }

    fn node_name(&self) -> &'static str {
        "LogicalMultiStageMember"
    }

    fn try_from_plan_node(plan_node: PlanNode) -> Result<Rc<Self>, CubeError> {
        if let PlanNode::LogicalMultiStageMember(item) = plan_node {
            Ok(item)
        } else {
            Err(cast_error(&plan_node, "LogicalMultiStageMember"))
        }
    }
}

impl PrettyPrint for LogicalMultiStageMember {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println(&format!("MultiStageMember `{}`: ", self.name), state);
        let details_state = state.new_level();
        self.member_type.pretty_print(result, &details_state);
    }
}
