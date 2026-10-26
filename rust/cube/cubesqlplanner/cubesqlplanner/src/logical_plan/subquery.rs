use crate::logical_plan::*;
use crate::planner::InputId;
use cubenativeutils::CubeError;
use std::rc::Rc;

/// What a `Subquery` computes.
pub enum SubqueryBody {
    /// A leaf query computing the base values of a multi-stage chain.
    LeafMeasure(Rc<Query>),
    MultipliedMeasure(Rc<AggregateMultipliedSubquery>),
    MeasureCalculation(Rc<MultiStageMeasureCalculation>),
    DimensionCalculation(Rc<MultiStageDimensionCalculation>),
    GetDateRange(Rc<MultiStageGetDateRange>),
    TimeSeries(Rc<MultiStageTimeSeries>),
    RollingWindow(Rc<MultiStageRollingWindow>),
}

impl SubqueryBody {
    fn as_plan_node(&self) -> PlanNode {
        match self {
            Self::LeafMeasure(item) => item.as_plan_node(),
            Self::MultipliedMeasure(item) => item.as_plan_node(),
            Self::MeasureCalculation(item) => item.as_plan_node(),
            Self::DimensionCalculation(item) => item.as_plan_node(),
            Self::GetDateRange(item) => item.as_plan_node(),
            Self::TimeSeries(item) => item.as_plan_node(),
            Self::RollingWindow(item) => item.as_plan_node(),
        }
    }

    fn with_plan_node(&self, plan_node: PlanNode) -> Result<Self, CubeError> {
        Ok(match self {
            Self::LeafMeasure(_) => Self::LeafMeasure(plan_node.into_logical_node()?),
            Self::MultipliedMeasure(_) => Self::MultipliedMeasure(plan_node.into_logical_node()?),
            Self::MeasureCalculation(_) => Self::MeasureCalculation(plan_node.into_logical_node()?),
            Self::DimensionCalculation(_) => {
                Self::DimensionCalculation(plan_node.into_logical_node()?)
            }
            Self::GetDateRange(_) => Self::GetDateRange(plan_node.into_logical_node()?),
            Self::TimeSeries(_) => Self::TimeSeries(plan_node.into_logical_node()?),
            Self::RollingWindow(_) => Self::RollingWindow(plan_node.into_logical_node()?),
        })
    }
}

impl PrettyPrint for SubqueryBody {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        match self {
            Self::LeafMeasure(measure) => measure.pretty_print(result, state),
            Self::MultipliedMeasure(subquery) => subquery.pretty_print(result, state),
            Self::MeasureCalculation(calculation) => calculation.pretty_print(result, state),
            Self::DimensionCalculation(calculation) => calculation.pretty_print(result, state),
            Self::GetDateRange(get_date_range) => get_date_range.pretty_print(result, state),
            Self::TimeSeries(time_series) => time_series.pretty_print(result, state),
            Self::RollingWindow(rolling_window) => rolling_window.pretty_print(result, state),
        }
    }
}

/// A named subquery of the plan, read through `SubqueryRef`s. `RootQuery`
/// holds the flat list of them; `inline` asks to render it in place of its
/// refs instead of as a `WITH` entry.
pub struct Subquery {
    pub id: InputId,
    pub name: String,
    pub body: SubqueryBody,
    pub inline: bool,
}

impl Subquery {
    pub fn new(id: InputId, name: String, body: SubqueryBody) -> Self {
        Self {
            id,
            name,
            body,
            inline: false,
        }
    }

    pub fn with_body(&self, body: SubqueryBody) -> Rc<Self> {
        Rc::new(Self {
            id: self.id,
            name: self.name.clone(),
            body,
            inline: self.inline,
        })
    }
}

impl LogicalNode for Subquery {
    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::Subquery(self.clone())
    }

    fn inputs(&self) -> Vec<PlanNode> {
        vec![self.body.as_plan_node()]
    }

    fn with_inputs(self: Rc<Self>, inputs: Vec<PlanNode>) -> Result<Rc<Self>, CubeError> {
        check_inputs_len(&inputs, 1, self.node_name())?;
        let input = inputs[0].clone();

        Ok(self.with_body(self.body.with_plan_node(input)?))
    }

    fn node_name(&self) -> &'static str {
        "Subquery"
    }

    fn try_from_plan_node(plan_node: PlanNode) -> Result<Rc<Self>, CubeError> {
        if let PlanNode::Subquery(item) = plan_node {
            Ok(item)
        } else {
            Err(cast_error(&plan_node, "Subquery"))
        }
    }
}

impl PrettyPrint for Subquery {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        let inline = if self.inline { " (inline)" } else { "" };
        result.println(
            &format!("Subquery `{}` {}{}: ", self.name, self.id, inline),
            state,
        );
        let details_state = state.new_level();
        self.body.pretty_print(result, &details_state);
    }
}
