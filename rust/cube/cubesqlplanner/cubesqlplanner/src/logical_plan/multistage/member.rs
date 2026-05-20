use crate::logical_plan::*;
use cubenativeutils::CubeError;
use std::rc::Rc;

/// What sits inside a `LogicalMultiStageMember`: either a nested plan
/// (Query-rooted, with its own bundled CTE pool) or one of the
/// special leaf nodes that don't need a CTE pool of their own.
#[derive(Clone)]
pub enum MultiStageMemberBody {
    /// Query-rooted body. Pre-agg treats it as one rewrite unit.
    Plan(Rc<LogicalPlan>),
    /// Time-series CTE — drives the date-range scaffold for rolling windows.
    TimeSeries(Rc<MultiStageTimeSeries>),
    /// Rolling-window CTE — applies the window function over a time-series + leaf.
    RollingWindow(Rc<MultiStageRollingWindow>),
}

impl PrettyPrint for MultiStageMemberBody {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        match self {
            Self::Plan(plan) => plan.pretty_print(result, state),
            Self::TimeSeries(ts) => ts.pretty_print(result, state),
            Self::RollingWindow(rw) => rw.pretty_print(result, state),
        }
    }
}

/// Named CTE in a multi-stage chain. The surrounding `LogicalPlan`
/// holds one per CTE its root consumes; `body` is a
/// `MultiStageMemberBody` (a nested plan, time-series, or
/// rolling-window node).
pub struct LogicalMultiStageMember {
    pub name: String,
    pub body: MultiStageMemberBody,
}

impl LogicalNode for LogicalMultiStageMember {
    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::LogicalMultiStageMember(self.clone())
    }

    fn inputs(&self) -> Vec<PlanNode> {
        // For TimeSeries / RollingWindow we surface the underlying node
        // so generic `PlanNode` traversals (cube-name collection,
        // pre-agg rewriter) keep working. For nested plans we stop here
        // — the nested `LogicalPlan` sits outside the PlanNode tree;
        // walkers that need to descend cross the boundary explicitly.
        match &self.body {
            MultiStageMemberBody::Plan(_) => vec![],
            MultiStageMemberBody::TimeSeries(ts) => vec![ts.as_plan_node()],
            MultiStageMemberBody::RollingWindow(rw) => vec![rw.as_plan_node()],
        }
    }

    fn with_inputs(self: Rc<Self>, inputs: Vec<PlanNode>) -> Result<Rc<Self>, CubeError> {
        match &self.body {
            MultiStageMemberBody::Plan(_) => {
                check_inputs_len(&inputs, 0, self.node_name())?;
                Ok(self)
            }
            MultiStageMemberBody::TimeSeries(_) | MultiStageMemberBody::RollingWindow(_) => {
                check_inputs_len(&inputs, 1, self.node_name())?;
                let new_body = match &self.body {
                    MultiStageMemberBody::TimeSeries(_) => {
                        MultiStageMemberBody::TimeSeries(inputs[0].clone().into_logical_node()?)
                    }
                    MultiStageMemberBody::RollingWindow(_) => {
                        MultiStageMemberBody::RollingWindow(inputs[0].clone().into_logical_node()?)
                    }
                    MultiStageMemberBody::Plan(_) => unreachable!(),
                };
                Ok(Rc::new(Self {
                    name: self.name.clone(),
                    body: new_body,
                }))
            }
        }
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
        self.body.pretty_print(result, &details_state);
    }
}
