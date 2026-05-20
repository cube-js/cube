use crate::logical_plan::*;
use cubenativeutils::CubeError;
use std::rc::Rc;

/// What sits inside a `LogicalMultiStageMember`: a Query body (the
/// regular case — leaf, DSQ, multi-stage inode, multiplied bodies) or
/// one of the special leaf nodes that don't fit the Query shape.
/// There's no nested CTE pool here — every CTE lives at the top-level
/// `LogicalPlan.ctes`, and FK refs reach them by name.
#[derive(Clone)]
pub enum MultiStageMemberBody {
    Query(Rc<Query>),
    TimeSeries(Rc<MultiStageTimeSeries>),
    RollingWindow(Rc<MultiStageRollingWindow>),
}

impl MultiStageMemberBody {
    /// Output schema of this CTE body: dimensions/measures projected by
    /// the rendered SQL. For `Query`, this is the embedded
    /// `LogicalSchema`; for `RollingWindow`, the schema it carries; for
    /// `TimeSeries`, a synthetic schema with just the time dimension.
    pub fn schema(&self) -> Rc<LogicalSchema> {
        match self {
            Self::Query(q) => q.schema().clone(),
            Self::RollingWindow(rw) => rw.schema.clone(),
            Self::TimeSeries(ts) => LogicalSchema::default()
                .set_time_dimensions(vec![ts.time_dimension().clone()])
                .into_rc(),
        }
    }
}

impl PrettyPrint for MultiStageMemberBody {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        match self {
            Self::Query(q) => q.pretty_print(result, state),
            Self::TimeSeries(ts) => ts.pretty_print(result, state),
            Self::RollingWindow(rw) => rw.pretty_print(result, state),
        }
    }
}

/// Named CTE in the top-level pool: the surrounding `LogicalPlan` holds
/// one per CTE its tree of FK refs reaches by name. `body` is a
/// `MultiStageMemberBody` — the actual SELECT-shaped node rendered as
/// the CTE body.
pub struct LogicalMultiStageMember {
    pub name: String,
    pub body: MultiStageMemberBody,
}

impl LogicalNode for LogicalMultiStageMember {
    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::LogicalMultiStageMember(self.clone())
    }

    fn inputs(&self) -> Vec<PlanNode> {
        match &self.body {
            MultiStageMemberBody::Query(q) => vec![q.as_plan_node()],
            MultiStageMemberBody::TimeSeries(ts) => vec![ts.as_plan_node()],
            MultiStageMemberBody::RollingWindow(rw) => vec![rw.as_plan_node()],
        }
    }

    fn with_inputs(self: Rc<Self>, inputs: Vec<PlanNode>) -> Result<Rc<Self>, CubeError> {
        check_inputs_len(&inputs, 1, self.node_name())?;
        let new_body = match &self.body {
            MultiStageMemberBody::Query(_) => {
                MultiStageMemberBody::Query(inputs[0].clone().into_logical_node()?)
            }
            MultiStageMemberBody::TimeSeries(_) => {
                MultiStageMemberBody::TimeSeries(inputs[0].clone().into_logical_node()?)
            }
            MultiStageMemberBody::RollingWindow(_) => {
                MultiStageMemberBody::RollingWindow(inputs[0].clone().into_logical_node()?)
            }
        };
        Ok(Rc::new(Self {
            name: self.name.clone(),
            body: new_body,
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
        self.body.pretty_print(result, &details_state);
    }
}
