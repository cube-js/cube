use crate::logical_plan::*;
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Named CTE in a multi-stage chain. The surrounding `LogicalPlan`
/// holds one per CTE its root consumes; the `body` is itself a plan,
/// so a member can bundle its own sub-CTE pool (e.g. leaf bodies that
/// internally use multiplied-measure CTEs).
pub struct LogicalMultiStageMember {
    pub name: String,
    pub body: Rc<LogicalPlan>,
}

impl LogicalNode for LogicalMultiStageMember {
    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::LogicalMultiStageMember(self.clone())
    }

    fn inputs(&self) -> Vec<PlanNode> {
        // The nested `LogicalPlan` sits outside the PlanNode tree —
        // `PlanNode`-based traversals stop here. Walkers that need to
        // descend (cube-name collection, pre-agg rewriter) explicitly
        // cross the boundary into `body`.
        vec![]
    }

    fn with_inputs(self: Rc<Self>, inputs: Vec<PlanNode>) -> Result<Rc<Self>, CubeError> {
        check_inputs_len(&inputs, 0, self.node_name())?;
        Ok(self)
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
