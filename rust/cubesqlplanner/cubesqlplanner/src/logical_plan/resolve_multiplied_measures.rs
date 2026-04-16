use super::*;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct ResolveMultipliedMeasures {
    pub schema: Rc<LogicalSchema>,
    pub filter: Rc<LogicalFilter>,
    pub multiplied_cte_names: Vec<String>,
}

impl LogicalNode for ResolveMultipliedMeasures {
    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::ResolveMultipliedMeasures(self.clone())
    }

    fn inputs(&self) -> Vec<PlanNode> {
        vec![]
    }

    fn with_inputs(self: Rc<Self>, inputs: Vec<PlanNode>) -> Result<Rc<Self>, CubeError> {
        check_inputs_len(&inputs, 0, self.node_name())?;
        Ok(self)
    }

    fn node_name(&self) -> &'static str {
        "ResolveMultipliedMeasures"
    }
    fn try_from_plan_node(plan_node: PlanNode) -> Result<Rc<Self>, CubeError> {
        if let PlanNode::ResolveMultipliedMeasures(item) = plan_node {
            Ok(item)
        } else {
            Err(cast_error(&plan_node, "ResolveMultipliedMeasures"))
        }
    }
}

impl PrettyPrint for ResolveMultipliedMeasures {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println("ResolveMultipliedMeasures: ", state);
        let state = state.new_level();
        let details_state = state.new_level();
        result.println("schema:", &state);
        self.schema.pretty_print(result, &details_state);
        result.println("filter:", &state);
        self.filter.pretty_print(result, &details_state);
        result.println(
            &format!("multiplied_cte_names: {:?}", self.multiplied_cte_names),
            &state,
        );
    }
}
