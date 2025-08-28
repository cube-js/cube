use super::*;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct MeasureSubquery {
    pub schema: Rc<LogicalSchema>,
    pub source: Rc<LogicalJoin>,
}

impl LogicalNode for MeasureSubquery {
    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::MeasureSubquery(self.clone())
    }

    fn inputs(&self) -> Vec<PlanNode> {
        vec![self.source.as_plan_node()]
    }

    fn with_inputs(self: Rc<Self>, inputs: Vec<PlanNode>) -> Result<Rc<Self>, CubeError> {
        check_inputs_len(&inputs, 1, self.node_name())?;
        let source = &inputs[0];
        Ok(Rc::new(Self {
            schema: self.schema.clone(),
            source: source.clone().into_logical_node()?,
        }))
    }

    fn node_name(&self) -> &'static str {
        "MeasureSubquery"
    }
    fn try_from_plan_node(plan_node: PlanNode) -> Result<Rc<Self>, CubeError> {
        if let PlanNode::MeasureSubquery(query) = plan_node {
            Ok(query)
        } else {
            Err(cast_error(&plan_node, "MeasureSubquery"))
        }
    }
}

impl PrettyPrint for MeasureSubquery {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        let details_state = state.new_level();
        result.println("schema:", &state);
        self.schema.pretty_print(result, &details_state);
        result.println("source:", state);
        self.source.pretty_print(result, &details_state);
    }
}
