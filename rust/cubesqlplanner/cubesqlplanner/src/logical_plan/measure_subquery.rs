use super::*;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct MeasureSubquery {
    pub schema: Rc<LogicalSchema>,
    pub source: Rc<LogicalJoin>,
}

impl LogicalNode for MeasureSubquery {
    type InputsType = SingleNodeInput;

    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::MeasureSubquery(self.clone())
    }

    fn inputs(&self) -> Self::InputsType {
        SingleNodeInput::new(self.source.as_plan_node())
    }

    fn with_inputs(self: Rc<Self>, inputs: Self::InputsType) -> Result<Rc<Self>, CubeError> {
        let source = inputs.unpack();
        Ok(Rc::new(Self {
            schema: self.schema.clone(),
            source: source.into_logical_node()?,
        }))
    }

    fn node_name() -> &'static str {
        "MeasureSubquery"
    }
    fn try_from_plan_node(plan_node: PlanNode) -> Result<Rc<Self>, CubeError> {
        if let PlanNode::MeasureSubquery(query) = plan_node {
            Ok(query)
        } else {
            Err(cast_error::<Self>(&plan_node))
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
