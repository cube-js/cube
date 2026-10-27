use super::*;
use crate::planner::{BaseCube, CubeId};
use cubenativeutils::CubeError;
use std::rc::Rc;
use typed_builder::TypedBuilder;

/// A cube referenced from the logical plan — wraps the planner's
/// `BaseCube`.
#[derive(Clone, TypedBuilder)]
pub struct Cube {
    cube: Rc<BaseCube>,
}

impl Cube {
    pub fn cube_id(&self) -> &CubeId {
        self.cube.cube_id()
    }

    pub fn cube(&self) -> &Rc<BaseCube> {
        &self.cube
    }
}

impl PrettyPrint for Cube {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println(&format!("Cube: {}", self.cube_id()), state);
    }
}

impl Cube {
    pub fn new(cube: Rc<BaseCube>) -> Rc<Self> {
        Rc::new(Self::builder().cube(cube).build())
    }
}

impl LogicalNode for Cube {
    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::Cube(self.clone())
    }

    fn inputs(&self) -> Vec<PlanNode> {
        vec![] // Cube has no inputs
    }

    fn with_inputs(self: Rc<Self>, inputs: Vec<PlanNode>) -> Result<Rc<Self>, CubeError> {
        check_inputs_len(&inputs, 0, self.node_name())?;
        Ok(self)
    }

    fn node_name(&self) -> &'static str {
        "Cube"
    }
    fn try_from_plan_node(plan_node: PlanNode) -> Result<Rc<Self>, CubeError> {
        if let PlanNode::Cube(item) = plan_node {
            Ok(item)
        } else {
            Err(cast_error(&plan_node, "Cube"))
        }
    }
}
