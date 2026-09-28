use crate::logical_plan::visitor::*;
use crate::logical_plan::*;
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::rc::Rc;

struct CubeNamesCollector {
    cube_names: Vec<String>,
}

impl LogicalNodeVisitor for CubeNamesCollector {
    fn process_node(&mut self, node: &PlanNode) -> Result<(), CubeError> {
        if let PlanNode::Cube(cube) = node {
            self.cube_names.push(cube.name().clone());
        }
        Ok(())
    }
}

pub fn collect_cube_names_from_node<T: LogicalNode>(
    node: &Rc<T>,
) -> Result<Vec<String>, CubeError> {
    let mut collector = CubeNamesCollector {
        cube_names: Vec::new(),
    };
    let visitor = LogicalPlanVisitor::new();
    visitor.visit(&mut collector, node)?;
    Ok(collector.cube_names.into_iter().unique().collect_vec())
}
