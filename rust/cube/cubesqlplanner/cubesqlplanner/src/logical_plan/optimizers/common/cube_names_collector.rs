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

/// `LogicalPlan` is not part of `PlanNode`, so the generic walker can't
/// descend through it. Recurse explicitly through `ctes` and the `root`
/// PlanNode subtree.
pub fn collect_cube_names_from_plan(plan: &Rc<LogicalPlan>) -> Result<Vec<String>, CubeError> {
    let mut collector = CubeNamesCollector {
        cube_names: Vec::new(),
    };
    walk_plan(&mut collector, plan)?;
    Ok(collector.cube_names.into_iter().unique().collect_vec())
}

fn walk_plan(collector: &mut CubeNamesCollector, plan: &Rc<LogicalPlan>) -> Result<(), CubeError> {
    let visitor = LogicalPlanVisitor::new();
    for cte in plan.ctes() {
        match &cte.body {
            MultiStageMemberBody::Plan(nested) => walk_plan(collector, nested)?,
            MultiStageMemberBody::TimeSeries(ts) => visitor.visit(collector, ts)?,
            MultiStageMemberBody::RollingWindow(rw) => visitor.visit(collector, rw)?,
        }
    }
    visitor.visit(collector, plan.root())?;
    Ok(())
}
