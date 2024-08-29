use super::EvaluationNode;
use super::MemberEvaluator;
use super::TraversalVisitor;
use super::{CubeNameEvaluator, DimensionEvaluator, MeasureEvaluator, MemberEvaluatorType};
use crate::planner::query_tools::QueryTools;
use cubenativeutils::CubeError;
use std::collections::HashSet;
use std::rc::Rc;

pub struct JoinHintsCollector {
    hints: HashSet<String>,
}

impl JoinHintsCollector {
    pub fn new() -> Self {
        Self {
            hints: HashSet::new(),
        }
    }

    pub fn extract_result(self) -> Vec<String> {
        self.hints.into_iter().collect()
    }
}

impl TraversalVisitor for JoinHintsCollector {
    fn on_node_traverse(&mut self, node: &Rc<EvaluationNode>) -> Result<(), CubeError> {
        match node.evaluator() {
            MemberEvaluatorType::Dimension(e) => self.hints.insert(e.cube_name().clone()),
            MemberEvaluatorType::Measure(e) => self.hints.insert(e.cube_name().clone()),
            MemberEvaluatorType::CubeName(e) => self.hints.insert(e.cube_name().clone()),
        };
        Ok(())
    }
}
