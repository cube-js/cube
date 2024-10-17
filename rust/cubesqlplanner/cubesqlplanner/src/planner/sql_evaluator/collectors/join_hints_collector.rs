use crate::planner::sql_evaluator::{
    EvaluationNode, MemberSymbol, MemberSymbolType, TraversalVisitor,
};
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
    fn on_node_traverse(&mut self, node: &Rc<EvaluationNode>) -> Result<bool, CubeError> {
        let res = match node.symbol() {
            MemberSymbolType::Dimension(e) => {
                if e.owned_by_cube() {
                    self.hints.insert(e.cube_name().clone());
                }
                true
            }
            MemberSymbolType::Measure(e) => {
                if e.owned_by_cube() {
                    self.hints.insert(e.cube_name().clone());
                }
                true
            }
            MemberSymbolType::CubeName(e) => {
                self.hints.insert(e.cube_name().clone());
                true
            }
            MemberSymbolType::CubeTable(e) => {
                self.hints.insert(e.cube_name().clone());
                true
            }
            _ => false,
        };
        Ok(res)
    }
}
