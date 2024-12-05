use crate::planner::sql_evaluator::{EvaluationNode, MemberSymbolType, TraversalVisitor};
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct HasCumulativeMembersCollector {
    pub has_cumulative_members: bool,
}

impl HasCumulativeMembersCollector {
    pub fn new() -> Self {
        Self {
            has_cumulative_members: false,
        }
    }

    pub fn extract_result(self) -> bool {
        self.has_cumulative_members
    }
}

impl TraversalVisitor for HasCumulativeMembersCollector {
    type State = ();
    fn on_node_traverse(
        &mut self,
        node: &Rc<EvaluationNode>,
        state: &Self::State,
    ) -> Result<Option<Self::State>, CubeError> {
        match node.symbol() {
            MemberSymbolType::Measure(s) => {
                if s.is_rolling_window() {
                    self.has_cumulative_members = true;
                }
            }
            _ => {}
        };
        if self.has_cumulative_members {
            Ok(None)
        } else {
            Ok(Some(()))
        }
    }
}

pub fn has_cumulative_members(node: &Rc<EvaluationNode>) -> Result<bool, CubeError> {
    let mut visitor = HasCumulativeMembersCollector::new();
    visitor.apply(node, &())?;
    Ok(visitor.extract_result())
}
