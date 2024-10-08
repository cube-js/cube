use crate::planner::sql_evaluator::{EvaluationNode, MemberSymbolType, TraversalVisitor};
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct HasPostAggregateMembersCollector {
    pub has_post_aggregate: bool,
}

impl HasPostAggregateMembersCollector {
    pub fn new() -> Self {
        Self {
            has_post_aggregate: false,
        }
    }

    pub fn extract_result(self) -> bool {
        self.has_post_aggregate
    }
}

impl TraversalVisitor for HasPostAggregateMembersCollector {
    fn on_node_traverse(&mut self, node: &Rc<EvaluationNode>) -> Result<bool, CubeError> {
        match node.symbol() {
            MemberSymbolType::Measure(s) => {
                if s.is_post_aggregate() {
                    self.has_post_aggregate = true;
                } else {
                    for filter_node in s.measure_filters() {
                        self.apply(filter_node)?
                    }
                }
            }
            MemberSymbolType::Dimension(s) => {
                if s.is_post_aggregate() {
                    self.has_post_aggregate = true;
                }
            }
            _ => {}
        };
        Ok(!self.has_post_aggregate)
    }
}

pub fn has_post_aggregate_members(node: &Rc<EvaluationNode>) -> Result<bool, CubeError> {
    let mut visitor = HasPostAggregateMembersCollector::new();
    visitor.apply(node)?;
    Ok(visitor.extract_result())
}
