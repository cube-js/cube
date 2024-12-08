use crate::planner::sql_evaluator::{EvaluationNode, MemberSymbolType, TraversalVisitor};
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct HasMultiStageMembersCollector {
    pub ignore_cumulative: bool,
    pub has_multi_stage: bool,
}

impl HasMultiStageMembersCollector {
    pub fn new(ignore_cumulative: bool) -> Self {
        Self {
            ignore_cumulative,
            has_multi_stage: false,
        }
    }

    pub fn extract_result(self) -> bool {
        self.has_multi_stage
    }
}

impl TraversalVisitor for HasMultiStageMembersCollector {
    fn on_node_traverse(&mut self, node: &Rc<EvaluationNode>) -> Result<bool, CubeError> {
        match node.symbol() {
            MemberSymbolType::Measure(s) => {
                if s.is_multi_stage() {
                    self.has_multi_stage = true;
                } else if !self.ignore_cumulative
                    && (s.is_rolling_window() || s.measure_type() == "runningTotal")
                {
                    self.has_multi_stage = true;
                } else {
                    for filter_node in s.measure_filters() {
                        self.apply(filter_node)?
                    }
                    for order_by in s.measure_order_by() {
                        self.apply(order_by.evaluation_node())?
                    }
                }
            }
            MemberSymbolType::Dimension(s) => {
                if s.is_multi_stage() {
                    self.has_multi_stage = true;
                }
            }
            _ => {}
        };
        Ok(!self.has_multi_stage)
    }
}

pub fn has_multi_stage_members(
    node: &Rc<EvaluationNode>,
    ignore_cumulative: bool,
) -> Result<bool, CubeError> {
    let mut visitor = HasMultiStageMembersCollector::new(ignore_cumulative);
    visitor.apply(node)?;
    Ok(visitor.extract_result())
}
