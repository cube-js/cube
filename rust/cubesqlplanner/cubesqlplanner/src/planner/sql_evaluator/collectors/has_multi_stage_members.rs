use crate::planner::sql_evaluator::{MemberSymbol, TraversalVisitor};
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
    type State = ();
    fn on_node_traverse(
        &mut self,
        node: &Rc<MemberSymbol>,
        _path: &Vec<String>,
        _: &Self::State,
    ) -> Result<Option<Self::State>, CubeError> {
        match node.as_ref() {
            MemberSymbol::Measure(s) => {
                if s.is_multi_stage() {
                    self.has_multi_stage = true;
                } else if !self.ignore_cumulative
                    && (s.is_rolling_window() || s.measure_type() == "runningTotal")
                {
                    self.has_multi_stage = true;
                }
            }
            MemberSymbol::Dimension(s) => {
                if s.is_multi_stage() {
                    self.has_multi_stage = true;
                }
            }
            _ => {}
        };
        if self.has_multi_stage {
            Ok(None)
        } else {
            Ok(Some(()))
        }
    }
}

pub fn has_multi_stage_members(
    node: &Rc<MemberSymbol>,
    ignore_cumulative: bool,
) -> Result<bool, CubeError> {
    let mut visitor = HasMultiStageMembersCollector::new(ignore_cumulative);
    visitor.apply(node, &())?;
    Ok(visitor.extract_result())
}
