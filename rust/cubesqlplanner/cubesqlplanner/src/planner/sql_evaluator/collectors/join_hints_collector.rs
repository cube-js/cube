use crate::planner::sql_evaluator::{
    EvaluationNode, MemberSymbol, MemberSymbolType, TraversalVisitor,
};
use crate::planner::BaseMeasure;
use cubenativeutils::CubeError;
use std::collections::HashSet;
use std::rc::Rc;

pub struct JoinHintsCollector {
    hints: Vec<String>,
}

impl JoinHintsCollector {
    pub fn new() -> Self {
        Self { hints: Vec::new() }
    }

    pub fn extract_result(self) -> Vec<String> {
        self.hints
    }
}

impl TraversalVisitor for JoinHintsCollector {
    type State = ();
    fn on_node_traverse(
        &mut self,
        node: &Rc<EvaluationNode>,
        state: &Self::State,
    ) -> Result<Option<Self::State>, CubeError> {
        match node.symbol() {
            MemberSymbolType::Dimension(e) => {
                if e.owned_by_cube() {
                    self.hints.push(e.cube_name().clone());
                }
            }
            MemberSymbolType::Measure(e) => {
                for filter_node in e.measure_filters() {
                    self.apply(filter_node, &())?
                }
                for order_by in e.measure_order_by() {
                    self.apply(order_by.evaluation_node(), &())?
                }
                if e.owned_by_cube() {
                    self.hints.push(e.cube_name().clone());
                }
            }
            MemberSymbolType::CubeName(e) => {
                self.hints.push(e.cube_name().clone());
            }
            MemberSymbolType::CubeTable(e) => {
                self.hints.push(e.cube_name().clone());
            }
            _ => {}
        };
        Ok(Some(()))
    }
}

pub fn collect_join_hints(node: &Rc<EvaluationNode>) -> Result<Vec<String>, CubeError> {
    let mut visitor = JoinHintsCollector::new();
    visitor.apply(node, &())?;
    Ok(visitor.extract_result())
}

pub fn collect_join_hints_for_measures(
    measures: &Vec<Rc<BaseMeasure>>,
) -> Result<Vec<String>, CubeError> {
    let mut visitor = JoinHintsCollector::new();
    for meas in measures.iter() {
        visitor.apply(&meas.member_evaluator(), &())?;
    }
    Ok(visitor.extract_result())
}
