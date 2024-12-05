use crate::planner::sql_evaluator::{
    EvaluationNode, MemberSymbol, MemberSymbolType, TraversalVisitor,
};
use cubenativeutils::CubeError;
use std::collections::HashSet;
use std::rc::Rc;

pub struct CubeNamesCollector {
    names: HashSet<String>,
}

impl CubeNamesCollector {
    pub fn new() -> Self {
        Self {
            names: HashSet::new(),
        }
    }

    pub fn extract_result(self) -> Vec<String> {
        self.names.into_iter().collect()
    }
}

impl TraversalVisitor for CubeNamesCollector {
    type State = ();
    fn on_node_traverse(
        &mut self,
        node: &Rc<EvaluationNode>,
        state: &Self::State,
    ) -> Result<Option<Self::State>, CubeError> {
        match node.symbol() {
            MemberSymbolType::Dimension(e) => {
                if e.owned_by_cube() {
                    self.names.insert(e.cube_name().clone());
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
                    self.names.insert(e.cube_name().clone());
                }
            }
            MemberSymbolType::CubeName(e) => {
                self.names.insert(e.cube_name().clone());
            }
            MemberSymbolType::CubeTable(e) => {
                self.names.insert(e.cube_name().clone());
            }
            _ => {}
        };
        Ok(Some(()))
    }
}

pub fn collect_cube_names(node: &Rc<EvaluationNode>) -> Result<Vec<String>, CubeError> {
    let mut visitor = CubeNamesCollector::new();
    visitor.apply(node, &())?;
    Ok(visitor.extract_result())
}
