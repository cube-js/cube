use crate::planner::sql_evaluator::{EvaluationNode, MemberSymbolType, TraversalVisitor};
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct MemberChildsCollector {
    pub is_root: bool,
    pub childs: Vec<Rc<EvaluationNode>>,
}

impl MemberChildsCollector {
    pub fn new() -> Self {
        Self {
            is_root: true,
            childs: vec![],
        }
    }

    pub fn extract_result(self) -> Vec<Rc<EvaluationNode>> {
        self.childs
    }
}

impl TraversalVisitor for MemberChildsCollector {
    fn on_node_traverse(&mut self, node: &Rc<EvaluationNode>) -> Result<bool, CubeError> {
        if self.is_root {
            self.is_root = false;
            match node.symbol() {
                MemberSymbolType::Measure(s) => {
                    for filter_node in s.measure_filters() {
                        self.apply(filter_node)?
                    }
                    for order_by in s.measure_order_by() {
                        self.apply(order_by.evaluation_node())?
                    }
                    Ok(true)
                }
                MemberSymbolType::Dimension(_) => Ok(true),
                _ => Ok(false),
            }
        } else {
            match node.symbol() {
                MemberSymbolType::Measure(_) | MemberSymbolType::Dimension(_) => {
                    self.childs.push(node.clone());
                    Ok(false)
                }
                _ => Ok(true),
            }
        }
    }
}

pub fn member_childs(node: &Rc<EvaluationNode>) -> Result<Vec<Rc<EvaluationNode>>, CubeError> {
    let mut visitor = MemberChildsCollector::new();
    visitor.apply(node)?;
    Ok(visitor.extract_result())
}
