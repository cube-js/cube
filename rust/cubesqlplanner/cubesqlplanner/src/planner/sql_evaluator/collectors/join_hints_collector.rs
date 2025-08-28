use crate::cube_bridge::join_hints::JoinHintItem;
use crate::planner::sql_evaluator::{MemberSymbol, TraversalVisitor};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::rc::Rc;

pub struct JoinHintsCollector {
    hints: Vec<JoinHintItem>,
}

impl JoinHintsCollector {
    pub fn new() -> Self {
        Self { hints: Vec::new() }
    }

    pub fn extract_result(self) -> Vec<JoinHintItem> {
        self.hints.into_iter().unique().collect()
    }
}

impl TraversalVisitor for JoinHintsCollector {
    type State = ();
    fn on_node_traverse(
        &mut self,
        node: &Rc<MemberSymbol>,
        path: &Vec<String>,
        _: &Self::State,
    ) -> Result<Option<Self::State>, CubeError> {
        match node.as_ref() {
            MemberSymbol::Dimension(e) => {
                if !e.is_view() {
                    if !path.is_empty() {
                        if path.len() == 1 {
                            self.hints.push(JoinHintItem::Single(path[0].clone()))
                        } else {
                            self.hints.push(JoinHintItem::Vector(path.clone()))
                        }
                    } else {
                        self.hints.push(JoinHintItem::Single(e.cube_name().clone()));
                    }
                }
                if e.is_sub_query() {
                    return Ok(None);
                }
            }
            MemberSymbol::TimeDimension(e) => {
                return self.on_node_traverse(e.base_symbol(), path, &())
            }
            MemberSymbol::Measure(e) => {
                if !e.is_view() {
                    if !path.is_empty() {
                        if path.len() == 1 {
                            self.hints.push(JoinHintItem::Single(path[0].clone()))
                        } else {
                            self.hints.push(JoinHintItem::Vector(path.clone()))
                        }
                    } else {
                        self.hints.push(JoinHintItem::Single(e.cube_name().clone()));
                    }
                }
            }
            MemberSymbol::CubeName(e) => {
                if !path.is_empty() {
                    let mut path = path.clone();
                    path.push(e.cube_name().clone());
                    self.hints.push(JoinHintItem::Vector(path));
                } else {
                    self.hints.push(JoinHintItem::Single(e.cube_name().clone()));
                }
            }
            MemberSymbol::CubeTable(_) => {}
            MemberSymbol::MemberExpression(_) => {}
        };
        Ok(Some(()))
    }
}

pub fn collect_join_hints(node: &Rc<MemberSymbol>) -> Result<Vec<JoinHintItem>, CubeError> {
    let mut visitor = JoinHintsCollector::new();
    visitor.apply(node, &())?;
    let res = visitor.extract_result();
    Ok(res)
}

pub fn collect_join_hints_for_measures(
    measures: &Vec<Rc<MemberSymbol>>,
) -> Result<Vec<JoinHintItem>, CubeError> {
    let mut visitor = JoinHintsCollector::new();
    for meas in measures.iter() {
        visitor.apply(&meas, &())?;
    }
    let res = visitor.extract_result();
    Ok(res)
}
