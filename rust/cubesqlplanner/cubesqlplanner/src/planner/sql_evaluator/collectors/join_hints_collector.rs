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
        if node.is_multi_stage() {
            //We don't add multi-stage members childs to join hints
            return Ok(None);
        }

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
    let mut collected_hints = visitor.extract_result();

    let join_map = match node.as_ref() {
        MemberSymbol::Dimension(d) => d.join_map(),
        MemberSymbol::TimeDimension(d) => d.join_map(),
        MemberSymbol::Measure(m) => m.join_map(),
        _ => &None,
    };

    if let Some(join_map) = join_map {
        for hint in collected_hints.iter_mut() {
            match hint {
                // If hints array has single element, check if it can be enriched with join hints
                JoinHintItem::Single(hints) => {
                    for path in join_map.iter() {
                        if let Some(hint_index) = path.iter().position(|p| p == hints) {
                            *hint = JoinHintItem::Vector(path[0..=hint_index].to_vec());
                            break;
                        }
                    }
                }
                // If hints is an array with multiple elements, it means it already
                // includes full join hint path
                JoinHintItem::Vector(_) => {}
            }
        }
    }

    Ok(collected_hints)
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
