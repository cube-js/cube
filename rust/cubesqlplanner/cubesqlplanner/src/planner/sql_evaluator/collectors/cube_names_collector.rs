use crate::cube_bridge::join_hints::JoinHintItem;
use crate::planner::sql_evaluator::{CubeRef, MemberSymbol, TraversalVisitor};
use cubenativeutils::CubeError;
use std::collections::HashSet;
use std::rc::Rc;

pub struct CubeNamesCollector {
    names: HashSet<String>,
    collect_hints: bool,
    hints: Vec<JoinHintItem>,
}

impl CubeNamesCollector {
    pub fn new() -> Self {
        Self {
            names: HashSet::new(),
            collect_hints: false,
            hints: Vec::new(),
        }
    }

    pub fn with_hints() -> Self {
        Self {
            names: HashSet::new(),
            collect_hints: true,
            hints: Vec::new(),
        }
    }

    fn add_from_path(&mut self, path: &[String], cube_name: &str) {
        if !path.is_empty() {
            for p in path {
                self.names.insert(p.clone());
            }
            if self.collect_hints {
                if path.len() == 1 {
                    self.hints.push(JoinHintItem::Single(path[0].clone()));
                } else {
                    self.hints.push(JoinHintItem::Vector(path.to_vec()));
                }
            }
        } else {
            self.names.insert(cube_name.to_string());
            if self.collect_hints {
                self.hints.push(JoinHintItem::Single(cube_name.to_string()));
            }
        }
    }

    pub fn extract_result(self) -> Vec<String> {
        self.names.into_iter().collect()
    }

    pub fn extract_hints(self) -> Vec<JoinHintItem> {
        self.hints
    }
}

impl TraversalVisitor for CubeNamesCollector {
    type State = ();
    fn on_node_traverse(
        &mut self,
        node: &Rc<MemberSymbol>,
        _: &Self::State,
    ) -> Result<Option<Self::State>, CubeError> {
        match node.as_ref() {
            MemberSymbol::Dimension(e) => {
                if !e.is_view() {
                    self.add_from_path(&node.path(), &e.cube_name());
                }
                if e.is_sub_query() {
                    return Ok(None);
                }
            }
            MemberSymbol::TimeDimension(e) => return self.on_node_traverse(e.base_symbol(), &()),
            MemberSymbol::Measure(e) => {
                if !e.is_view() {
                    self.add_from_path(&node.path(), &e.cube_name());
                }
            }
            MemberSymbol::MemberExpression(_) => {}
        };
        Ok(Some(()))
    }

    fn on_cube_ref(&mut self, cube_ref: &CubeRef, _state: &Self::State) -> Result<(), CubeError> {
        if let CubeRef::Name(symbol) = cube_ref {
            let path = symbol.path();
            for p in path {
                self.names.insert(p.clone());
            }
            if self.collect_hints {
                if path.len() > 1 {
                    self.hints.push(JoinHintItem::Vector(path.clone()));
                } else {
                    self.hints
                        .push(JoinHintItem::Single(symbol.cube_name().clone()));
                }
            }
        }
        Ok(())
    }
}

pub fn collect_cube_names(node: &Rc<MemberSymbol>) -> Result<Vec<String>, CubeError> {
    let mut visitor = CubeNamesCollector::new();
    visitor.apply(node, &())?;
    Ok(visitor.extract_result())
}

pub fn collect_cube_names_from_symbols(
    nodes: &Vec<Rc<MemberSymbol>>,
) -> Result<Vec<String>, CubeError> {
    let mut visitor = CubeNamesCollector::new();
    for node in nodes {
        visitor.apply(node, &())?;
    }
    Ok(visitor.extract_result())
}

pub fn collect_cube_join_hint_items_from_symbols(
    nodes: &Vec<Rc<MemberSymbol>>,
) -> Result<Vec<JoinHintItem>, CubeError> {
    let mut visitor = CubeNamesCollector::with_hints();
    for node in nodes {
        visitor.apply(node, &())?;
    }
    Ok(visitor.extract_hints())
}
