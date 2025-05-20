use crate::planner::sql_evaluator::{MemberSymbol, TraversalVisitor};
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
        node: &Rc<MemberSymbol>,
        path: &Vec<String>,
        _: &Self::State,
    ) -> Result<Option<Self::State>, CubeError> {
        match node.as_ref() {
            MemberSymbol::Dimension(e) => {
                if e.owned_by_cube() {
                    if !path.is_empty() {
                        for p in path {
                            self.names.insert(p.clone());
                        }
                    } else {
                        self.names.insert(e.cube_name().clone());
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
                if e.owned_by_cube() {
                    if !path.is_empty() {
                        for p in path {
                            self.names.insert(p.clone());
                        }
                    } else {
                        self.names.insert(e.cube_name().clone());
                    }
                }
            }
            MemberSymbol::CubeName(e) => {
                if !path.is_empty() {
                    for p in path {
                        self.names.insert(p.clone());
                    }
                }
                self.names.insert(e.cube_name().clone());
            }
            MemberSymbol::CubeTable(_) => {}
            MemberSymbol::MemberExpression(_) => {}
        };
        Ok(Some(()))
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
