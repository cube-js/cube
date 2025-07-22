use crate::cube_bridge::join_definition::JoinDefinition;
use crate::planner::planners::JoinPlanner;
use crate::planner::sql_evaluator::{DimensionSymbol, MemberSymbol, TraversalVisitor};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::rc::Rc;

pub struct SubQueryDimensionsCollector {
    pub sub_query_dimensions: Vec<Rc<MemberSymbol>>,
}

impl SubQueryDimensionsCollector {
    pub fn new() -> Self {
        Self {
            sub_query_dimensions: vec![],
        }
    }

    pub fn extract_result(self) -> Vec<Rc<MemberSymbol>> {
        self.sub_query_dimensions
            .into_iter()
            .unique_by(|m| m.full_name())
            .collect()
    }

    fn check_dim_has_measures(&self, dim: &DimensionSymbol) -> bool {
        for dep in dim.get_dependencies().iter() {
            if let MemberSymbol::Measure(_) = dep.as_ref() {
                return true;
            }
        }
        false
    }
}

impl TraversalVisitor for SubQueryDimensionsCollector {
    type State = ();
    fn on_node_traverse(
        &mut self,
        node: &Rc<MemberSymbol>,
        path: &Vec<String>,
        _state: &Self::State,
    ) -> Result<Option<Self::State>, CubeError> {
        match node.as_ref() {
            MemberSymbol::Dimension(dim) => {
                if dim.is_sub_query() {
                    if !self.check_dim_has_measures(dim) {
                        return Err(CubeError::user(format!(
                            "Subquery dimension {} should reference at least one measure",
                            dim.full_name()
                        )));
                    }
                    self.sub_query_dimensions.push(node.clone());
                }
                Ok(Some(()))
            }
            MemberSymbol::TimeDimension(dim) => self.on_node_traverse(dim.base_symbol(), path, &()),
            _ => Ok(Some(())),
        }
    }
}

pub fn collect_sub_query_dimensions_from_members(
    members: &Vec<Rc<MemberSymbol>>,
    join_planner: &JoinPlanner,
    join: &Rc<dyn JoinDefinition>,
) -> Result<Vec<Rc<MemberSymbol>>, CubeError> {
    collect_sub_query_dimensions_from_symbols(&members, join_planner, join)
}

pub fn collect_sub_query_dimensions_from_symbols(
    members: &Vec<Rc<MemberSymbol>>,
    join_planner: &JoinPlanner,
    join: &Rc<dyn JoinDefinition>,
) -> Result<Vec<Rc<MemberSymbol>>, CubeError> {
    let mut visitor = SubQueryDimensionsCollector::new();
    for member in members.iter() {
        visitor.apply(&member, &())?;
    }
    for join_item in join.joins()? {
        let condition = join_planner.compile_join_condition(join_item.clone())?;
        for dep in condition.get_dependencies() {
            visitor.apply(&dep, &())?;
        }
    }
    Ok(visitor.extract_result())
}

pub fn collect_sub_query_dimensions(
    node: &Rc<MemberSymbol>,
) -> Result<Vec<Rc<MemberSymbol>>, CubeError> {
    let mut visitor = SubQueryDimensionsCollector::new();
    visitor.apply(node, &())?;
    Ok(visitor.extract_result())
}
