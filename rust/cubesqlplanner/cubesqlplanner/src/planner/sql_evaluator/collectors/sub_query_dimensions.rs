use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::{DimensionSymbol, MemberSymbol, TraversalVisitor};
use crate::planner::{BaseDimension, BaseMember};
use cubenativeutils::CubeError;
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
    }

    fn check_dim_has_measures(&self, dim: &DimensionSymbol) -> bool {
        for dep in dim.get_dependencies().iter() {
            match dep.as_ref() {
                MemberSymbol::Measure(_) => return true,
                _ => {}
            }
        }
        return false;
    }
}

impl TraversalVisitor for SubQueryDimensionsCollector {
    type State = ();
    fn on_node_traverse(
        &mut self,
        node: &Rc<MemberSymbol>,
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
            MemberSymbol::TimeDimension(dim) => {
                self.on_node_traverse(dim.base_symbol(), &())

            }
            _ => Ok(Some(())),
        }
    }
}

pub fn collect_sub_query_dimensions_from_members(
    members: &Vec<Rc<dyn BaseMember>>,
    query_tools: Rc<QueryTools>,
) -> Result<Vec<Rc<BaseDimension>>, CubeError> {
    let mut visitor = SubQueryDimensionsCollector::new();
    for member in members.iter() {
        visitor.apply(&member.member_evaluator(), &())?;
    }
    visitor
        .extract_result()
        .into_iter()
        .map(|s| BaseDimension::try_new_required(s, query_tools.clone()))
        .collect::<Result<Vec<_>, CubeError>>()
}

pub fn collect_sub_query_dimensions(
    node: &Rc<MemberSymbol>,
) -> Result<Vec<Rc<MemberSymbol>>, CubeError> {
    let mut visitor = SubQueryDimensionsCollector::new();
    visitor.apply(node, &())?;
    Ok(visitor.extract_result())
}
