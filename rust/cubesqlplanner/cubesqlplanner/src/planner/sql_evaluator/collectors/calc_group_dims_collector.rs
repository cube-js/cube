use crate::planner::sql_evaluator::{MemberSymbol, TraversalVisitor};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::rc::Rc;

pub struct CalcGroupDimsCollector {
    calc_groups: Vec<Rc<MemberSymbol>>,
}

impl CalcGroupDimsCollector {
    pub fn new() -> Self {
        Self {
            calc_groups: Vec::new(),
        }
    }

    pub fn extract_result(self) -> Vec<Rc<MemberSymbol>> {
        self.calc_groups
            .into_iter()
            .unique_by(|dim| dim.full_name())
            .collect()
    }
}

impl TraversalVisitor for CalcGroupDimsCollector {
    type State = ();
    fn on_node_traverse(
        &mut self,
        node: &Rc<MemberSymbol>,
        path: &Vec<String>,
        _: &Self::State,
    ) -> Result<Option<Self::State>, CubeError> {
        match node.as_ref() {
            MemberSymbol::Dimension(e) => {
                if e.is_calc_group() {
                    self.calc_groups.push(node.clone());
                    return Ok(None);
                }
            }
            MemberSymbol::TimeDimension(e) => {
                return self.on_node_traverse(e.base_symbol(), path, &())
            }
            MemberSymbol::Measure(_) => {}
            MemberSymbol::CubeName(_) => {}
            MemberSymbol::CubeTable(_) => {}
            MemberSymbol::MemberExpression(_) => {}
        };
        Ok(Some(()))
    }
}

pub fn collect_calc_group_dims(
    node: &Rc<MemberSymbol>,
) -> Result<Vec<Rc<MemberSymbol>>, CubeError> {
    let mut visitor = CalcGroupDimsCollector::new();
    visitor.apply(node, &())?;
    let res = visitor.extract_result();
    Ok(res)
}

pub fn collect_calc_group_dims_from_nodes<'a, T>(
    nodes: T,
) -> Result<Vec<Rc<MemberSymbol>>, CubeError>
where
    T: Iterator<Item = &'a Rc<MemberSymbol>>,
{
    let mut visitor = CalcGroupDimsCollector::new();
    for node in nodes {
        visitor.apply(node, &())?;
    }
    let res = visitor
        .extract_result()
        .into_iter()
        .unique_by(|s| s.full_name())
        .collect();
    Ok(res)
}
