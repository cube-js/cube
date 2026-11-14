use crate::planner::{MemberId, MemberSymbol, TraversalVisitor};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::HashSet;
use std::rc::Rc;

pub struct CalcGroupDimsCollector {
    calc_groups: Vec<Rc<MemberSymbol>>,
    // Members read as they are, by the id of their reference chain: the calc
    // groups they depend on are not read.
    provided: HashSet<MemberId>,
}

impl CalcGroupDimsCollector {
    pub fn new() -> Self {
        Self {
            calc_groups: Vec::new(),
            provided: HashSet::new(),
        }
    }

    pub fn extract_result(self) -> Vec<Rc<MemberSymbol>> {
        self.calc_groups
            .into_iter()
            .unique_by(|dim| dim.id().clone())
            .collect()
    }
}

impl TraversalVisitor for CalcGroupDimsCollector {
    type State = ();
    fn on_node_traverse(
        &mut self,
        node: &Rc<MemberSymbol>,
        _: &Self::State,
    ) -> Result<Option<Self::State>, CubeError> {
        if !self.provided.is_empty()
            && self
                .provided
                .contains(node.clone().resolve_reference_chain().id())
        {
            return Ok(None);
        }
        match node.as_ref() {
            MemberSymbol::Dimension(e) => {
                if e.is_calc_group() {
                    self.calc_groups.push(node.clone());
                    return Ok(None);
                }
            }
            MemberSymbol::TimeDimension(e) => return self.on_node_traverse(e.base_symbol(), &()),
            MemberSymbol::Measure(_) => {}
            MemberSymbol::MemberExpression(_) | MemberSymbol::Ref(_) => {}
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
        .unique_by(|s| s.id().clone())
        .collect();
    Ok(res)
}

/// The calc groups `nodes` read, except through the `provided` members, given
/// by the ids of their reference chains.
pub fn collect_calc_group_dims_outside<'a, T>(
    nodes: T,
    provided: HashSet<MemberId>,
) -> Result<Vec<Rc<MemberSymbol>>, CubeError>
where
    T: Iterator<Item = &'a Rc<MemberSymbol>>,
{
    let mut visitor = CalcGroupDimsCollector {
        calc_groups: Vec::new(),
        provided,
    };
    for node in nodes {
        visitor.apply(node, &())?;
    }
    Ok(visitor.extract_result())
}
