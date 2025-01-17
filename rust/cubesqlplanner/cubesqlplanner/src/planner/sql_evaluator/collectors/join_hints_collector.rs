use crate::planner::sql_evaluator::{MemberSymbol, TraversalVisitor};
use crate::planner::BaseMeasure;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct JoinHintsCollector {
    hints: Vec<String>,
}

impl JoinHintsCollector {
    pub fn new() -> Self {
        Self { hints: Vec::new() }
    }

    pub fn extract_result(self) -> Vec<String> {
        self.hints
    }
}

impl TraversalVisitor for JoinHintsCollector {
    type State = ();
    fn on_node_traverse(
        &mut self,
        node: &Rc<MemberSymbol>,
        _: &Self::State,
    ) -> Result<Option<Self::State>, CubeError> {
        match node.as_ref() {
            MemberSymbol::Dimension(e) => {
                if e.owned_by_cube() {
                    self.hints.push(e.cube_name().clone());
                }
                for name in e.get_dependent_cubes().into_iter() {
                    self.hints.push(name);
                }
            }
            MemberSymbol::Measure(e) => {
                if e.owned_by_cube() {
                    self.hints.push(e.cube_name().clone());
                }
                for name in e.get_dependent_cubes().into_iter() {
                    self.hints.push(name);
                }
            }
            MemberSymbol::CubeName(e) => {
                self.hints.push(e.cube_name().clone());
            }
            MemberSymbol::CubeTable(e) => {
                self.hints.push(e.cube_name().clone());
            }
        };
        Ok(Some(()))
    }
}

pub fn collect_join_hints(node: &Rc<MemberSymbol>) -> Result<Vec<String>, CubeError> {
    let mut visitor = JoinHintsCollector::new();
    visitor.apply(node, &())?;
    Ok(visitor.extract_result())
}

pub fn collect_join_hints_for_measures(
    measures: &Vec<Rc<BaseMeasure>>,
) -> Result<Vec<String>, CubeError> {
    let mut visitor = JoinHintsCollector::new();
    for meas in measures.iter() {
        visitor.apply(&meas.member_evaluator(), &())?;
    }
    Ok(visitor.extract_result())
}
