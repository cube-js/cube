use crate::planner::{CubeId, JoinTree, MemberId, MemberSymbol, TraversalVisitor};
use cubenativeutils::CubeError;
use std::collections::HashSet;
use std::rc::Rc;

struct CompositeMeasuresCollector {
    composite_measures: HashSet<MemberId>,
}

#[derive(Clone)]
struct CompositeMeasureCollectorState {
    pub parent_measure: Option<Rc<MemberSymbol>>,
}

impl CompositeMeasureCollectorState {
    pub fn new(parent_measure: Option<Rc<MemberSymbol>>) -> Self {
        Self { parent_measure }
    }
}

impl CompositeMeasuresCollector {
    pub fn new() -> Self {
        Self {
            composite_measures: HashSet::new(),
        }
    }

    pub fn extract_result(self) -> HashSet<MemberId> {
        self.composite_measures
    }
}

impl TraversalVisitor for CompositeMeasuresCollector {
    type State = CompositeMeasureCollectorState;
    fn on_node_traverse(
        &mut self,
        node: &Rc<MemberSymbol>,
        state: &Self::State,
    ) -> Result<Option<Self::State>, CubeError> {
        let res = match node.as_ref() {
            MemberSymbol::Measure(_) => {
                if let Some(parent) = &state.parent_measure {
                    if parent.cube_id() != node.cube_id() {
                        self.composite_measures.insert(parent.id().clone());
                    }
                }

                let new_state = CompositeMeasureCollectorState::new(Some(node.clone()));
                Some(new_state)
            }
            MemberSymbol::Dimension(_) => None,
            MemberSymbol::MemberExpression(_) | MemberSymbol::Ref(_) => Some(state.clone()),
            MemberSymbol::TimeDimension(_) => None,
        };
        Ok(res)
    }
}

#[derive(Debug)]
pub struct MeasureResult {
    pub multiplied: bool,
    pub measure: Rc<MemberSymbol>,
    pub cube_id: CubeId,
}

/// A part of a measure aggregated on its own, found without a join.
#[derive(Clone, Debug)]
pub struct MeasureLeaf {
    pub measure: Rc<MemberSymbol>,
    pub cubes: LeafCubes,
}

#[derive(Clone, Debug)]
pub enum LeafCubes {
    /// A measure aggregated over the rows of its own cube.
    Own(CubeId),
    /// An expression over dimensions only, of these cubes.
    DimensionOnly(Vec<CubeId>),
}

impl MeasureLeaf {
    /// The leaf under `join`: the cube it aggregates over and whether `join`
    /// multiplies that cube's rows.
    pub fn resolve(&self, join: &JoinTree) -> Result<MeasureResult, CubeError> {
        let (cube_id, multiplied) = match &self.cubes {
            LeafCubes::Own(cube_id) => (cube_id.clone(), join.is_multiplied(cube_id)),
            LeafCubes::DimensionOnly(cube_ids) => match cube_ids.as_slice() {
                [] => (self.measure.cube_id().clone(), false),
                [cube_id] => (cube_id.clone(), join.is_multiplied(cube_id)),
                _ => {
                    if cube_ids.iter().any(|cube_id| join.is_multiplied(cube_id)) {
                        return Err(CubeError::user(format!(
                            "Dimension-only measure {} references cubes {:?} that lead to row multiplication. Please rewrite it using sub query.",
                            self.measure.full_name(),
                            cube_ids
                        )));
                    }
                    // Dimensions from several cubes, but none of them is on the
                    // multiplied side of a join - safe to evaluate the expression
                    // on top of the join tree as a regular measure.
                    (self.measure.cube_id().clone(), false)
                }
            },
        };
        Ok(MeasureResult {
            multiplied,
            measure: self.measure.clone(),
            cube_id,
        })
    }
}

struct MeasureLeavesCollector {
    composite_measures: HashSet<MemberId>,
    leaves: Vec<MeasureLeaf>,
}

impl TraversalVisitor for MeasureLeavesCollector {
    type State = ();
    fn on_node_traverse(
        &mut self,
        node: &Rc<MemberSymbol>,
        _: &Self::State,
    ) -> Result<Option<Self::State>, CubeError> {
        let res = match node.as_ref() {
            MemberSymbol::Measure(e) => {
                if self.composite_measures.contains(e.id()) {
                    Some(())
                } else {
                    self.leaves.push(MeasureLeaf {
                        measure: node.clone(),
                        cubes: LeafCubes::Own(node.cube_id()),
                    });
                    None
                }
            }
            MemberSymbol::MemberExpression(_) | MemberSymbol::Ref(_) => Some(()),
            MemberSymbol::Dimension(_) => None,
            MemberSymbol::TimeDimension(_) => None,
        };
        Ok(res)
    }
}

/// The parts of a measure aggregated on their own: a measure reading another
/// cube's measures is split into those, anything else is a leaf itself.
pub fn collect_measure_leaves(node: &Rc<MemberSymbol>) -> Result<Vec<MeasureLeaf>, CubeError> {
    if let Ok(member_expression) = node.as_member_expression() {
        if let Some(cube_names) = member_expression.cube_names_if_dimension_only_expression()? {
            return Ok(vec![MeasureLeaf {
                measure: node.clone(),
                cubes: LeafCubes::DimensionOnly(cube_names),
            }]);
        }
    }

    let mut composite_collector = CompositeMeasuresCollector::new();
    composite_collector.apply(node, &CompositeMeasureCollectorState::new(None))?;
    let mut visitor = MeasureLeavesCollector {
        composite_measures: composite_collector.extract_result(),
        leaves: Vec::new(),
    };
    visitor.apply(node, &())?;
    Ok(visitor.leaves)
}

pub fn collect_multiplied_measures(
    node: &Rc<MemberSymbol>,
    join: &Rc<JoinTree>,
) -> Result<Vec<MeasureResult>, CubeError> {
    collect_measure_leaves(node)?
        .iter()
        .map(|leaf| leaf.resolve(join))
        .collect()
}
