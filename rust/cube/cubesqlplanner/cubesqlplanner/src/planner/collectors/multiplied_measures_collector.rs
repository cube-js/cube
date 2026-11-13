use crate::planner::{CubeId, JoinTree, MemberId, MemberSymbol};
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

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
    /// The measures the leaf is reached through, outermost first.
    pub path: Vec<Rc<MemberSymbol>>,
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

/// The parts of a measure aggregated on their own: a measure reading, at any
/// depth, measures of another cube is split into its parts, anything else is a
/// leaf itself.
pub fn collect_measure_leaves(node: &Rc<MemberSymbol>) -> Result<Vec<MeasureLeaf>, CubeError> {
    if let Ok(member_expression) = node.as_member_expression() {
        if let Some(cube_names) = member_expression.cube_names_if_dimension_only_expression()? {
            return Ok(vec![MeasureLeaf {
                measure: node.clone(),
                cubes: LeafCubes::DimensionOnly(cube_names),
                path: vec![],
            }]);
        }
    }
    let mut collector = MeasureLeavesCollector {
        composite: HashMap::new(),
        path: Vec::new(),
        leaves: Vec::new(),
    };
    collector.collect(node)?;
    Ok(collector.leaves)
}

struct MeasureLeavesCollector {
    composite: HashMap<MemberId, bool>,
    path: Vec<Rc<MemberSymbol>>,
    leaves: Vec<MeasureLeaf>,
}

impl MeasureLeavesCollector {
    fn collect(&mut self, node: &Rc<MemberSymbol>) -> Result<(), CubeError> {
        match node.as_ref() {
            MemberSymbol::Measure(_) => {
                if !self.is_composite(node)? {
                    self.leaves.push(MeasureLeaf {
                        measure: node.clone(),
                        cubes: LeafCubes::Own(node.cube_id()),
                        path: self.path.clone(),
                    });
                    return Ok(());
                }
                self.path.push(node.clone());
                for dep in node.get_dependencies() {
                    self.collect(&dep)?;
                }
                self.path.pop();
            }
            MemberSymbol::MemberExpression(_) | MemberSymbol::Ref(_) => {
                for dep in node.get_dependencies() {
                    self.collect(&dep)?;
                }
            }
            MemberSymbol::Dimension(_) | MemberSymbol::TimeDimension(_) => {}
        }
        Ok(())
    }

    // Whether the measure reads, at any depth, a measure of another cube.
    fn is_composite(&mut self, measure: &Rc<MemberSymbol>) -> Result<bool, CubeError> {
        if let Some(composite) = self.composite.get(measure.id()) {
            return Ok(*composite);
        }
        let mut composite = false;
        for child in nearest_measures(measure) {
            if child.cube_id() != measure.cube_id() || self.is_composite(&child)? {
                composite = true;
                break;
            }
        }
        self.composite.insert(measure.id().clone(), composite);
        Ok(composite)
    }
}

// The measures `node` reads directly, through member expressions and references.
fn nearest_measures(node: &Rc<MemberSymbol>) -> Vec<Rc<MemberSymbol>> {
    let mut result = Vec::new();
    for dep in node.get_dependencies() {
        match dep.as_ref() {
            MemberSymbol::Measure(_) => result.push(dep),
            MemberSymbol::MemberExpression(_) | MemberSymbol::Ref(_) => {
                result.extend(nearest_measures(&dep))
            }
            MemberSymbol::Dimension(_) | MemberSymbol::TimeDimension(_) => {}
        }
    }
    result
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
