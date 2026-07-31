use super::cube_names_collector::collect_cube_names;
use crate::planner::{JoinTree, MemberSymbol, TraversalVisitor};
use cubenativeutils::CubeError;
use std::collections::HashSet;
use std::rc::Rc;

struct CompositeMeasuresCollector {
    composite_measures: HashSet<String>,
    join: Rc<JoinTree>,
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
    pub fn new(join: Rc<JoinTree>) -> Self {
        Self {
            composite_measures: HashSet::new(),
            join,
        }
    }

    pub fn extract_result(self) -> HashSet<String> {
        self.composite_measures
    }

    /// True when only component measures stand between `node` and whatever it
    /// reaches. Those components can be computed separately and the expression
    /// rebuilt on top of them; anything else - a dimension, a raw cube
    /// reference - has to be evaluated where the node itself is evaluated, and
    /// there is nowhere to read it from once the components have moved out.
    fn travels_only_through_measures(node: &Rc<MemberSymbol>) -> bool {
        if !node.get_cube_refs().is_empty() {
            return false;
        }
        let dependencies = node.get_dependencies();
        !dependencies.is_empty() && dependencies.iter().all(|dep| dep.as_measure().is_ok())
    }

    /// True when evaluating `node` needs a cube other than the one it is
    /// defined on - the condition under which the planner builds a measure-join
    /// subquery rather than reading the key cube directly.
    fn reaches_other_cube(node: &Rc<MemberSymbol>) -> Result<bool, CubeError> {
        let own_cube = node.cube_name();
        Ok(collect_cube_names(node)?
            .iter()
            .any(|cube_name| cube_name != &own_cube))
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
            MemberSymbol::Measure(measure) => {
                if let Some(parent) = &state.parent_measure {
                    if parent.cube_name() != node.cube_name() {
                        self.composite_measures.insert(parent.full_name());
                    }
                }

                // A calculated measure carries no aggregate of its own, so it
                // cannot be re-aggregated on top of the ungrouped measure-join
                // subquery. Treat it as composite so its components travel
                // instead, each on the join tree its own definition asks for.
                // Only where that subquery is actually built: the measure has
                // to be multiplied by this join, and to reach past its own cube.
                // Anywhere else it is read straight off a leaf-measure query
                // that keeps its aggregate, and splitting it would only move
                // work around.
                let owned = node.with_stripped_join_prefix();
                if measure.is_calculated()
                    && self.join.is_multiplied(&owned.cube_name())
                    && Self::travels_only_through_measures(&owned)
                    && Self::reaches_other_cube(&owned)?
                {
                    self.composite_measures.insert(node.full_name());
                }

                let new_state = CompositeMeasureCollectorState::new(Some(node.clone()));
                Some(new_state)
            }
            MemberSymbol::Dimension(_) => None,
            MemberSymbol::MemberExpression(_) => Some(state.clone()),
            _ => None,
        };
        Ok(res)
    }
}

#[derive(Debug)]
pub struct MeasureResult {
    pub multiplied: bool,
    pub measure: Rc<MemberSymbol>,
    pub cube_name: String,
}

pub struct MultipliedMeasuresCollector {
    composite_measures: HashSet<String>,
    colllected_measures: Vec<MeasureResult>,
    join: Rc<JoinTree>,
}

impl MultipliedMeasuresCollector {
    pub fn new(composite_measures: HashSet<String>, join: Rc<JoinTree>) -> Self {
        Self {
            composite_measures,
            join,
            colllected_measures: vec![],
        }
    }

    pub fn extract_result(self) -> Vec<MeasureResult> {
        self.colllected_measures
    }
}

impl TraversalVisitor for MultipliedMeasuresCollector {
    type State = ();
    fn on_node_traverse(
        &mut self,
        node: &Rc<MemberSymbol>,
        _: &Self::State,
    ) -> Result<Option<Self::State>, CubeError> {
        let res = match node.as_ref() {
            MemberSymbol::Measure(e) => {
                let full_name = e.full_name();
                let multiplied = self.join.is_multiplied(&e.cube_name());

                if !self.composite_measures.contains(&full_name) {
                    self.colllected_measures.push(MeasureResult {
                        multiplied,
                        measure: node.clone(),
                        cube_name: node.cube_name(),
                    })
                }

                if self.composite_measures.contains(&full_name) {
                    Some(())
                } else {
                    None
                }
            }
            MemberSymbol::MemberExpression(_) => Some(()),
            MemberSymbol::Dimension(_) => None,
            _ => None,
        };
        Ok(res)
    }
}

pub fn collect_multiplied_measures(
    node: &Rc<MemberSymbol>,
    join: &Rc<JoinTree>,
) -> Result<Vec<MeasureResult>, CubeError> {
    if let Ok(member_expression) = node.as_member_expression() {
        if let Some(cube_names) = member_expression.cube_names_if_dimension_only_expression()? {
            let result = if cube_names.is_empty() {
                vec![MeasureResult {
                    cube_name: node.cube_name().clone(),
                    measure: node.clone(),
                    multiplied: false,
                }]
            } else if cube_names.len() == 1 {
                let cube_name = cube_names[0].clone();
                let multiplied = join.is_multiplied(&cube_name);

                vec![MeasureResult {
                    measure: node.clone(),
                    cube_name,
                    multiplied,
                }]
            } else {
                if cube_names
                    .iter()
                    .any(|cube_name| join.is_multiplied(cube_name))
                {
                    return Err(CubeError::user(format!(
                        "Dimension-only measure {} references cubes {:?} that lead to row multiplication. Please rewrite it using sub query.",
                        node.full_name(),
                        cube_names
                    )));
                }
                // Dimensions from several cubes, but none of them is on the
                // multiplied side of a join - safe to evaluate the expression
                // on top of the join tree as a regular measure.
                vec![MeasureResult {
                    cube_name: node.cube_name().clone(),
                    measure: node.clone(),
                    multiplied: false,
                }]
            };
            return Ok(result);
        }
    }

    let mut composite_collector = CompositeMeasuresCollector::new(join.clone());
    composite_collector.apply(node, &CompositeMeasureCollectorState::new(None))?;
    let composite_measures = composite_collector.extract_result();
    let mut visitor = MultipliedMeasuresCollector::new(composite_measures, join.clone());
    visitor.apply(node, &())?;
    let result = visitor.extract_result();
    Ok(result)
}
