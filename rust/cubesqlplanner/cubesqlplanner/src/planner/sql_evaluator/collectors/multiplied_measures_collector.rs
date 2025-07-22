use crate::cube_bridge::join_definition::JoinDefinition;
use crate::planner::sql_evaluator::{MemberSymbol, TraversalVisitor};
use cubenativeutils::CubeError;
use std::collections::HashSet;
use std::rc::Rc;

struct CompositeMeasuresCollector {
    composite_measures: HashSet<String>,
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

    pub fn extract_result(self) -> HashSet<String> {
        self.composite_measures
    }
}

impl TraversalVisitor for CompositeMeasuresCollector {
    type State = CompositeMeasureCollectorState;
    fn on_node_traverse(
        &mut self,
        node: &Rc<MemberSymbol>,
        _path: &Vec<String>,
        state: &Self::State,
    ) -> Result<Option<Self::State>, CubeError> {
        let res = match node.as_ref() {
            MemberSymbol::Measure(_) => {
                if let Some(parent) = &state.parent_measure {
                    if parent.cube_name() != node.cube_name() {
                        self.composite_measures.insert(parent.full_name());
                    }
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
    join: Rc<dyn JoinDefinition>,
}

impl MultipliedMeasuresCollector {
    pub fn new(composite_measures: HashSet<String>, join: Rc<dyn JoinDefinition>) -> Self {
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
        _path: &Vec<String>,
        _: &Self::State,
    ) -> Result<Option<Self::State>, CubeError> {
        let res = match node.as_ref() {
            MemberSymbol::Measure(e) => {
                let full_name = e.full_name();
                let multiplied = self
                    .join
                    .static_data()
                    .multiplication_factor
                    .get(e.cube_name())
                    .unwrap_or(&false)
                    .clone();

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
    join: Rc<dyn JoinDefinition>,
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
                let multiplied = join
                    .static_data()
                    .multiplication_factor
                    .get(&cube_name)
                    .unwrap_or(&false)
                    .clone();

                vec![MeasureResult {
                    measure: node.clone(),
                    cube_name,
                    multiplied,
                }]
            } else {
                return Err(CubeError::user(format!(
                    "Expected single cube for dimension-only measure {}, got {:?}",
                    node.full_name(),
                    cube_names
                )));
            };
            return Ok(result);
        }
    }

    let mut composite_collector = CompositeMeasuresCollector::new();
    composite_collector.apply(node, &CompositeMeasureCollectorState::new(None))?;
    let composite_measures = composite_collector.extract_result();
    let mut visitor = MultipliedMeasuresCollector::new(composite_measures, join.clone());
    visitor.apply(node, &())?;
    let result = visitor.extract_result();
    Ok(result)
}
