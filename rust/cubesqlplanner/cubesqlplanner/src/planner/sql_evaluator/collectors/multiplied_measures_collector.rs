use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::{
    EvaluationNode, MemberSymbol, MemberSymbolType, TraversalVisitor,
};
use crate::planner::BaseMeasure;
use cubenativeutils::CubeError;
use std::collections::HashSet;
use std::rc::Rc;

struct CompositeMeasuresCollector {
    composite_measures: HashSet<String>,
}

struct CompositeMeasureCollectorState {
    pub parent_measure: Option<Rc<EvaluationNode>>,
}

impl CompositeMeasureCollectorState {
    pub fn new(parent_measure: Option<Rc<EvaluationNode>>) -> Self {
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
        node: &Rc<EvaluationNode>,
        state: &Self::State,
    ) -> Result<Option<Self::State>, CubeError> {
        let res = match node.symbol() {
            MemberSymbolType::Measure(e) => {
                if let Some(parent) = &state.parent_measure {
                    if parent.cube_name() != node.cube_name() {
                        self.composite_measures.insert(parent.full_name());
                    }
                }

                let new_state = CompositeMeasureCollectorState::new(Some(node.clone()));
                Some(new_state)
            }
            MemberSymbolType::Dimension(_) => None,
            _ => None,
        };
        Ok(res)
    }
}

pub struct MeasureResult {
    pub multiplied: bool,
    pub measure: Rc<BaseMeasure>,
}

pub struct MultipliedMeasuresCollector {
    query_tools: Rc<QueryTools>,
    composite_measures: HashSet<String>,
    colllected_measures: Vec<MeasureResult>,
}

impl MultipliedMeasuresCollector {
    pub fn new(query_tools: Rc<QueryTools>, composite_measures: HashSet<String>) -> Self {
        Self {
            query_tools,
            composite_measures,
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
        node: &Rc<EvaluationNode>,
        state: &Self::State,
    ) -> Result<Option<Self::State>, CubeError> {
        let res = match node.symbol() {
            MemberSymbolType::Measure(e) => {
                let full_name = e.full_name();
                let join = self.query_tools.cached_data().join()?;
                let multiplied = join
                    .static_data()
                    .multiplication_factor
                    .get(e.cube_name())
                    .unwrap_or(&false)
                    .clone();

                if !self.composite_measures.contains(&full_name) {
                    self.colllected_measures.push(MeasureResult {
                        multiplied,
                        measure: BaseMeasure::try_new(node.clone(), self.query_tools.clone())?
                            .unwrap(),
                    })
                }

                if self.composite_measures.contains(&full_name) {
                    Some(())
                } else {
                    None
                }
            }
            MemberSymbolType::Dimension(_) => None,
            _ => None,
        };
        Ok(res)
    }
}

pub fn collect_multiplied_measures(
    query_tools: Rc<QueryTools>,
    node: &Rc<EvaluationNode>,
) -> Result<Vec<MeasureResult>, CubeError> {
    let mut composite_collector = CompositeMeasuresCollector::new();
    composite_collector.apply(node, &CompositeMeasureCollectorState::new(None))?;
    let composite_measures = composite_collector.extract_result();
    let mut visitor = MultipliedMeasuresCollector::new(query_tools, composite_measures);
    visitor.apply(node, &())?;
    Ok(visitor.extract_result())
}
