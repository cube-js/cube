use crate::cube_bridge::join_definition::JoinDefinition;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::{MemberSymbol, TraversalVisitor};
use crate::planner::BaseMeasure;
use cubenativeutils::CubeError;
use std::collections::HashSet;
use std::rc::Rc;

struct CompositeMeasuresCollector {
    composite_measures: HashSet<String>,
}

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
    join: Rc<dyn JoinDefinition>,
}

impl MultipliedMeasuresCollector {
    pub fn new(
        query_tools: Rc<QueryTools>,
        composite_measures: HashSet<String>,
        join: Rc<dyn JoinDefinition>,
    ) -> Self {
        Self {
            query_tools,
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
            MemberSymbol::Dimension(_) => None,
            _ => None,
        };
        Ok(res)
    }
}

pub fn collect_multiplied_measures(
    query_tools: Rc<QueryTools>,
    node: &Rc<MemberSymbol>,
    join: Rc<dyn JoinDefinition>,
) -> Result<Vec<MeasureResult>, CubeError> {
    let mut composite_collector = CompositeMeasuresCollector::new();
    composite_collector.apply(node, &CompositeMeasureCollectorState::new(None))?;
    let composite_measures = composite_collector.extract_result();
    let mut visitor = MultipliedMeasuresCollector::new(query_tools, composite_measures, join);
    visitor.apply(node, &())?;
    Ok(visitor.extract_result())
}
