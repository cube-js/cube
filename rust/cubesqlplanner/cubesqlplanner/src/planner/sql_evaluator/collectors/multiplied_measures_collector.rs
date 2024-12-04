use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::{
    EvaluationNode, MemberSymbol, MemberSymbolType, TraversalVisitor,
};
use crate::planner::BaseMeasure;
use cubenativeutils::CubeError;
use std::collections::HashSet;
use std::rc::Rc;

struct CompositeMeasuresCollector {
    parent_measure: Option<Rc<EvaluationNode>>,
    composite_measures: HashSet<String>,
}

impl CompositeMeasuresCollector {
    pub fn new() -> Self {
        Self {
            parent_measure: None,
            composite_measures: HashSet::new(),
        }
    }

    pub fn extract_result(self) -> HashSet<String> {
        self.composite_measures
    }
}

impl TraversalVisitor for CompositeMeasuresCollector {
    fn on_node_traverse(&mut self, node: &Rc<EvaluationNode>) -> Result<bool, CubeError> {
        let res = match node.symbol() {
            MemberSymbolType::Measure(e) => {
                if let Some(parent) = &self.parent_measure {
                    if parent.cube_name() != node.cube_name() {
                        self.composite_measures.insert(parent.full_name());
                    }
                }

                self.parent_measure = Some(node.clone());
                true
            }
            MemberSymbolType::Dimension(_) => false,
            _ => false,
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
    parent_measure: Option<String>,
    root_measure: Option<MeasureResult>,
    colllected_measures: Vec<MeasureResult>,
}

impl MultipliedMeasuresCollector {
    pub fn new(query_tools: Rc<QueryTools>, composite_measures: HashSet<String>) -> Self {
        Self {
            query_tools,
            composite_measures,
            parent_measure: None,
            root_measure: None,
            colllected_measures: vec![],
        }
    }

    pub fn extract_result(self) -> Vec<MeasureResult> {
        self.colllected_measures
    }
}

impl TraversalVisitor for MultipliedMeasuresCollector {
    fn on_node_traverse(&mut self, node: &Rc<EvaluationNode>) -> Result<bool, CubeError> {
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

                self.parent_measure = Some(full_name.clone());
                if self.composite_measures.contains(&full_name) {
                    true
                } else {
                    false
                }
            }
            MemberSymbolType::Dimension(_) => false,
            _ => false,
        };
        Ok(res)
    }
}

pub fn collect_multiplied_measures(
    query_tools: Rc<QueryTools>,
    node: &Rc<EvaluationNode>,
) -> Result<Vec<MeasureResult>, CubeError> {
    let mut composite_collector = CompositeMeasuresCollector::new();
    composite_collector.apply(node)?;
    let composite_measures = composite_collector.extract_result();
    let mut visitor = MultipliedMeasuresCollector::new(query_tools, composite_measures);
    visitor.apply(node)?;
    Ok(visitor.extract_result())
}
