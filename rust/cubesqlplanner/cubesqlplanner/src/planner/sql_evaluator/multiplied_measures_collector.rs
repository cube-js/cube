use super::EvaluationNode;
use super::MemberEvaluator;
use super::TraversalVisitor;
use super::{CubeNameEvaluator, DimensionEvaluator, MeasureEvaluator, MemberEvaluatorType};
use crate::planner::query_tools::QueryTools;
use cubenativeutils::CubeError;
use std::collections::HashSet;
use std::rc::Rc;

pub struct RootMeasureResult {
    pub multiplied: bool,
    pub measure: String,
}

pub struct MultipliedMeasuresCollector {
    query_tools: Rc<QueryTools>,
    parent_measure: Option<String>,
    root_measure: Option<RootMeasureResult>,
}

impl MultipliedMeasuresCollector {
    pub fn new(query_tools: Rc<QueryTools>) -> Self {
        Self {
            query_tools,
            parent_measure: None,
            root_measure: None,
        }
    }

    pub fn extract_result(self) -> Option<RootMeasureResult> {
        self.root_measure
    }
}

impl TraversalVisitor for MultipliedMeasuresCollector {
    fn on_node_traverse(&mut self, node: &Rc<EvaluationNode>) -> Result<bool, CubeError> {
        let res = match node.evaluator() {
            MemberEvaluatorType::Measure(e) => {
                let full_name = e.full_name();
                let join = self.query_tools.cached_data().join()?;
                let multiplied = join
                    .static_data()
                    .multiplication_factor
                    .get(e.cube_name())
                    .unwrap_or(&false)
                    .clone();

                if self.parent_measure.is_none() {
                    self.root_measure = Some(RootMeasureResult {
                        multiplied,
                        measure: full_name.clone(),
                    })
                }
                self.parent_measure = Some(full_name);
                true
            }
            MemberEvaluatorType::Dimension(e) => true,
            MemberEvaluatorType::CubeName(e) => false,
            MemberEvaluatorType::CubeTable(e) => false,
            MemberEvaluatorType::JoinCondition(_) => false,
        };
        Ok(res)
    }
}

pub fn collect_multiplied_measures(
    query_tools: Rc<QueryTools>,
    node: &Rc<EvaluationNode>,
) -> Result<Option<RootMeasureResult>, CubeError> {
    let mut visitor = MultipliedMeasuresCollector::new(query_tools);
    visitor.apply(node)?;
    Ok(visitor.extract_result())
}
