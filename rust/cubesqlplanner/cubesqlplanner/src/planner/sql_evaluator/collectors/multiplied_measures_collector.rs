use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::{
    EvaluationNode, MemberSymbol, MemberSymbolType, TraversalVisitor,
};
use cubenativeutils::CubeError;
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

                if self.parent_measure.is_none() {
                    self.root_measure = Some(RootMeasureResult {
                        multiplied,
                        measure: full_name.clone(),
                    })
                }
                self.parent_measure = Some(full_name);
                true
            }
            MemberSymbolType::Dimension(_) => true,
            _ => false,
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
