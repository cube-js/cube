use super::{
    AutoPrefixNodeProcessor, EvaluateSqlProcessor, FinalMeasureNodeProcessor,
    MeasureFilterNodeProcessor, RenderReferencesNodeProcessor, RootNodeProcessor,
};
use crate::planner::sql_evaluator::default_visitor::NodeProcessorItem;
use std::collections::HashMap;
use std::rc::Rc;

pub fn default_node_processor() -> Rc<dyn NodeProcessorItem> {
    let evaluate_sql_processor = EvaluateSqlProcessor::new();
    let auto_prefix_processor = AutoPrefixNodeProcessor::new(evaluate_sql_processor.clone());
    let measure_filter_processor = MeasureFilterNodeProcessor::new(auto_prefix_processor.clone());
    let final_measure_processor = FinalMeasureNodeProcessor::new(measure_filter_processor.clone());
    RootNodeProcessor::new(
        auto_prefix_processor.clone(),
        final_measure_processor.clone(),
        auto_prefix_processor.clone(),
        evaluate_sql_processor.clone(),
    )
}

pub fn with_render_references_default_node_processor(
    references: HashMap<String, String>,
) -> Rc<dyn NodeProcessorItem> {
    let default_processor = default_node_processor();
    RenderReferencesNodeProcessor::new(references, default_processor)
}
