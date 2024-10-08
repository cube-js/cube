use super::{
    AutoPrefixSqlNode, EvaluateSqlNode, FinalMeasureSqlNode, MeasureFilterSqlNode,
    RenderReferencesSqlNode, RootSqlNode, SqlNode,
};
use std::collections::HashMap;
use std::rc::Rc;

pub fn default_node_processor() -> Rc<dyn SqlNode> {
    let evaluate_sql_processor = EvaluateSqlNode::new();
    let auto_prefix_processor = AutoPrefixSqlNode::new(evaluate_sql_processor.clone());
    let measure_filter_processor = MeasureFilterSqlNode::new(auto_prefix_processor.clone());
    let final_measure_processor = FinalMeasureSqlNode::new(measure_filter_processor.clone());
    RootSqlNode::new(
        auto_prefix_processor.clone(),
        final_measure_processor.clone(),
        auto_prefix_processor.clone(),
        evaluate_sql_processor.clone(),
    )
}

pub fn with_render_references_default_node_processor(
    references: HashMap<String, String>,
) -> Rc<dyn SqlNode> {
    let default_processor = default_node_processor();
    RenderReferencesSqlNode::new(references, default_processor)
}
