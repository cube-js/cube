use super::{
    AutoPrefixSqlNode, EvaluateSqlNode, FinalMeasureSqlNode, MeasureFilterSqlNode,
    MultiStageRankNode, MultiStageWindowNode, RenderReferencesSqlNode, RootSqlNode, SqlNode,
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

pub fn multi_stage_rank_node_processor(
    partition: Vec<String>,
    references: HashMap<String, String>,
) -> Rc<dyn SqlNode> {
    let evaluate_sql_processor = EvaluateSqlNode::new();
    let auto_prefix_processor = AutoPrefixSqlNode::new(evaluate_sql_processor.clone());
    let measure_filter_processor = MeasureFilterSqlNode::new(auto_prefix_processor.clone());
    let final_measure_processor = FinalMeasureSqlNode::new(measure_filter_processor.clone());

    let rank_processor = MultiStageRankNode::new(final_measure_processor.clone(), partition);

    let root_processor = RootSqlNode::new(
        auto_prefix_processor.clone(),
        rank_processor.clone(),
        auto_prefix_processor.clone(),
        evaluate_sql_processor.clone(),
    );
    let references_processor = RenderReferencesSqlNode::new(references, root_processor);
    references_processor
}

pub fn multi_stage_window_node_processor(
    partition: Vec<String>,
    references: HashMap<String, String>,
) -> Rc<dyn SqlNode> {
    let evaluate_sql_processor = EvaluateSqlNode::new();
    let auto_prefix_processor = AutoPrefixSqlNode::new(evaluate_sql_processor.clone());
    let measure_filter_processor = MeasureFilterSqlNode::new(auto_prefix_processor.clone());
    let final_measure_processor = FinalMeasureSqlNode::new(measure_filter_processor.clone());

    let rank_processor = MultiStageWindowNode::new(
        evaluate_sql_processor.clone(),
        final_measure_processor.clone(),
        partition,
    );

    let root_processor = RootSqlNode::new(
        auto_prefix_processor.clone(),
        rank_processor.clone(),
        auto_prefix_processor.clone(),
        evaluate_sql_processor.clone(),
    );
    let references_processor = RenderReferencesSqlNode::new(references, root_processor);
    references_processor
}
