use super::{
    AutoPrefixSqlNode, EvaluateSqlNode, FinalMeasureSqlNode, MeasureFilterSqlNode,
    MultiStageRankNode, MultiStageWindowNode, RenderReferencesSqlNode, RootSqlNode, SqlNode,
    TimeShiftSqlNode,
};
use std::collections::HashMap;
use std::rc::Rc;

pub struct SqlNodesFactory {
    time_shifts: Option<HashMap<String, String>>,
}

impl SqlNodesFactory {
    pub fn new() -> Rc<Self> {
        Rc::new(Self { time_shifts: None })
    }
    pub fn new_with_time_shifts(time_shifts: HashMap<String, String>) -> Rc<Self> {
        Rc::new(Self {
            time_shifts: Some(time_shifts),
        })
    }
    pub fn default_node_processor(&self) -> Rc<dyn SqlNode> {
        let evaluate_sql_processor = EvaluateSqlNode::new();
        let auto_prefix_processor = AutoPrefixSqlNode::new(evaluate_sql_processor.clone());
        let measure_filter_processor = MeasureFilterSqlNode::new(auto_prefix_processor.clone());
        let final_measure_processor = FinalMeasureSqlNode::new(measure_filter_processor.clone());
        let root_node = RootSqlNode::new(
            self.dimension_processor(auto_prefix_processor.clone()),
            final_measure_processor.clone(),
            auto_prefix_processor.clone(),
            evaluate_sql_processor.clone(),
        );
        RenderReferencesSqlNode::new(root_node)
    }

    pub fn multi_stage_rank_node_processor(&self, partition: Vec<String>) -> Rc<dyn SqlNode> {
        let evaluate_sql_processor = EvaluateSqlNode::new();
        let auto_prefix_processor = AutoPrefixSqlNode::new(evaluate_sql_processor.clone());
        let measure_filter_processor = MeasureFilterSqlNode::new(auto_prefix_processor.clone());
        let final_measure_processor = FinalMeasureSqlNode::new(measure_filter_processor.clone());

        let rank_processor = MultiStageRankNode::new(final_measure_processor.clone(), partition);

        let root_processor = RootSqlNode::new(
            self.dimension_processor(auto_prefix_processor.clone()),
            rank_processor.clone(),
            auto_prefix_processor.clone(),
            evaluate_sql_processor.clone(),
        );
        let references_processor = RenderReferencesSqlNode::new(root_processor);
        references_processor
    }

    pub fn multi_stage_window_node_processor(&self, partition: Vec<String>) -> Rc<dyn SqlNode> {
        let evaluate_sql_processor = EvaluateSqlNode::new();
        let auto_prefix_processor = AutoPrefixSqlNode::new(evaluate_sql_processor.clone());
        let measure_filter_processor = MeasureFilterSqlNode::new(auto_prefix_processor.clone());
        let final_measure_processor = FinalMeasureSqlNode::new(measure_filter_processor.clone());

        let window_processor = MultiStageWindowNode::new(
            evaluate_sql_processor.clone(),
            final_measure_processor.clone(),
            partition,
        );

        let root_processor = RootSqlNode::new(
            self.dimension_processor(auto_prefix_processor.clone()),
            window_processor.clone(),
            auto_prefix_processor.clone(),
            evaluate_sql_processor.clone(),
        );
        let references_processor = RenderReferencesSqlNode::new(root_processor);
        references_processor
    }

    fn dimension_processor(&self, input: Rc<dyn SqlNode>) -> Rc<dyn SqlNode> {
        if let Some(time_shifts) = &self.time_shifts {
            TimeShiftSqlNode::new(time_shifts.clone(), input)
        } else {
            input
        }
    }
}
