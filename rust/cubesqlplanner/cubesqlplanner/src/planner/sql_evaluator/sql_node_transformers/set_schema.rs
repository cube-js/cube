use crate::plan::schema::Schema;
use crate::planner::sql_evaluator::sql_nodes::final_measure::FinalMeasureSqlNode;
use crate::planner::sql_evaluator::sql_nodes::{
    AutoPrefixSqlNode, EvaluateSqlNode, MeasureFilterSqlNode, MultiStageRankNode,
    MultiStageWindowNode, RenderReferencesSqlNode, RootSqlNode, SqlNode, TimeShiftSqlNode,
};
use std::rc::Rc;

pub fn set_schema(node_processors: Rc<dyn SqlNode>, schema: Rc<Schema>) -> Rc<dyn SqlNode> {
    set_schema_impl(node_processors, schema)
}

pub fn set_schema_impl(sql_node: Rc<dyn SqlNode>, schema: Rc<Schema>) -> Rc<dyn SqlNode> {
    if let Some(auto_prefix) = sql_node
        .clone()
        .as_any()
        .downcast_ref::<AutoPrefixSqlNode>()
    {
        let input = set_schema_impl(auto_prefix.input().clone(), schema.clone());
        AutoPrefixSqlNode::new_with_schema(input, schema)
    } else if let Some(_) = sql_node.clone().as_any().downcast_ref::<EvaluateSqlNode>() {
        sql_node
    } else if let Some(final_measure) = sql_node
        .clone()
        .as_any()
        .downcast_ref::<FinalMeasureSqlNode>()
    {
        let input = set_schema_impl(final_measure.input().clone(), schema.clone());
        FinalMeasureSqlNode::new(input)
    } else if let Some(measure_filter) = sql_node
        .clone()
        .as_any()
        .downcast_ref::<MeasureFilterSqlNode>()
    {
        let input = set_schema_impl(measure_filter.input().clone(), schema.clone());
        MeasureFilterSqlNode::new(input)
    } else if let Some(multi_stage_rank) = sql_node
        .clone()
        .as_any()
        .downcast_ref::<MultiStageRankNode>()
    {
        let else_processor =
            set_schema_impl(multi_stage_rank.else_processor().clone(), schema.clone());
        MultiStageRankNode::new(else_processor, multi_stage_rank.partition().clone())
    } else if let Some(multi_stage_window) = sql_node
        .clone()
        .as_any()
        .downcast_ref::<MultiStageWindowNode>()
    {
        let input = set_schema_impl(multi_stage_window.input().clone(), schema.clone());
        let else_processor =
            set_schema_impl(multi_stage_window.else_processor().clone(), schema.clone());
        MultiStageWindowNode::new(
            input,
            else_processor,
            multi_stage_window.partition().clone(),
        )
    } else if let Some(render_references) = sql_node
        .clone()
        .as_any()
        .downcast_ref::<RenderReferencesSqlNode>()
    {
        let input = set_schema_impl(render_references.input().clone(), schema.clone());
        RenderReferencesSqlNode::new_with_schema(input, schema)
    } else if let Some(root_node) = sql_node.clone().as_any().downcast_ref::<RootSqlNode>() {
        let dimension_processor =
            set_schema_impl(root_node.dimension_processor().clone(), schema.clone());
        let measure_processor =
            set_schema_impl(root_node.measure_processor().clone(), schema.clone());
        let cube_name_processor =
            set_schema_impl(root_node.cube_name_processor().clone(), schema.clone());
        let default_processor =
            set_schema_impl(root_node.default_processor().clone(), schema.clone());
        RootSqlNode::new(
            dimension_processor,
            measure_processor,
            cube_name_processor,
            default_processor,
        )
    } else if let Some(time_shift) = sql_node.clone().as_any().downcast_ref::<TimeShiftSqlNode>() {
        let input = set_schema_impl(time_shift.input().clone(), schema.clone());
        TimeShiftSqlNode::new(time_shift.shifts().clone(), input)
    } else {
        unreachable!("Not all nodes are implemented");
    }
}
