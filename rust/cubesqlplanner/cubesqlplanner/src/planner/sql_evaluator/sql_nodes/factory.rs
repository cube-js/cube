use super::{
    AutoPrefixSqlNode, CaseSqlNode, EvaluateSqlNode, FinalMeasureSqlNode,
    FinalPreAggregationMeasureSqlNode, GeoDimensionSqlNode, MeasureFilterSqlNode,
    MultiStageRankNode, MultiStageWindowNode, OriginalSqlPreAggregationSqlNode,
    RenderReferencesSqlNode, RenderReferencesType, RollingWindowNode, RootSqlNode, SqlNode,
    TimeDimensionNode, TimeShiftSqlNode, UngroupedMeasureSqlNode,
    UngroupedQueryFinalMeasureSqlNode,
};
use crate::planner::planners::multi_stage::TimeShiftState;
use crate::planner::sql_evaluator::sql_nodes::calendar_time_shift::CalendarTimeShiftSqlNode;
use crate::planner::sql_evaluator::sql_nodes::RenderReferences;
use crate::planner::sql_evaluator::symbols::CalendarDimensionTimeShift;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

#[derive(Clone, Default)]
pub struct SqlNodesFactory {
    time_shifts: TimeShiftState,
    calendar_time_shifts: HashMap<String, CalendarDimensionTimeShift>,
    ungrouped: bool,
    ungrouped_measure: bool,
    count_approx_as_state: bool,
    render_references: RenderReferences,
    pre_aggregation_dimensions_references: RenderReferences,
    pre_aggregation_measures_references: RenderReferences,
    rendered_as_multiplied_measures: HashSet<String>,
    ungrouped_measure_references: RenderReferences,
    cube_name_references: HashMap<String, String>,
    multi_stage_rank: Option<Vec<String>>,   //partition_by
    multi_stage_window: Option<Vec<String>>, //partition_by
    rolling_window: bool,
    dimensions_with_ignored_timezone: HashSet<String>,
    use_local_tz_in_date_range: bool,
    original_sql_pre_aggregations: HashMap<String, String>,
}

impl SqlNodesFactory {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_time_shifts(&mut self, time_shifts: TimeShiftState) {
        self.time_shifts = time_shifts;
    }

    pub fn set_calendar_time_shifts(
        &mut self,
        calendar_time_shifts: HashMap<String, CalendarDimensionTimeShift>,
    ) {
        self.calendar_time_shifts = calendar_time_shifts;
    }

    pub fn set_ungrouped(&mut self, value: bool) {
        self.ungrouped = value;
    }

    pub fn set_use_local_tz_in_date_range(&mut self, value: bool) {
        self.use_local_tz_in_date_range = value;
    }

    pub fn use_local_tz_in_date_range(&self) -> bool {
        self.use_local_tz_in_date_range
    }

    pub fn set_ungrouped_measure(&mut self, value: bool) {
        self.ungrouped_measure = value;
    }

    pub fn add_render_reference<T: Into<RenderReferencesType>>(&mut self, name: String, value: T) {
        self.render_references.insert(name, value);
    }

    pub fn render_references(&self) -> &RenderReferences {
        &self.render_references
    }

    pub fn clear_render_references(&mut self) {
        self.render_references = RenderReferences::default();
    }

    pub fn render_references_mut(&mut self) -> &mut RenderReferences {
        &mut self.render_references
    }

    pub fn set_rendered_as_multiplied_measures(&mut self, value: HashSet<String>) {
        self.rendered_as_multiplied_measures = value;
    }

    pub fn add_pre_aggregation_dimension_reference<T: Into<RenderReferencesType>>(
        &mut self,
        name: String,
        value: T,
    ) {
        self.pre_aggregation_dimensions_references
            .insert(name, value);
    }

    pub fn set_original_sql_pre_aggregations(&mut self, value: HashMap<String, String>) {
        self.original_sql_pre_aggregations = value;
    }

    pub fn add_dimensions_with_ignored_timezone(&mut self, value: String) {
        self.dimensions_with_ignored_timezone.insert(value);
    }

    pub fn set_multi_stage_rank(&mut self, partition_by: Vec<String>) {
        self.multi_stage_rank = Some(partition_by);
    }

    pub fn set_multi_stage_window(&mut self, partition_by: Vec<String>) {
        self.multi_stage_window = Some(partition_by);
    }

    pub fn add_pre_aggregation_measure_reference<T: Into<RenderReferencesType>>(
        &mut self,
        name: String,
        value: T,
    ) {
        self.pre_aggregation_measures_references.insert(name, value);
    }

    pub fn set_rolling_window(&mut self, value: bool) {
        self.rolling_window = value;
    }

    pub fn set_count_approx_as_state(&mut self, value: bool) {
        self.count_approx_as_state = value;
    }

    pub fn add_ungrouped_measure_reference<T: Into<RenderReferencesType>>(
        &mut self,
        name: String,
        value: T,
    ) {
        self.ungrouped_measure_references.insert(name, value);
    }

    pub fn set_cube_name_references(&mut self, value: HashMap<String, String>) {
        self.cube_name_references = value;
    }

    pub fn add_cube_name_reference(&mut self, key: String, value: String) {
        self.cube_name_references.insert(key, value);
    }

    pub fn default_node_processor(&self) -> Rc<dyn SqlNode> {
        let evaluate_sql_processor = EvaluateSqlNode::new();
        let auto_prefix_processor = AutoPrefixSqlNode::new(
            evaluate_sql_processor.clone(),
            self.cube_name_references.clone(),
        );

        let measure_filter_processor = MeasureFilterSqlNode::new(auto_prefix_processor.clone());
        let measure_processor = CaseSqlNode::new(measure_filter_processor.clone());

        let measure_processor = self.add_ungrouped_measure_reference_if_needed(measure_processor);
        let measure_processor = self.final_measure_node_processor(measure_processor);
        let measure_processor = self
            .add_multi_stage_window_if_needed(measure_processor, measure_filter_processor.clone());
        let measure_processor = self.add_multi_stage_rank_if_needed(measure_processor);

        let default_processor: Rc<dyn SqlNode> =
            if !self.pre_aggregation_dimensions_references.is_empty() {
                RenderReferencesSqlNode::new(
                    evaluate_sql_processor.clone(),
                    self.pre_aggregation_dimensions_references.clone(),
                )
            } else {
                evaluate_sql_processor.clone()
            };

        let root_node = RootSqlNode::new(
            self.dimension_processor(evaluate_sql_processor.clone()),
            self.time_dimension_processor(evaluate_sql_processor.clone()),
            measure_processor.clone(),
            auto_prefix_processor.clone(),
            self.cube_table_processor(evaluate_sql_processor.clone()),
            default_processor,
        );
        RenderReferencesSqlNode::new(root_node, self.render_references.clone())
    }

    fn cube_table_processor(&self, default: Rc<dyn SqlNode>) -> Rc<dyn SqlNode> {
        if !self.original_sql_pre_aggregations.is_empty() {
            OriginalSqlPreAggregationSqlNode::new(
                default,
                self.original_sql_pre_aggregations.clone(),
            )
        } else {
            default
        }
    }
    fn add_ungrouped_measure_reference_if_needed(
        &self,
        default: Rc<dyn SqlNode>,
    ) -> Rc<dyn SqlNode> {
        if !self.ungrouped_measure_references.is_empty() {
            RenderReferencesSqlNode::new(default, self.ungrouped_measure_references.clone())
        } else {
            default
        }
    }

    fn add_multi_stage_rank_if_needed(&self, default: Rc<dyn SqlNode>) -> Rc<dyn SqlNode> {
        if let Some(partition_by) = &self.multi_stage_rank {
            MultiStageRankNode::new(default, partition_by.clone())
        } else {
            default
        }
    }

    fn add_multi_stage_window_if_needed(
        &self,
        default: Rc<dyn SqlNode>,
        multi_stage_input: Rc<dyn SqlNode>,
    ) -> Rc<dyn SqlNode> {
        if let Some(partition_by) = &self.multi_stage_window {
            MultiStageWindowNode::new(multi_stage_input, default, partition_by.clone())
        } else {
            default
        }
    }

    fn final_measure_node_processor(&self, input: Rc<dyn SqlNode>) -> Rc<dyn SqlNode> {
        if self.ungrouped_measure {
            UngroupedMeasureSqlNode::new(input)
        } else if self.ungrouped {
            UngroupedQueryFinalMeasureSqlNode::new(input)
        } else {
            let final_processor: Rc<dyn SqlNode> = FinalMeasureSqlNode::new(
                input.clone(),
                self.rendered_as_multiplied_measures.clone(),
                self.count_approx_as_state,
            );
            let final_processor = if !self.pre_aggregation_measures_references.is_empty() {
                FinalPreAggregationMeasureSqlNode::new(
                    final_processor,
                    self.pre_aggregation_measures_references.clone(),
                )
            } else {
                final_processor
            };
            if self.rolling_window {
                RollingWindowNode::new(input, final_processor)
            } else {
                final_processor
            }
        }
    }

    fn dimension_processor(&self, input: Rc<dyn SqlNode>) -> Rc<dyn SqlNode> {
        let input = if !self.pre_aggregation_dimensions_references.is_empty() {
            RenderReferencesSqlNode::new(input, self.pre_aggregation_dimensions_references.clone())
        } else {
            let input: Rc<dyn SqlNode> = GeoDimensionSqlNode::new(input);
            let input: Rc<dyn SqlNode> = CaseSqlNode::new(input);
            input
        };
        let input: Rc<dyn SqlNode> =
            TimeDimensionNode::new(self.dimensions_with_ignored_timezone.clone(), input);

        let input: Rc<dyn SqlNode> =
            AutoPrefixSqlNode::new(input, self.cube_name_references.clone());

        let input = if !self.calendar_time_shifts.is_empty() {
            CalendarTimeShiftSqlNode::new(self.calendar_time_shifts.clone(), input)
        } else {
            input
        };

        let input = if !self.time_shifts.is_empty() {
            TimeShiftSqlNode::new(self.time_shifts.clone(), input)
        } else {
            input
        };

        input
    }

    fn time_dimension_processor(&self, input: Rc<dyn SqlNode>) -> Rc<dyn SqlNode> {
        let input: Rc<dyn SqlNode> =
            TimeDimensionNode::new(self.dimensions_with_ignored_timezone.clone(), input);

        input
    }
}
