use super::{NodeProcessor, Op, RenderReferencesType};
use crate::physical_plan::cube_ref_evaluator::CubeRefEvaluator;
use crate::physical_plan::sql_nodes::RenderReferences;
use crate::planner::planners::multi_stage::TimeShiftState;
use crate::planner::symbols::CalendarDimensionTimeShift;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

/// Prepend an outer op in front of `tail` so the resulting `Vec<Op>` runs
/// `outer` first and then continues into the previously built pipeline.
fn prepend(outer: Op, tail: Vec<Op>) -> Vec<Op> {
    let mut ops = Vec::with_capacity(tail.len() + 1);
    ops.push(outer);
    ops.extend(tail);
    ops
}

fn op_paren(tail: Vec<Op>) -> Vec<Op> {
    prepend(Op::parenthesize(), tail)
}

fn op_auto_prefix(tail: Vec<Op>, cube_references: HashMap<String, String>) -> Vec<Op> {
    prepend(Op::auto_prefix(cube_references), tail)
}

fn op_measure_filter(tail: Vec<Op>) -> Vec<Op> {
    prepend(Op::measure_filter(), tail)
}

fn op_geo_dimension(tail: Vec<Op>) -> Vec<Op> {
    prepend(Op::geo_dimension(), tail)
}

fn op_render_references(tail: Vec<Op>, references: RenderReferences) -> Vec<Op> {
    prepend(Op::render_references(references), tail)
}

fn op_masked(tail: Vec<Op>, ungrouped: bool) -> Vec<Op> {
    prepend(Op::masked(ungrouped), tail)
}

fn op_case(tail: Vec<Op>) -> Vec<Op> {
    prepend(Op::case(), tail)
}

fn op_dispatch_by_kind(
    dimension: Vec<Op>,
    time_dimension: Vec<Op>,
    measure: Vec<Op>,
    default: Vec<Op>,
) -> Vec<Op> {
    vec![Op::dispatch_by_kind(
        dimension,
        time_dimension,
        measure,
        default,
    )]
}

fn op_final_measure(
    tail: Vec<Op>,
    rendered_as_multiplied_measures: HashSet<String>,
    count_approx_as_state: bool,
) -> Vec<Op> {
    prepend(
        Op::final_measure(rendered_as_multiplied_measures, count_approx_as_state),
        tail,
    )
}

fn op_final_pre_aggregation_measure(tail: Vec<Op>, references: RenderReferences) -> Vec<Op> {
    prepend(Op::final_pre_aggregation_measure(references), tail)
}

fn op_ungrouped_measure(tail: Vec<Op>) -> Vec<Op> {
    prepend(Op::ungrouped_measure(), tail)
}

fn op_ungrouped_query_final_measure(tail: Vec<Op>) -> Vec<Op> {
    prepend(Op::ungrouped_query_final_measure(), tail)
}

fn op_time_dimension(dimensions_with_ignored_timezone: HashSet<String>, tail: Vec<Op>) -> Vec<Op> {
    prepend(Op::time_dimension(dimensions_with_ignored_timezone), tail)
}

fn op_time_shift(shifts: TimeShiftState, tail: Vec<Op>) -> Vec<Op> {
    prepend(Op::time_shift(shifts), tail)
}

fn op_calendar_time_shift(
    shifts: HashMap<String, CalendarDimensionTimeShift>,
    tail: Vec<Op>,
) -> Vec<Op> {
    prepend(Op::calendar_time_shift(shifts), tail)
}

fn op_multi_stage_rank(tail: Vec<Op>, partition: Vec<String>) -> Vec<Op> {
    prepend(Op::multi_stage_rank(partition), tail)
}

fn op_multi_stage_window(
    multi_stage_input: Vec<Op>,
    else_pipeline: Vec<Op>,
    partition: Vec<String>,
) -> Vec<Op> {
    vec![Op::multi_stage_window(
        multi_stage_input,
        else_pipeline,
        partition,
    )]
}

fn op_rolling_window(input_pipeline: Vec<Op>, default_pipeline: Vec<Op>) -> Vec<Op> {
    vec![Op::rolling_window(input_pipeline, default_pipeline)]
}

/// Configuration carrier for assembling a query's render pipeline. Options
/// (time shifts, multi-stage settings, pre-aggregation references, masked
/// measures, …) accumulate via `set_*`/`add_*` methods and feed into
/// [`Self::default_node_processor`].
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

    pub fn cube_ref_evaluator(&self) -> CubeRefEvaluator {
        CubeRefEvaluator::new(
            self.cube_name_references.clone(),
            self.original_sql_pre_aggregations.clone(),
        )
    }

    /// Build the `NodeProcessor` for the current configuration.
    pub fn default_node_processor(&self) -> Rc<NodeProcessor> {
        let evaluate_sql_processor = op_masked(vec![Op::evaluate_symbol()], false);
        let auto_prefix_processor = op_auto_prefix(
            evaluate_sql_processor.clone(),
            self.cube_name_references.clone(),
        );
        let parenthesize_processor = op_paren(auto_prefix_processor);

        let measure_filter_processor = op_measure_filter(parenthesize_processor);
        let measure_processor = op_case(measure_filter_processor.clone());

        let measure_processor = self.add_ungrouped_measure_reference_if_needed(measure_processor);
        let measure_processor = self.final_measure_node_processor(measure_processor);
        // Wrap the entire measure chain with a Masked op so masked measures
        // are intercepted before aggregation/ungrouped wrapping.
        let measure_processor =
            op_masked(measure_processor, self.ungrouped || self.ungrouped_measure);
        let measure_processor =
            self.add_multi_stage_window_if_needed(measure_processor, measure_filter_processor);
        let measure_processor = self.add_multi_stage_rank_if_needed(measure_processor);

        let default_processor = if !self.pre_aggregation_dimensions_references.is_empty() {
            op_render_references(
                evaluate_sql_processor.clone(),
                self.pre_aggregation_dimensions_references.clone(),
            )
        } else {
            evaluate_sql_processor.clone()
        };
        let default_processor = op_paren(default_processor);

        let root_ops = op_dispatch_by_kind(
            self.dimension_processor(evaluate_sql_processor.clone()),
            self.time_dimension_processor(op_paren(evaluate_sql_processor)),
            measure_processor,
            default_processor,
        );
        let root_ops = op_render_references(root_ops, self.render_references.clone());
        NodeProcessor::new(root_ops)
    }

    fn add_ungrouped_measure_reference_if_needed(&self, default: Vec<Op>) -> Vec<Op> {
        if !self.ungrouped_measure_references.is_empty() {
            op_render_references(default, self.ungrouped_measure_references.clone())
        } else {
            default
        }
    }

    fn add_multi_stage_rank_if_needed(&self, default: Vec<Op>) -> Vec<Op> {
        if let Some(partition_by) = &self.multi_stage_rank {
            op_multi_stage_rank(default, partition_by.clone())
        } else {
            default
        }
    }

    fn add_multi_stage_window_if_needed(
        &self,
        else_pipeline: Vec<Op>,
        multi_stage_input: Vec<Op>,
    ) -> Vec<Op> {
        if let Some(partition_by) = &self.multi_stage_window {
            op_multi_stage_window(multi_stage_input, else_pipeline, partition_by.clone())
        } else {
            else_pipeline
        }
    }

    fn final_measure_node_processor(&self, input: Vec<Op>) -> Vec<Op> {
        if self.ungrouped_measure {
            op_ungrouped_measure(input)
        } else if self.ungrouped {
            op_ungrouped_query_final_measure(input)
        } else {
            let final_processor = op_final_measure(
                input.clone(),
                self.rendered_as_multiplied_measures.clone(),
                self.count_approx_as_state,
            );
            let final_processor = if !self.pre_aggregation_measures_references.is_empty() {
                op_final_pre_aggregation_measure(
                    final_processor,
                    self.pre_aggregation_measures_references.clone(),
                )
            } else {
                final_processor
            };
            if self.rolling_window {
                op_rolling_window(input, final_processor)
            } else {
                final_processor
            }
        }
    }

    fn dimension_processor(&self, input: Vec<Op>) -> Vec<Op> {
        let input = if !self.pre_aggregation_dimensions_references.is_empty() {
            op_render_references(input, self.pre_aggregation_dimensions_references.clone())
        } else {
            let input = op_geo_dimension(input);
            op_case(input)
        };

        let input = op_auto_prefix(input, self.cube_name_references.clone());
        let input = op_paren(input);
        let input = op_time_dimension(self.dimensions_with_ignored_timezone.clone(), input);

        let input = if !self.calendar_time_shifts.is_empty() {
            op_calendar_time_shift(self.calendar_time_shifts.clone(), input)
        } else {
            input
        };

        if !self.time_shifts.is_empty() {
            op_time_shift(self.time_shifts.clone(), input)
        } else {
            input
        }
    }

    fn time_dimension_processor(&self, input: Vec<Op>) -> Vec<Op> {
        op_time_dimension(self.dimensions_with_ignored_timezone.clone(), input)
    }
}
