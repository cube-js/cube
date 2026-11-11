use super::{
    AutoPrefixSqlNode, CaseSqlNode, EvaluateSqlNode, FinalMeasureSqlNode,
    FinalPreAggregationMeasureSqlNode, GeoDimensionSqlNode, MaskedSqlNode, MeasureFilterSqlNode,
    MeasureRenderModifierSqlNode, MultiStageRankNode, MultiStageWindowNode, ParenthesizeSqlNode,
    PreAggregationColumnSqlNode, PreAggregationColumns, RollingWindowNode, RootSqlNode,
    SegmentDimensionSqlNode, SqlNode, TimeDimensionNode, TimeShiftSqlNode, UngroupedMeasureSqlNode,
    UngroupedQueryFinalMeasureSqlNode,
};
use crate::physical_plan::cube_ref_evaluator::CubeRefEvaluator;
use crate::physical_plan::sql_nodes::calendar_time_shift::CalendarTimeShiftSqlNode;
use crate::physical_plan::QualifiedColumnName;
use crate::planner::planners::multi_stage::{FilterParamsTimeShifts, TimeShiftState};
use crate::planner::query_tools::QueryTools;
use crate::planner::symbols::CalendarDimensionTimeShift;
use crate::planner::time_dimension::SqlInterval;
use crate::planner::{CubeId, MemberId};
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

/// Builds the SQL-node chain for a query. Carries the reference maps
/// the query needs (time shifts, pre-aggregation columns, cube aliases)
/// and assembles them into a layered `SqlNode` via
/// `default_node_processor`.
#[derive(Clone, Default)]
pub struct SqlNodesFactory {
    time_shifts: TimeShiftState,
    calendar_time_shifts: HashMap<MemberId, CalendarDimensionTimeShift>,
    calendar_interval_shifts: TimeShiftState,
    // Members the cube join conditions reference; `None` when unknown.
    members_in_join_conditions: Option<Rc<HashSet<MemberId>>>,
    renders_join_condition: bool,
    // The cube a rendered join condition brings in; only its own calendar
    // keys are shifted there.
    joined_cube: Option<CubeId>,
    filter_params_time_shifts: FilterParamsTimeShifts,
    pre_aggregation_dimensions_references: PreAggregationColumns,
    pre_aggregation_measures_references: PreAggregationColumns,
    cube_name_references: HashMap<CubeId, String>,
    use_local_tz_in_date_range: bool,
    original_sql_pre_aggregations: HashMap<CubeId, String>,
    // Ids of the members present in the query GROUP BY. Used by
    // MaskedSqlNode to decide whether conditional masking can be applied to an
    // aggregate measure.
    group_by_members: HashSet<MemberId>,
}

impl SqlNodesFactory {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_time_shifts(&mut self, time_shifts: TimeShiftState) {
        self.time_shifts = time_shifts;
    }

    pub fn time_shifts(&self) -> &TimeShiftState {
        &self.time_shifts
    }

    pub fn set_filter_params_time_shifts(&mut self, shifts: FilterParamsTimeShifts) {
        self.filter_params_time_shifts = shifts;
    }

    pub fn filter_params_time_shifts(&self) -> &FilterParamsTimeShifts {
        &self.filter_params_time_shifts
    }

    pub fn set_calendar_time_shifts(
        &mut self,
        interval_shifts: TimeShiftState,
        calendar_shifts: HashMap<MemberId, CalendarDimensionTimeShift>,
    ) {
        self.calendar_interval_shifts = interval_shifts;
        self.calendar_time_shifts = calendar_shifts;
    }

    /// A calendar shift is rendered only in the join into the calendar when that
    /// join references the calendar key, so the calendar's own columns stay on
    /// the reporting dates; otherwise it is rendered wherever the key is read.
    pub fn set_members_in_join_conditions(&mut self, keys: HashSet<MemberId>) {
        self.members_in_join_conditions = Some(Rc::new(keys));
    }

    /// The factory a cube join condition renders with, `None` when it would
    /// render the same as this one.
    pub fn join_condition_factory(&self) -> Option<Self> {
        if self.time_shifts.is_empty()
            && self.calendar_interval_shifts.is_empty()
            && self.calendar_time_shifts.is_empty()
        {
            return None;
        }
        let mut factory = self.clone();
        factory.renders_join_condition = true;
        Some(factory)
    }

    pub fn with_joined_cube(mut self, cube: CubeId) -> Self {
        self.joined_cube = Some(cube);
        self
    }

    fn is_shifted_in_join(&self, key: &MemberId) -> bool {
        self.members_in_join_conditions
            .as_ref()
            .is_none_or(|keys| keys.contains(key))
    }

    // Only the join into a member's own cube picks which of its rows match;
    // any other join reads the member the way the select does.
    fn renders_join_into_cube_of(&self, key: &MemberId) -> bool {
        self.renders_join_condition
            && self
                .joined_cube
                .as_ref()
                .is_some_and(|cube| key.cube() == cube)
    }

    fn renders_calendar_shift(&self, key: &MemberId) -> bool {
        if self.renders_join_into_cube_of(key) {
            self.is_shifted_in_join(key)
        } else {
            self.members_in_join_conditions.is_none() || !self.is_shifted_in_join(key)
        }
    }

    // A shifted join picks the calendar row the other way round.
    fn oriented_interval(
        &self,
        key: &MemberId,
        interval: &Option<SqlInterval>,
    ) -> Option<SqlInterval> {
        if self.is_shifted_in_join(key) {
            interval.as_ref().map(|interval| interval.inverse())
        } else {
            interval.clone()
        }
    }

    fn rendered_time_shifts(&self) -> TimeShiftState {
        let mut shifts = self.time_shifts.clone();
        // An interval moves the value read, not which rows of its cube match.
        shifts
            .dimensions_shifts
            .retain(|key, _| !self.renders_join_into_cube_of(key));
        for (key, shift) in self.calendar_interval_shifts.dimensions_shifts.iter() {
            if self.renders_calendar_shift(key) {
                let mut shift = shift.clone();
                shift.interval = self.oriented_interval(key, &shift.interval);
                shifts.dimensions_shifts.insert(key.clone(), shift);
            }
        }
        shifts
    }

    fn rendered_calendar_time_shifts(&self) -> HashMap<MemberId, CalendarDimensionTimeShift> {
        self.calendar_time_shifts
            .iter()
            .filter(|(key, _)| self.renders_calendar_shift(key))
            .map(|(key, shift)| {
                let mut shift = shift.clone();
                shift.interval = self.oriented_interval(key, &shift.interval);
                (key.clone(), shift)
            })
            .collect()
    }

    pub fn set_use_local_tz_in_date_range(&mut self, value: bool) {
        self.use_local_tz_in_date_range = value;
    }

    pub fn set_group_by_members(&mut self, value: HashSet<MemberId>) {
        self.group_by_members = value;
    }

    pub fn use_local_tz_in_date_range(&self) -> bool {
        self.use_local_tz_in_date_range
    }

    pub fn reading_pre_aggregation(&self) -> bool {
        !self.pre_aggregation_dimensions_references.is_empty()
    }

    pub fn add_pre_aggregation_dimension_reference(
        &mut self,
        name: String,
        column: QualifiedColumnName,
    ) {
        self.pre_aggregation_dimensions_references
            .insert(name, column);
    }

    pub fn set_original_sql_pre_aggregations(&mut self, value: HashMap<CubeId, String>) {
        self.original_sql_pre_aggregations = value;
    }

    pub fn add_pre_aggregation_measure_reference(
        &mut self,
        name: String,
        column: QualifiedColumnName,
    ) {
        self.pre_aggregation_measures_references
            .insert(name, column);
    }

    pub fn set_cube_name_references(&mut self, value: HashMap<CubeId, String>) {
        self.cube_name_references = value;
    }

    pub fn add_cube_name_reference(&mut self, key: CubeId, value: String) {
        self.cube_name_references.insert(key, value);
    }

    pub fn cube_ref_evaluator(&self) -> CubeRefEvaluator {
        CubeRefEvaluator::new(
            self.cube_name_references.clone(),
            self.original_sql_pre_aggregations.clone(),
        )
    }

    /// Assembles the full SQL-node chain for the configured options.
    /// Three sub-chains hang off a `RootSqlNode` keyed by member
    /// kind: a dimension chain (geo / case / time-shift / calendar
    /// time-shift wraps), a time-dimension chain, and a measure
    /// chain (case → measure filter → render-modifier dispatch over
    /// the final-measure / rolling-merge / ungrouped chains → mask →
    /// multi-stage window and rank wraps).
    pub fn default_node_processor(&self, query_tools: &QueryTools) -> Rc<dyn SqlNode> {
        // Build an "unmasked" copy of the tree (masking disabled, but still
        // dispatching by member kind) only when the query has masked members. It
        // is handed to the masked nodes so they can render mask-filter member
        // references through it — routing dimensions through the dimension chain
        // and avoiding mask recursion. For queries with no masked members
        // `MaskedSqlNode::resolve_mask` short-circuits and never dereferences the
        // unmasked root, so building it would be pure waste; passing `None` falls
        // back to the masked node's own input (see masked.rs).
        let unmasked_root = if query_tools.has_masked_members() {
            Some(self.build_node_processor(true, None))
        } else {
            None
        };
        self.build_node_processor(false, unmasked_root)
    }

    fn build_node_processor(
        &self,
        skip_masking: bool,
        unmasked_root: Option<Rc<dyn SqlNode>>,
    ) -> Rc<dyn SqlNode> {
        let evaluate_sql_processor = MaskedSqlNode::new(
            EvaluateSqlNode::new(),
            false,
            self.group_by_members.clone(),
            skip_masking,
            unmasked_root.clone(),
        );
        let auto_prefix_processor = AutoPrefixSqlNode::new(
            evaluate_sql_processor.clone(),
            self.cube_name_references.clone(),
        );
        let parenthesize_processor: Rc<dyn SqlNode> =
            ParenthesizeSqlNode::new(auto_prefix_processor.clone());

        let measure_filter_processor = MeasureFilterSqlNode::new(parenthesize_processor.clone());
        let measure_processor = CaseSqlNode::new(measure_filter_processor.clone());

        let measure_processor = self.final_measure_node_processor(measure_processor);
        // Wrap the entire measure chain with MaskedSqlNode so masked measures
        // are intercepted before aggregation/ungrouped wrapping.
        let measure_processor = MaskedSqlNode::new(
            measure_processor,
            true,
            self.group_by_members.clone(),
            skip_masking,
            unmasked_root.clone(),
        );
        let measure_processor: Rc<dyn SqlNode> =
            MultiStageWindowNode::new(measure_filter_processor.clone(), measure_processor);
        let measure_processor: Rc<dyn SqlNode> = MultiStageRankNode::new(measure_processor);

        let default_processor: Rc<dyn SqlNode> =
            if !self.pre_aggregation_dimensions_references.is_empty() {
                // Reading from a pre-aggregation: a segment is already a stored
                // column, so it needs no SegmentDimensionSqlNode, only the mask.
                self.pre_aggregation_dimension_columns(
                    evaluate_sql_processor.clone(),
                    skip_masking,
                    unmasked_root.clone(),
                )
            } else {
                // Building/evaluating the expression: wrap segment dimensions per
                // dialect so a boolean is a valid projected value.
                SegmentDimensionSqlNode::new(evaluate_sql_processor.clone())
            };
        let default_processor: Rc<dyn SqlNode> = ParenthesizeSqlNode::new(default_processor);

        // A reference renders its target; of its own it only applies its own
        // mask and parenthesizes the target for its context.
        let reference_processor: Rc<dyn SqlNode> =
            ParenthesizeSqlNode::new(evaluate_sql_processor.clone());

        RootSqlNode::new(
            self.dimension_processor(
                evaluate_sql_processor.clone(),
                skip_masking,
                unmasked_root.clone(),
            ),
            self.time_dimension_processor(ParenthesizeSqlNode::new(evaluate_sql_processor.clone())),
            measure_processor.clone(),
            reference_processor,
            EvaluateSqlNode::new(),
            default_processor,
        )
    }

    /// When an ungrouped query reads from a pre-aggregation, a measure must
    /// resolve to its stored rollup column. Unlike the grouped case, the column
    /// already holds the aggregated value and is returned as-is, so it's a plain
    /// column reference with no `sum()` wrap. Wrapping the ungrouped node keeps
    /// the reference outermost so the measure is intercepted before it would
    /// otherwise re-render its base-table SQL.
    fn wrap_ungrouped_pre_aggregation_measure(&self, node: Rc<dyn SqlNode>) -> Rc<dyn SqlNode> {
        if !self.pre_aggregation_measures_references.is_empty() {
            PreAggregationColumnSqlNode::new(node, self.pre_aggregation_measures_references.clone())
        } else {
            node
        }
    }

    fn final_measure_node_processor(&self, input: Rc<dyn SqlNode>) -> Rc<dyn SqlNode> {
        let aggregated: Rc<dyn SqlNode> = {
            let final_processor: Rc<dyn SqlNode> = FinalMeasureSqlNode::new(input.clone());
            if !self.pre_aggregation_measures_references.is_empty() {
                FinalPreAggregationMeasureSqlNode::new(
                    final_processor,
                    self.pre_aggregation_measures_references.clone(),
                )
            } else {
                final_processor
            }
        };
        let rolling_merge = RollingWindowNode::new(input.clone(), aggregated.clone());
        let raw_value = self
            .wrap_ungrouped_pre_aggregation_measure(UngroupedMeasureSqlNode::new(input.clone()));
        let ungrouped_final = self
            .wrap_ungrouped_pre_aggregation_measure(UngroupedQueryFinalMeasureSqlNode::new(input));
        MeasureRenderModifierSqlNode::new(aggregated, rolling_merge, raw_value, ungrouped_final)
    }

    /// A stored column holds the raw value, so a masked member must be
    /// masked before its column is substituted, not inside the evaluation
    /// the substitution skips.
    fn pre_aggregation_dimension_columns(
        &self,
        input: Rc<dyn SqlNode>,
        skip_masking: bool,
        unmasked_root: Option<Rc<dyn SqlNode>>,
    ) -> Rc<dyn SqlNode> {
        MaskedSqlNode::new(
            PreAggregationColumnSqlNode::new(
                input,
                self.pre_aggregation_dimensions_references.clone(),
            ),
            false,
            self.group_by_members.clone(),
            skip_masking,
            unmasked_root,
        )
    }

    fn dimension_processor(
        &self,
        input: Rc<dyn SqlNode>,
        skip_masking: bool,
        unmasked_root: Option<Rc<dyn SqlNode>>,
    ) -> Rc<dyn SqlNode> {
        let input = if !self.pre_aggregation_dimensions_references.is_empty() {
            self.pre_aggregation_dimension_columns(input, skip_masking, unmasked_root)
        } else {
            let input: Rc<dyn SqlNode> = GeoDimensionSqlNode::new(input);
            let input: Rc<dyn SqlNode> = CaseSqlNode::new(input);
            input
        };

        let input: Rc<dyn SqlNode> =
            AutoPrefixSqlNode::new(input, self.cube_name_references.clone());

        let input: Rc<dyn SqlNode> = ParenthesizeSqlNode::new(input);

        let input: Rc<dyn SqlNode> =
            TimeDimensionNode::new(self.pre_aggregation_dimensions_references.clone(), input);

        let calendar_time_shifts = self.rendered_calendar_time_shifts();
        let input = if !calendar_time_shifts.is_empty() {
            CalendarTimeShiftSqlNode::new(calendar_time_shifts, input)
        } else {
            input
        };

        let time_shifts = self.rendered_time_shifts();
        let input = if !time_shifts.is_empty() {
            TimeShiftSqlNode::new(
                time_shifts,
                self.pre_aggregation_dimensions_references.clone(),
                input,
            )
        } else {
            input
        };

        input
    }

    fn time_dimension_processor(&self, input: Rc<dyn SqlNode>) -> Rc<dyn SqlNode> {
        let input: Rc<dyn SqlNode> =
            TimeDimensionNode::new(self.pre_aggregation_dimensions_references.clone(), input);

        input
    }
}
