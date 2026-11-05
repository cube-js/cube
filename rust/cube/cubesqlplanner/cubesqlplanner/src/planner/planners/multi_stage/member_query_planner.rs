use super::{
    EvaluationContext, MultiStageInodeMember, MultiStageInodeMemberType, PlanningScope,
    RollingWindowDescription, StageRef, TimeSeriesEntry,
};
use crate::logical_plan::transforms as logical_transforms;
use crate::logical_plan::*;
use crate::planner::planners::{multi_stage::RollingWindowType, QueryPlanner, SimpleQueryPlanner};
use crate::planner::state::State;
use crate::planner::symbols::transforms;
use crate::planner::GranularityHelper;
use crate::planner::InputId;
use crate::planner::TimeDimensionSymbol;
use crate::planner::{MeasureRenderModifier, MemberSymbol};
use crate::planner::{OrderByItem, QueryProperties};

use cubenativeutils::CubeError;
use itertools::Itertools;
use std::rc::Rc;
use std::vec;

/// Builds the `Subquery` of one multi-stage stage: a rolling window, a
/// dimension or measure calculation, or a leaf (base measure, time series,
/// time-series date range).
pub struct MultiStageMemberQueryPlanner {
    query_tools: Rc<State>,
    query_properties: Rc<QueryProperties>,
}

impl MultiStageMemberQueryPlanner {
    pub fn new(query_tools: Rc<State>, query_properties: Rc<QueryProperties>) -> Self {
        Self {
            query_tools,
            query_properties,
        }
    }

    /// Builds the leaf `GetDateRange` subquery used when a rolling-window
    /// time dimension has no explicit date range — runs a
    /// `SimpleQueryPlanner` to compute the actual bounds at query
    /// time.
    pub fn plan_time_series_get_range_query(
        &self,
        id: InputId,
        alias: String,
        time_dimension: Rc<MemberSymbol>,
        scope: &mut PlanningScope,
    ) -> Result<Rc<Subquery>, CubeError> {
        let subquery_properties = QueryProperties::builder()
            .query_tools(self.query_tools.clone())
            .time_dimensions(vec![time_dimension.clone()])
            .order_by(QueryProperties::default_order(
                &[],
                std::slice::from_ref(&time_dimension),
                &[],
            ))
            .ignore_cumulative(true)
            .ungrouped(true)
            .build()?;

        let simple_query_planer =
            SimpleQueryPlanner::try_new_analyzed(self.query_tools.clone(), subquery_properties)?;

        let source = simple_query_planer.source_and_subquery_dimensions(scope)?;

        let result = MultiStageGetDateRange {
            time_dimension: time_dimension.clone(),
            source,
        };
        let member = Subquery::new(
            id,
            alias,
            SubqueryBody::MultiStageGetDateRange(Rc::new(result)),
        );

        Ok(Rc::new(member))
    }

    /// Builds the leaf `TimeSeries` subquery — the date axis a rolling
    /// window walks over. References a sibling `GetDateRange` subquery
    /// for its bounds when the time dimension has no explicit
    /// `date_range`.
    pub fn plan_time_series_query(
        &self,
        time_series: &TimeSeriesEntry,
        scope: &mut PlanningScope,
    ) -> Result<Rc<Subquery>, CubeError> {
        let time_dimension = time_series.time_dimension.clone();
        let period_dimensions = self.calendar_period_dimensions(time_series)?;
        let calendar_source = if period_dimensions.is_empty() {
            None
        } else {
            Some(self.plan_calendar_period_source(&time_dimension, &period_dimensions, scope)?)
        };
        let result = MultiStageTimeSeries::builder()
            .name(time_series.stage.name().clone())
            .time_dimension(time_dimension.clone())
            .date_range(time_dimension.as_time_dimension()?.date_range_vec())
            .get_date_range_multistage_ref(time_series.date_range_subquery.clone())
            .calendar_source(calendar_source)
            .period_dimensions(period_dimensions)
            .build();
        Ok(Rc::new(Subquery::new(
            time_series.stage.id(),
            time_series.stage.name().clone(),
            SubqueryBody::MultiStageTimeSeries(Rc::new(result)),
        )))
    }

    /// The series time dimension re-granularized to each calendar granularity
    /// a `to_date` window on this series bounds itself by.
    fn calendar_period_dimensions(
        &self,
        time_series: &TimeSeriesEntry,
    ) -> Result<Vec<Rc<MemberSymbol>>, CubeError> {
        let time_dimension = time_series.time_dimension.as_time_dimension()?;
        let granularities = &time_series.calendar_period_granularities;

        let evaluator_compiler_cell = self.query_tools.compiler().clone();
        let mut evaluator_compiler = evaluator_compiler_cell.borrow_mut();

        granularities
            .iter()
            .map(|granularity| {
                let granularity_obj = GranularityHelper::make_granularity_obj(
                    self.query_tools.cube_evaluator().clone(),
                    &mut evaluator_compiler,
                    time_dimension.base_symbol(),
                    Some(granularity.clone()),
                )?;
                Ok(MemberSymbol::new_time_dimension(TimeDimensionSymbol::new(
                    time_dimension.base_symbol().clone(),
                    Some(granularity.clone()),
                    granularity_obj,
                    time_dimension
                        .date_range_vec()
                        .map(|range| (range[0].clone(), range[1].clone())),
                )))
            })
            .collect()
    }

    /// A query over the calendar cube pairing every point of the series with
    /// the period it falls into, so a `to_date` window can bound itself by the
    /// calendar instead of by interval math.
    fn plan_calendar_period_source(
        &self,
        time_dimension: &Rc<MemberSymbol>,
        period_dimensions: &[Rc<MemberSymbol>],
        scope: &mut PlanningScope,
    ) -> Result<Rc<Query>, CubeError> {
        // The series may already be granularized to the period a window bounds
        // itself by; projecting it twice makes the column ambiguous.
        let mut time_dimensions = vec![time_dimension.clone()];
        for period_dimension in period_dimensions {
            if !time_dimensions
                .iter()
                .any(|dimension| dimension.id() == period_dimension.id())
            {
                time_dimensions.push(period_dimension.clone());
            }
        }

        // Left unfiltered on purpose: the series restricts itself to the query
        // range only after it has read each period's end off the next point.
        let subquery_properties = QueryProperties::builder()
            .query_tools(self.query_tools.clone())
            .order_by(QueryProperties::default_order(&[], &time_dimensions, &[]))
            .time_dimensions(time_dimensions)
            .ignore_cumulative(true)
            .build()?;

        SimpleQueryPlanner::try_new_analyzed(self.query_tools.clone(), subquery_properties)?
            .plan(scope)
    }

    /// Builds the rolling-window subquery that combines a time-series
    /// input with a measure input, dispatching on
    /// `RollingWindowDescription` into the regular / to-date /
    /// running-total variant.
    pub fn plan_rolling_window_query(
        &self,
        id: InputId,
        alias: String,
        member: &Rc<MemberSymbol>,
        state: &QueryProperties,
        is_ungrouped: bool,
        rolling_window_desc: &RollingWindowDescription,
        time_series_input: &StageRef,
        measure_input: &StageRef,
    ) -> Result<Rc<Subquery>, CubeError> {
        let rolling_window = match &rolling_window_desc.rolling_window {
            RollingWindowType::Regular(regular_rolling_window) => {
                MultiStageRollingWindowType::Regular(MultiStageRegularRollingWindow {
                    trailing: regular_rolling_window.trailing.clone(),
                    leading: regular_rolling_window.leading.clone(),
                    offset: regular_rolling_window.offset.clone(),
                })
            }
            RollingWindowType::ToDate(to_date_rolling_window) => {
                let time_dimension = &rolling_window_desc.time_dimension;
                let query_granularity = to_date_rolling_window.granularity.clone();

                let evaluator_compiler_cell = self.query_tools.compiler().clone();
                let mut evaluator_compiler = evaluator_compiler_cell.borrow_mut();

                let Some(granularity_obj) = GranularityHelper::make_granularity_obj(
                    self.query_tools.cube_evaluator().clone(),
                    &mut evaluator_compiler,
                    time_dimension,
                    Some(query_granularity.clone()),
                )?
                else {
                    return Err(CubeError::internal(format!(
                        "Rolling window granularity '{}' is not found in time dimension '{}'",
                        query_granularity,
                        time_dimension.name()
                    )));
                };

                MultiStageRollingWindowType::ToDate(MultiStageToDateRollingWindow {
                    granularity_obj: Rc::new(granularity_obj),
                })
            }
        };

        // The subquery must project the dimensions of the state this rolling
        // window is requested at, not the root query dimensions: a parent
        // inode can widen the grain (`grain.include`, a `case` switch
        // dimension) and consumers resolve those dimensions — e.g. as
        // `partition_by` of the enclosing window function — against this
        // subquery's schema.
        let schema = LogicalSchema::default()
            .set_dimensions(state.dimensions().clone())
            .set_time_dimensions(state.time_dimensions().clone())
            .set_measures(vec![member.clone()])
            .into_rc();

        let result = MultiStageRollingWindow {
            schema,
            is_ungrouped,
            rolling_window,
            order_by: query_order_by(member, state),
            time_series_input: time_series_input.subquery().as_ref().clone(),
            measure_input: measure_input.subquery().as_ref().clone(),
            rolling_time_dimension: rolling_window_desc.time_dimension.clone(),
            time_dimension_in_measure_input: rolling_window_desc.base_time_dimension.clone(),
        };
        Ok(Rc::new(Subquery::new(
            id,
            alias,
            SubqueryBody::MultiStageRollingWindow(Rc::new(result)),
        )))
    }

    /// Builds a measure-calculation subquery (Rank / Aggregate /
    /// Calculate). Wires the input subqueries into a `Merge`
    /// source; for the JOIN-based path (when the stage has keys inputs)
    /// also wires keys-side refs.
    pub fn plan_for_measure_calculation(
        &self,
        id: InputId,
        alias: String,
        member: &Rc<MemberSymbol>,
        state: &QueryProperties,
        is_ungrouped: bool,
        multi_stage_member: &MultiStageInodeMember,
        inputs: &[StageRef],
        keys_inputs: &[StageRef],
    ) -> Result<Rc<Subquery>, CubeError> {
        let partition_by = multi_stage_member.grain().partition(&all_dimensions(state));

        // Rank always uses a window function. Aggregate inodes are
        // routed through `Merge` by default; only the narrow
        // optimisation-eligible subset (planner sets `use_window_path`)
        // is emitted as a Window expression and additionally requires
        // partition_by to be a strict subset of all dimensions —
        // otherwise the window collapses into a plain group-by.
        let window_function_to_use = match multi_stage_member.inode_type() {
            MultiStageInodeMemberType::Rank => MultiStageCalculationWindowFunction::Rank,
            MultiStageInodeMemberType::Aggregate
                if multi_stage_member.use_window_path()
                    && partition_by.len() != all_dimensions(state).len() =>
            {
                MultiStageCalculationWindowFunction::Window
            }
            _ => MultiStageCalculationWindowFunction::None,
        };

        let measures = if member.is_measure() {
            vec![member.clone()]
        } else {
            vec![]
        };
        let schema = LogicalSchema::default()
            .set_dimensions(state.dimensions().clone())
            .set_time_dimensions(state.time_dimensions().clone())
            .set_measures(measures)
            .into_rc();

        let calculation_type = match multi_stage_member.inode_type() {
            MultiStageInodeMemberType::Rank => MultiStageCalculationType::Rank,
            MultiStageInodeMemberType::Aggregate => MultiStageCalculationType::Aggregate,
            MultiStageInodeMemberType::Calculate => MultiStageCalculationType::Calculate,
            _ => {
                return Err(CubeError::internal(format!(
                    "Wrong inode type for measure calculation"
                )))
            }
        };

        let result = MultiStageMeasureCalculation::builder()
            .schema(schema)
            .is_ungrouped(is_ungrouped)
            .calculation_type(calculation_type)
            .partition_by(partition_by)
            .window_function_to_use(window_function_to_use)
            .order_by(query_order_by(member, state))
            .source(Rc::new(
                Merge::builder()
                    .schema(input_schema(inputs))
                    .subqueries(input_subqueries(inputs))
                    .keys(input_subqueries(keys_inputs))
                    .build(),
            ))
            .build();

        let result = Subquery::new(
            id,
            alias,
            SubqueryBody::MultiStageMeasureCalculation(Rc::new(result)),
        );
        Ok(Rc::new(result))
    }

    /// Builds a dimension-calculation subquery for a multi-stage
    /// dimension. Includes the dimension itself plus every
    /// non-multi-stage dimension reachable from the subtree, so the
    /// resulting subquery can be joined back into the host query on
    /// those dimensions.
    pub fn plan_for_dimension_calculation(
        &self,
        stage: &StageRef,
        member: &Rc<MemberSymbol>,
        state: &QueryProperties,
        inputs: &[StageRef],
    ) -> Result<Rc<Subquery>, CubeError> {
        let mut dimensions = state.dimensions().clone();
        let mut time_dimensions = state.time_dimensions().clone();
        let mut measures = vec![];
        add_member_by_kind(member, &mut dimensions, &mut time_dimensions, &mut measures);
        // We add all non–multi-stage dimensions from the underlying states because
        // they’re needed to join a multi-stage dimension into the measure query
        let (all_dependend_dimensions, all_dependend_time_dimensions) =
            stage.non_multi_stage_dimensions()?;
        dimensions.extend(all_dependend_dimensions.iter().cloned());
        time_dimensions.extend(all_dependend_time_dimensions.iter().cloned());
        dimensions = dimensions
            .into_iter()
            .unique_by(|d| d.id().clone())
            .collect_vec();
        time_dimensions = time_dimensions
            .into_iter()
            .unique_by(|d| d.id().clone())
            .collect_vec();

        let schema = LogicalSchema::default()
            .set_dimensions(dimensions)
            .set_time_dimensions(time_dimensions)
            .set_measures(measures)
            .into_rc();

        let result = MultiStageDimensionCalculation::builder()
            .schema(schema)
            .order_by(query_order_by(member, state))
            .multi_stage_dimension(member.clone())
            .source(Rc::new(
                Merge::builder()
                    .schema(input_schema(inputs))
                    .subqueries(input_subqueries(inputs))
                    .build(),
            ))
            .build();

        let result = Subquery::new(
            stage.id(),
            stage.name().clone(),
            SubqueryBody::MultiStageDimensionCalculation(Rc::new(result)),
        );
        Ok(Rc::new(result))
    }

    /// Builds the leaf subquery for a base measure — runs a `QueryPlanner`
    /// on the stage's state with `allow_multi_stage = false`,
    /// then wraps the result in a leaf subquery. The shared
    /// `scope` receives any subqueries the leaf planning produces
    /// (multiplied-measure subqueries), so they land in the root
    /// `WITH` list instead of nesting. `without_member` selects only the
    /// dimension grid, for cases like `Rank` that compute nothing in the leaf.
    /// `has_aggregates_on_top` materializes mergeable state for an outer
    /// rolling window.
    pub fn plan_for_leaf_query(
        &self,
        id: InputId,
        alias: String,
        member: &Rc<MemberSymbol>,
        state: &QueryProperties,
        is_ungrouped: bool,
        has_aggregates_on_top: bool,
        without_member: bool,
        scope: &mut PlanningScope,
    ) -> Result<Rc<Subquery>, CubeError> {
        // An aggregating stage on top (a rolling window) merges the leaf's
        // values, so measures with a mergeable state form must materialize
        // the state, not the final value.
        let leaf_as_state = has_aggregates_on_top;
        let member_node = if leaf_as_state {
            transforms::measures_as_state(member)?
        } else {
            member.clone()
        };
        let member_node = &member_node;
        let mut dimensions = state.dimensions().clone();
        let mut time_dimensions = state.time_dimensions().clone();
        let mut measures = vec![];
        if !without_member {
            add_member_by_kind(
                member_node,
                &mut dimensions,
                &mut time_dimensions,
                &mut measures,
            );
        }

        let mut measures_filters = state.measures_filters().clone();
        if leaf_as_state {
            for filter_item in measures_filters.iter_mut() {
                *filter_item = transforms::map_filter_item_symbols(
                    filter_item,
                    &transforms::measures_as_state,
                )?;
            }
        }
        let order_by = QueryProperties::default_order(&dimensions, &time_dimensions, &measures);
        let subquery_properties = QueryProperties::builder()
            .query_tools(self.query_tools.clone())
            .measures(measures)
            .dimensions(dimensions)
            .time_dimensions(time_dimensions)
            .order_by(order_by)
            .time_dimensions_filters(state.time_dimensions_filters().clone())
            .dimensions_filters(state.dimensions_filters().clone())
            .measures_filters(measures_filters)
            .segments(state.segments().clone())
            .ignore_cumulative(true)
            .ungrouped(is_ungrouped)
            .query_join_hints(self.query_properties.query_join_hints().clone())
            .allow_multi_stage(false)
            .build()?;

        let query_planner =
            QueryPlanner::new(subquery_properties.clone(), self.query_tools.clone());
        // Subqueries hoisted out of this leaf (e.g. multiplied-measure
        // subqueries) must render under the same context the leaf
        // itself renders with.
        let evaluation_context = EvaluationContext {
            time_shifts: state.time_shifts().clone(),
            measure_for_ungrouped: is_ungrouped,
        };
        let query = scope.with_evaluation_context(evaluation_context.clone(), |scope| {
            query_planner.plan(scope)
        })?;
        // An ungrouped leaf feeds a stage that aggregates its rows, so its
        // measures render as row-level values.
        let query = if evaluation_context.measure_for_ungrouped {
            logical_transforms::measures_render_modifier_in_query(
                &query,
                &MeasureRenderModifier::RawValue,
            )?
        } else {
            query
        };
        let result = Subquery::new(
            id,
            alias,
            SubqueryBody::Query(query.with_evaluation_context(Some(evaluation_context))),
        );
        Ok(Rc::new(result))
    }
}

fn all_dimensions(state: &QueryProperties) -> Vec<Rc<MemberSymbol>> {
    state
        .dimensions()
        .iter()
        .cloned()
        .chain(state.time_dimensions().iter().cloned())
        .collect_vec()
}

fn input_schema(inputs: &[StageRef]) -> Rc<LogicalSchema> {
    let dimensions = inputs
        .iter()
        .flat_map(|input| input.schema().dimensions.iter().cloned())
        .unique_by(|dim| dim.id().clone())
        .collect_vec();
    let time_dimensions = inputs
        .iter()
        .flat_map(|input| input.schema().time_dimensions.iter().cloned())
        .unique_by(|dim| dim.id().clone())
        .collect_vec();

    LogicalSchema::default()
        .set_dimensions(dimensions)
        .set_time_dimensions(time_dimensions)
        .into_rc()
}

fn input_subqueries(inputs: &[StageRef]) -> Vec<Rc<SubqueryRef>> {
    inputs
        .iter()
        .map(|input| input.subquery().clone())
        .unique_by(|r| r.id())
        .collect_vec()
}

fn query_order_by(member: &Rc<MemberSymbol>, state: &QueryProperties) -> Vec<OrderByItem> {
    let measures = if member.as_measure().is_ok() {
        vec![member.clone()]
    } else {
        vec![]
    };

    QueryProperties::default_order(&state.dimensions(), &state.time_dimensions(), &measures)
}

/// Adds `member` to the schema list of its kind, skipping a dimension already
/// present under another name of the same reference chain.
fn add_member_by_kind(
    member: &Rc<MemberSymbol>,
    dimensions: &mut Vec<Rc<MemberSymbol>>,
    time_dimensions: &mut Vec<Rc<MemberSymbol>>,
    measures: &mut Vec<Rc<MemberSymbol>>,
) {
    let same_chain = |d: &Rc<MemberSymbol>| {
        d.clone().resolve_reference_chain() == member.clone().resolve_reference_chain()
    };
    match member.as_ref() {
        MemberSymbol::Dimension(_) => {
            if !dimensions.iter().any(same_chain) {
                dimensions.push(member.clone())
            }
        }
        MemberSymbol::TimeDimension(_) => {
            if !time_dimensions.iter().any(same_chain) {
                time_dimensions.push(member.clone())
            }
        }
        MemberSymbol::Measure(_) => measures.push(member.clone()),
        MemberSymbol::Ref(_) => {
            if member.is_measure() {
                measures.push(member.clone())
            } else if member.is_dimension() && !dimensions.iter().any(same_chain) {
                dimensions.push(member.clone())
            }
        }
        MemberSymbol::MemberExpression(_) => {}
    }
}
