use super::{MultiStageQueryPlanner, PlanningScope, StageRef, StageRegistry, TimeSeriesEntry};
use crate::logical_plan::*;
use crate::planner::planners::SimpleQueryPlanner;
use crate::planner::state::State;
use crate::planner::GranularityHelper;
use crate::planner::InputId;
use crate::planner::MemberSymbol;
use crate::planner::QueryProperties;
use crate::planner::TimeDimensionSymbol;
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Plans the time series a rolling window walks over, and the query
/// resolving its date range when the query gives none.
pub(super) struct TimeSeriesStagePlanner<'a> {
    planner: &'a MultiStageQueryPlanner,
    query_tools: Rc<State>,
}

impl<'a> TimeSeriesStagePlanner<'a> {
    pub(super) fn new(planner: &'a MultiStageQueryPlanner) -> Self {
        Self {
            query_tools: planner.query_tools().clone(),
            planner,
        }
    }

    /// Adds (or reuses) the `time_series_get_range` leaf subquery — used
    /// by `plan` when the requested time dimension has no
    /// explicit date range and the planner needs to compute one.
    fn add_time_series_get_range_query(
        &self,
        time_dimension: Rc<MemberSymbol>,
        state: Rc<QueryProperties>,
        registry: &mut StageRegistry,
        scope: &mut PlanningScope,
    ) -> Result<StageRef, CubeError> {
        let member = time_dimension.as_time_dimension()?.resolved_base_id();
        if let Some(stage) = registry.date_range_query(&member) {
            return Ok(stage.clone());
        }
        let id = scope.next_input_id();
        let alias = match registry.date_range_query_count() {
            0 => "time_series_get_range".to_string(),
            count => format!("time_series_get_range_{}", count),
        };
        let stage = StageRef::new(id, alias.clone(), &state, &time_dimension, false, &[]);
        let subquery =
            self.plan_time_series_get_range_query(id, alias, time_dimension.clone(), scope)?;
        registry.add_date_range_query(member, stage.clone());
        self.planner.add_stage(
            &time_dimension,
            state,
            stage,
            subquery,
            true,
            registry,
            scope,
        )
    }

    /// Adds (or reuses) the `time_series` leaf subquery that drives a
    /// rolling window. When the time dimension has no `date_range`,
    /// also arranges for a sibling `time_series_get_range` subquery to
    /// feed it.
    pub(super) fn plan(
        &self,
        time_dimension: Rc<MemberSymbol>,
        calendar_period_granularity: Option<String>,
        state: Rc<QueryProperties>,
        registry: &mut StageRegistry,
        scope: &mut PlanningScope,
    ) -> Result<StageRef, CubeError> {
        // Windows share a series only when they walk the same dimension over the
        // same date range and granularity: an include or a stage can change any of them.
        let series_key = |symbol: Rc<MemberSymbol>| -> Result<_, CubeError> {
            let symbol = symbol.as_time_dimension()?;
            Ok((
                symbol.resolved_base_id(),
                symbol.date_range_vec(),
                symbol.granularity().clone(),
            ))
        };
        let key = series_key(time_dimension.clone())?;
        let mut existing = None;
        for (index, series) in registry.time_series().iter().enumerate() {
            if series_key(series.time_dimension.clone())? == key {
                existing = Some(index);
                break;
            }
        }
        if let Some(index) = existing {
            // A window joining an existing series has to register its own
            // boundary column on it, and the series is already planned without it.
            let series = registry.time_series_mut(index);
            if let Some(granularity) = &calendar_period_granularity {
                if !series.calendar_period_granularities.contains(granularity) {
                    series
                        .calendar_period_granularities
                        .push(granularity.clone());
                    let series = series.clone();
                    scope.replan_member(series.stage.id(), |scope| {
                        self.plan_time_series_query(&series, scope)
                    })?;
                }
            }
            return Ok(registry.time_series()[index].stage.clone());
        }
        let series_count = registry.time_series().len();
        let get_range_query = if time_dimension
            .as_time_dimension()?
            .date_range_vec()
            .is_some()
        {
            None
        } else {
            Some(self.add_time_series_get_range_query(
                time_dimension.clone(),
                state.clone(),
                registry,
                scope,
            )?)
        };
        let id = scope.next_input_id();
        let alias = if series_count == 0 {
            "time_series".to_string()
        } else {
            format!("time_series_{}", series_count)
        };
        let series = TimeSeriesEntry {
            stage: StageRef::new(id, alias, &state, &time_dimension, false, &[]),
            time_dimension: time_dimension.clone(),
            date_range_subquery: get_range_query.map(|d| {
                Rc::new(
                    SubqueryRef::builder()
                        .id(d.id())
                        .name(d.name().clone())
                        .schema(LogicalSchema::default().into_rc())
                        .build(),
                )
            }),
            calendar_period_granularities: calendar_period_granularity.into_iter().collect(),
        };
        let subquery = self.plan_time_series_query(&series, scope)?;
        let stage = series.stage.clone();
        registry.add_time_series(series);
        self.planner.add_stage(
            &time_dimension,
            state,
            stage,
            subquery,
            true,
            registry,
            scope,
        )
    }

    /// Builds the leaf `GetDateRange` subquery used when a rolling-window
    /// time dimension has no explicit date range — runs a
    /// `SimpleQueryPlanner` to compute the actual bounds at query
    /// time.
    fn plan_time_series_get_range_query(
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
    fn plan_time_series_query(
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
}
