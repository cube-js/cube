use super::leaf::LeafStagePlanner;
use super::time_series::TimeSeriesStagePlanner;
use super::{MultiStageQueryPlanner, PlanningScope, StageRef, StageRegistry};
use crate::cube_bridge::base_query_options::FilterValue;
use crate::cube_bridge::measure_definition::RollingWindow;
use crate::logical_plan::*;
use crate::planner::collectors::member_childs;
use crate::planner::filter::FilterItem;
use crate::planner::filter::FilterOperator;
use crate::planner::state::State;
use crate::planner::symbols::transforms;
use crate::planner::symbols::TimeDimensionSymbol;
use crate::planner::GranularityHelper;
use crate::planner::InputId;
use crate::planner::MemberSymbol;
use crate::planner::QueryDateTimeHelper;
use crate::planner::QueryProperties;
use crate::planner::QueryTimeSeries;
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::rc::Rc;

use super::stage::query_order_by;

/// Bounds of a regular rolling window: trailing / leading interval
/// strings and a time-series offset.
#[derive(Clone)]
pub struct RegularRollingWindow {
    pub trailing: Option<String>,
    pub leading: Option<String>,
    pub offset: String,
}

/// To-date rolling window — accumulates since the start of
/// `granularity` (month-to-date, year-to-date, …).
#[derive(Clone)]
pub struct ToDateRollingWindow {
    pub granularity: String,
}

/// Flavour of rolling-window computation: regular trailing/leading
/// window or a to-date window.
#[derive(Clone)]
pub enum RollingWindowType {
    Regular(RegularRollingWindow),
    ToDate(ToDateRollingWindow),
}

/// Planner-side description of a rolling window: the time
/// dimension used for windowing (and its lower-granularity base
/// version produced in the leaf subquery) plus the chosen window type.
#[derive(Clone)]
pub struct RollingWindowDescription {
    pub time_dimension: Rc<MemberSymbol>,
    pub base_time_dimension: Rc<MemberSymbol>,
    pub rolling_window: RollingWindowType,
}

impl RollingWindowDescription {
    pub fn new_regular(
        time_dimension: Rc<MemberSymbol>,
        base_time_dimension: Rc<MemberSymbol>,
        trailing: Option<String>,
        leading: Option<String>,
        offset: String,
    ) -> Self {
        let regular_window = RegularRollingWindow {
            trailing,
            leading,
            offset,
        };
        Self {
            time_dimension,
            base_time_dimension,
            rolling_window: RollingWindowType::Regular(regular_window),
        }
    }

    pub fn new_to_date(
        time_dimension: Rc<MemberSymbol>,
        base_time_dimension: Rc<MemberSymbol>,
        granularity: String,
    ) -> Self {
        Self {
            time_dimension,
            base_time_dimension,
            rolling_window: RollingWindowType::ToDate(ToDateRollingWindow { granularity }),
        }
    }
}

/// Plans a cumulative measure: a rolling window over a time series and the
/// base values it accumulates.
pub(super) struct RollingWindowStagePlanner<'a> {
    planner: &'a MultiStageQueryPlanner,
    query_tools: Rc<State>,
    query_properties: Rc<QueryProperties>,
}

impl<'a> RollingWindowStagePlanner<'a> {
    pub(super) fn new(planner: &'a MultiStageQueryPlanner) -> Self {
        Self {
            query_tools: planner.query_tools().clone(),
            query_properties: planner.query_properties().clone(),
            planner,
        }
    }

    /// If `member` is a cumulative measure, plans the time-series
    /// and rolling-window subqueries and returns the rolling-window
    /// stage. Returns `None` for other measures and for
    /// non-measure members.
    pub(super) fn try_plan(
        &self,
        member: Rc<MemberSymbol>,
        state: Rc<QueryProperties>,
        registry: &mut StageRegistry,
        scope: &mut PlanningScope,
    ) -> Result<Option<StageRef>, CubeError> {
        if let Ok(measure) = member.as_measure() {
            if measure.is_cumulative() {
                // A rollup stores a rolling measure without its window and queries
                // apply the window over it, so it is built unrolled.
                if measure.is_multi_stage() && self.query_properties.is_pre_aggregation_query() {
                    return Ok(Some(self.planner.plan_member(
                        MemberSymbol::new_measure(transforms::strip_rolling_window(&measure)),
                        state,
                        registry,
                        scope,
                    )?));
                }
                let rolling_window = if let Some(rolling_window) = measure.rolling_window() {
                    rolling_window.clone()
                } else {
                    RollingWindow {
                        trailing: None,
                        leading: None,
                        offset: None,
                        rolling_type: None,
                        granularity: None,
                    }
                };

                if !measure.is_multi_stage() {
                    let childs = member_childs(&member, true)?;
                    let measures = childs
                        .iter()
                        .filter(|s| s.as_measure().is_ok())
                        .collect_vec();
                    if !measures.is_empty() {
                        return Err(CubeError::user(
                            format!("Measure {} references another measures ({}). In this case, {} must have multi_stage: true defined",
                            member.full_name(),
                            measures.into_iter().map(|m| m.full_name()).join(", "),
                            member.full_name(),
                                        ),
                        ));
                    }
                }

                let grain = measure
                    .multi_stage()
                    .map(|ms| ms.grain.clone())
                    .unwrap_or_default();
                let grain_include = grain.include.clone().unwrap_or_default();

                let ungrouped = measure.is_rolling_window() && !measure.is_additive();

                let mut time_dimensions = state
                    .time_dimensions()
                    .iter()
                    .map(|d| d.as_time_dimension())
                    .collect::<Result<Vec<_>, _>>()?;
                for dim in state.dimensions() {
                    let dim = dim.clone().resolve_reference_chain();
                    if let Ok(time_dimension) = dim.as_time_dimension() {
                        time_dimensions.push(with_state_date_range(time_dimension, &state)?);
                    }
                }

                let base_member = MemberSymbol::new_measure(transforms::unroll_rolling(&measure));

                if time_dimensions.is_empty() {
                    let base_state =
                        self.replace_date_range_for_rolling_window(&rolling_window, state.clone())?;
                    let rolling_base = if !measure.is_multi_stage() {
                        LeafStagePlanner::new(self.planner).plan_rolling_window_base(
                            base_member,
                            base_state,
                            false,
                            registry,
                            scope,
                        )?
                    } else {
                        // Without a time dimension there is no series to walk,
                        // so the window collapses to a single bucket: no frame
                        // to build, and no outer stage to carry the aggregation.
                        // What is left is the measure's own multi-stage
                        // definition — aggregation and grain included — over the
                        // base state prepared above.
                        self.planner.plan_member(
                            MemberSymbol::new_measure(transforms::strip_rolling_window(&measure)),
                            base_state,
                            registry,
                            scope,
                        )?
                    };
                    return Ok(Some(rolling_base));
                }
                let uniq_time_dimensions = time_dimensions
                    .iter()
                    .unique_by(|a| {
                        (
                            a.base_symbol()
                                .clone()
                                .resolve_reference_chain()
                                .id()
                                .clone(),
                            a.date_range_vec(),
                        )
                    })
                    .collect_vec();
                if uniq_time_dimensions.len() != 1 {
                    return Err(CubeError::internal(
                        "Rolling window requires one time dimension and equal date ranges"
                            .to_string(),
                    ));
                }

                let time_dimension =
                    GranularityHelper::find_dimension_with_min_granularity(&time_dimensions)?;
                let time_dimension = MemberSymbol::new_time_dimension(time_dimension);

                // Of the grain keys only `include` reaches the window assembly.
                // It extends the grain the values inside a bucket are computed
                // at, which the base subquery carries anyway. `exclude` and
                // `keep_only` narrow the grain the value is *reported* at, and a
                // narrowed value has to be broadcast back onto the query grid;
                // the rolling window node has no side enumerating that grid, so
                // there is nothing to broadcast from.
                //
                // What is rejected is the narrowing actually happening, not the
                // keys being declared: `exclude` of a member this grain does not
                // carry subtracts nothing, and `keep_only` listing everything the
                // query groups by intersects to the same list. Such a key costs
                // the query nothing, and the value is the one the measure would
                // have without it. `MultiStageGrain::partition` only ever removes, so a
                // shorter list is exactly the case that has no answer here.
                //
                // The check sits below the branch above on purpose. Without a
                // time dimension the window has no frame and the measure is
                // planned through the ordinary multi-stage path, which narrows
                // the grain and broadcasts it back the usual way.
                let narrows = grain.partition(state.dimensions()).len() != state.dimensions().len()
                    || grain.partition(state.time_dimensions()).len()
                        != state.time_dimensions().len();
                if narrows {
                    return Err(CubeError::user(format!(
                        "Measure {} narrows the grain of this query through `grain.exclude` / \
                         `reduce_by` or `grain.keep_only` / `group_by` while also declaring a \
                         `rolling_window` over {}, which is not supported. Drop the narrowing \
                         keys, drop the window, or query the measure without a time dimension.",
                        member.full_name(),
                        time_dimension.full_name(),
                    )));
                }

                let (base_rolling_state, base_time_dimension) = self.make_rolling_base_state(
                    time_dimension.clone(),
                    &rolling_window,
                    state.clone(),
                )?;

                // `grain.include` extends the grain the values inside the window
                // are computed at. The frame still keys off the base time
                // dimension, so the extension only splits rows the outer
                // aggregation merges back together.
                let base_rolling_state = if grain_include.is_empty() {
                    base_rolling_state
                } else {
                    let mut extended = base_rolling_state.as_ref().clone();
                    extended.add_dimensions(grain_include);
                    Rc::new(extended)
                };

                let calendar_period_granularity = self
                    .planner
                    .bounds()
                    .calendar_to_date_granularity(&rolling_window, &time_dimension)?;

                let time_series = TimeSeriesStagePlanner::new(self.planner).plan(
                    time_dimension.clone(),
                    calendar_period_granularity,
                    state.clone(),
                    registry,
                    scope,
                )?;

                let rolling_base = if !measure.is_multi_stage() {
                    LeafStagePlanner::new(self.planner).plan_rolling_window_base(
                        base_member,
                        base_rolling_state,
                        ungrouped,
                        registry,
                        scope,
                    )?
                } else {
                    self.planner
                        .plan_member(base_member, base_rolling_state, registry, scope)?
                };

                let input = vec![time_series, rolling_base];

                let (id, alias) = scope.next_subquery();

                let rolling_window_descr = if let Some(granularity) = self
                    .planner
                    .bounds()
                    .get_to_date_rolling_granularity(&rolling_window)?
                {
                    RollingWindowDescription::new_to_date(
                        time_dimension,
                        base_time_dimension,
                        granularity,
                    )
                } else {
                    RollingWindowDescription::new_regular(
                        time_dimension,
                        base_time_dimension,
                        rolling_window.trailing.clone(),
                        rolling_window.leading.clone(),
                        rolling_window.offset.clone().unwrap_or("end".to_string()),
                    )
                };

                let stage = StageRef::new(id, alias.clone(), &state, &member, false, &input);
                let subquery = self.plan_rolling_window_query(
                    id,
                    alias,
                    &member,
                    &state,
                    self.query_properties.ungrouped(),
                    &rolling_window_descr,
                    &input[0],
                    &input[1],
                )?;
                Ok(Some(self.planner.add_stage(
                    &member, state, stage, subquery, true, registry, scope,
                )?))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    /// Adjust date range filters for rolling window when there's no granularity.
    /// Without granularity there's no time_series subquery, so the InDateRange filter
    /// is rewritten into the rolling-window bounds (anchored by the window offset)
    /// applied directly to the base measure.
    fn replace_date_range_for_rolling_window(
        &self,
        rolling_window: &RollingWindow,
        state: Rc<QueryProperties>,
    ) -> Result<Rc<QueryProperties>, CubeError> {
        let mut new_state = state.as_ref().clone();
        let to_date_granularity = rolling_window
            .granularity
            .as_ref()
            .filter(|_| rolling_window.rolling_type.as_deref() == Some("to_date"));
        for filter_item in state.time_dimensions_filters() {
            if let FilterItem::Item(filter) = filter_item {
                if !matches!(filter.filter_operator(), FilterOperator::InDateRange) {
                    continue;
                }
                if let Some(granularity) = to_date_granularity {
                    // The window runs from the start of the period the range starts in.
                    let time_dimension = filter.raw_member_evaluator().as_time_dimension().ok();
                    if let (Some(time_dimension), [FilterValue::Str(from), FilterValue::Str(to)]) =
                        (time_dimension, filter.values().as_slice())
                    {
                        if let Some(from) = self.planner.bounds().to_date_period_start(
                            &time_dimension,
                            granularity,
                            from,
                        )? {
                            new_state.replace_bounds_of_date_filter(filter, from, to.clone())?;
                        }
                    }
                } else {
                    new_state.replace_date_range_for_rolling_window_without_granularity(
                        &filter.member_id(),
                        &rolling_window.trailing,
                        &rolling_window.leading,
                        rolling_window.offset.as_deref().unwrap_or("end"),
                    )?;
                }
            }
        }
        Ok(Rc::new(new_state))
    }

    /// Builds the state for a rolling-window base subquery: reduces the
    /// time dimension to the minimum granularity required by the
    /// window, drops other dimensions that resolve to time
    /// dimensions, and rewrites the time-dimension date-range
    /// filter to either a `to_date` bound or a regular trailing /
    /// leading bound.
    fn make_rolling_base_state(
        &self,
        time_dimension: Rc<MemberSymbol>,
        rolling_window: &RollingWindow,
        state: Rc<QueryProperties>,
    ) -> Result<(Rc<QueryProperties>, Rc<MemberSymbol>), CubeError> {
        let time_dimension_symbol = time_dimension.as_time_dimension()?;
        let time_dimension_base_id = time_dimension_symbol.base_symbol().id().clone();
        let mut new_state = state.as_ref().clone();
        let trailing_granularity =
            GranularityHelper::granularity_from_interval(&rolling_window.trailing);
        let leading_granularity =
            GranularityHelper::granularity_from_interval(&rolling_window.leading);
        let window_granularity =
            GranularityHelper::min_granularity(&trailing_granularity, &leading_granularity)?;
        // Base buckets must not straddle a query bucket or an interval-defined `to_date`
        // period boundary, so a custom granularity contributes the finest unit its origin needs.
        let query_granularity = time_dimension_symbol
            .granularity_obj()
            .as_ref()
            .map(|g| g.min_granularity())
            .transpose()?
            .flatten();
        let to_date_granularity = self
            .planner
            .bounds()
            .get_to_date_rolling_granularity(rolling_window)?;
        let to_date_period = match &to_date_granularity {
            Some(granularity) => self
                .planner
                .bounds()
                .to_date_period_granularity(&time_dimension_symbol, granularity)?,
            None => None,
        };
        let period_granularity = to_date_period
            .as_ref()
            .filter(|period| period.calendar_sql().is_none())
            .map(|period| period.min_granularity())
            .transpose()?
            .flatten();
        let result_granularity = GranularityHelper::min_granularity(
            &GranularityHelper::min_granularity(&window_granularity, &query_granularity)?,
            &period_granularity,
        )?;

        let new_time_dimension_symbol = time_dimension_symbol
            .change_granularity(self.query_tools.clone(), result_granularity.clone())?;
        let new_time_dimension = MemberSymbol::new_time_dimension(new_time_dimension_symbol);
        //We keep only one time_dimension in the leaf query because, even if time_dimension values have different granularity, in the leaf query we need to group by the lowest granularity.
        new_state.set_time_dimensions(vec![new_time_dimension.clone()]);

        let dimensions = new_state
            .dimensions()
            .clone()
            .into_iter()
            .filter(|d| {
                d.clone()
                    .resolve_reference_chain()
                    .as_time_dimension()
                    .is_err()
            })
            .collect_vec();
        new_state.set_dimensions(dimensions);

        if let Some(granularity) = &to_date_granularity {
            let window_bounds = match &to_date_period {
                Some(period) => self
                    .planner
                    .bounds()
                    .to_date_window_bounds(&time_dimension_symbol, period)?,
                None => None,
            };
            new_state.replace_to_date_date_range_filter(
                &time_dimension_base_id,
                granularity,
                window_bounds,
            )?;
        } else {
            new_state.replace_regular_date_range_filter(
                &time_dimension_base_id,
                rolling_window.trailing.clone(),
                rolling_window.leading.clone(),
                self.planner
                    .bounds()
                    .regular_scan_span(&time_dimension_symbol, rolling_window)?,
            )?;
        }

        Ok((Rc::new(new_state), new_time_dimension))
    }

    /// Builds the rolling-window subquery that combines a time-series
    /// input with a measure input, dispatching on
    /// `RollingWindowDescription` into the regular / to-date /
    /// running-total variant.
    fn plan_rolling_window_query(
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
}

// A time dimension a stage adds to its grain has no date range of its own: it
// takes the range of the same member the state groups by, else its date filters.
fn with_state_date_range(
    time_dimension: Rc<TimeDimensionSymbol>,
    state: &QueryProperties,
) -> Result<Rc<TimeDimensionSymbol>, CubeError> {
    if time_dimension.date_range_vec().is_some() {
        return Ok(time_dimension);
    }
    let base_id = time_dimension
        .base_symbol()
        .clone()
        .resolve_reference_chain()
        .id()
        .clone();
    let grouped_range = state.time_dimensions().iter().find_map(|d| {
        let symbol = d
            .clone()
            .resolve_reference_chain()
            .as_time_dimension()
            .ok()?;
        if symbol.base_symbol().clone().resolve_reference_chain().id() != &base_id {
            return None;
        }
        match symbol.date_range_vec()?.as_slice() {
            [from, to] => Some((from.clone(), to.clone())),
            _ => None,
        }
    });
    let range = match grouped_range {
        Some(range) => Some(range),
        None => {
            let mut range: Option<(String, String)> = None;
            for item in state.time_dimensions_filters() {
                let FilterItem::Item(filter) = item else {
                    continue;
                };
                if !matches!(filter.filter_operator(), FilterOperator::InDateRange)
                    || filter.member_evaluator().resolve_reference_chain().id() != &base_id
                {
                    continue;
                }
                let [FilterValue::Str(from), FilterValue::Str(to)] = filter.values().as_slice()
                else {
                    continue;
                };
                let filter_range = (from.clone(), to.clone());
                range = Some(match range {
                    Some(range) => QueryDateTimeHelper::intersect_date_ranges(
                        &range,
                        &filter_range,
                        QueryTimeSeries::MILLISECOND_PRECISION,
                    )?,
                    None => filter_range,
                });
            }
            range
        }
    };
    Ok(match range {
        Some(range) => time_dimension.with_date_range(Some(range)),
        None => time_dimension,
    })
}
