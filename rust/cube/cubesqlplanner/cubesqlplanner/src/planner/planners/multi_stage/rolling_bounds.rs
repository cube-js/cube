use crate::cube_bridge::measure_definition::RollingWindow;
use crate::planner::state::State;
use crate::planner::time_dimension::shift_bound_wall_clock;
use crate::planner::Granularity;
use crate::planner::GranularityHelper;
use crate::planner::MemberSymbol;
use crate::planner::QueryDateTime;
use crate::planner::QueryDateTimeHelper;
use crate::planner::QueryTimeSeries;
use crate::planner::SeriesSpan;
use crate::planner::TimeDimensionSymbol;
use chrono::Duration;
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Plan-time arithmetic of rolling windows: the span a window's base scan
/// reads and the granularity a `to_date` window counts its period in.
pub struct RollingWindowBounds {
    query_tools: Rc<State>,
}

impl RollingWindowBounds {
    pub fn new(query_tools: Rc<State>) -> Self {
        Self { query_tools }
    }

    /// Outer bounds of the series a rolling window over `time_dimension` walks,
    /// rendered by the base-scan filter as literals instead of read back off
    /// the series.
    ///
    /// `None` where the series is not derivable at plan time: a date range that
    /// is itself a query, a granularity whose periods come off a calendar cube,
    /// and a range standing for a pre-aggregation's partition.
    fn rolling_series_bounds(
        &self,
        time_dimension: &Rc<TimeDimensionSymbol>,
    ) -> Result<Option<SeriesSpan>, CubeError> {
        let Some(granularity) = time_dimension.granularity_obj() else {
            return Ok(None);
        };
        if granularity.calendar_sql().is_some() {
            return Ok(None);
        }
        let Some(date_range) = time_dimension.date_range_vec() else {
            return Ok(None);
        };
        if date_range
            .iter()
            .any(|bound| QueryDateTimeHelper::parse_native_date_time(bound).is_err())
        {
            return Ok(None);
        }
        let bounds = if granularity.is_predefined_granularity() {
            QueryTimeSeries::covering_bounds_predefined(
                granularity.granularity(),
                granularity.granularity_interval(),
                &[date_range[0].clone(), date_range[1].clone()],
                QueryTimeSeries::MILLISECOND_PRECISION,
            )?
        } else {
            self.custom_series_bounds(&granularity, &date_range)?
        };
        Ok(Some(bounds))
    }

    /// [`Self::rolling_series_bounds`] for a custom granularity, whose buckets
    /// are placed by stepping its interval from its origin. Both ends align to
    /// that origin the way the series itself is placed, which is where either
    /// series shape puts its points — so the two upper bounds coincide.
    fn custom_series_bounds(
        &self,
        granularity: &Granularity,
        date_range: &[String],
    ) -> Result<SeriesSpan, CubeError> {
        let interval = granularity.granularity_interval();
        if interval.is_zero() {
            return Err(CubeError::user(format!(
                "Granularity interval can't be zero: {}",
                granularity.granularity()
            )));
        }
        let tz = self.query_tools.query_tools().timezone();
        let first =
            granularity.align_date_to_origin(QueryDateTime::from_date_str(tz, &date_range[0])?)?;
        let last =
            granularity.align_date_to_origin(QueryDateTime::from_date_str(tz, &date_range[1])?)?;
        let past_end = last
            .add_interval(interval)?
            .add_duration(Duration::seconds(-1))?;
        let past_end = format!("{}.999", past_end.format("%Y-%m-%dT%H:%M:%S"));
        Ok(SeriesSpan {
            from: first.default_format(),
            to_aligned: past_end.clone(),
            to_stepped: past_end,
            predefined_granularity: false,
        })
    }

    /// The `Granularity` a `to_date` window counts its period in. The compiler
    /// borrow lives no longer than the build, so a caller is free to reach for
    /// it again — `change_date_range_filter_impl` takes the same one right
    /// after this returns.
    pub(super) fn to_date_period_granularity(
        &self,
        time_dimension: &Rc<TimeDimensionSymbol>,
        granularity: &str,
    ) -> Result<Option<Granularity>, CubeError> {
        let compiler_cell = self.query_tools.compiler().clone();
        let mut compiler = compiler_cell.borrow_mut();
        GranularityHelper::make_granularity_obj(
            self.query_tools.cube_evaluator().clone(),
            &mut compiler,
            time_dimension.base_symbol(),
            Some(granularity.to_string()),
        )
    }

    /// Span a regular rolling window's base scan reads: the series' own bounds
    /// with the window's frame folded in.
    ///
    /// Folded here rather than applied to the bound in SQL, so the predicate is
    /// a bare literal comparison. A dialect that evaluates interval arithmetic
    /// over a constant while planning the query cannot always do it — the
    /// argument reaches its interval function before the surrounding cast does
    /// — and a bare bound is the one every engine can both read and prune by.
    ///
    /// An `unbounded` side keeps the unshifted bound, which the filter drops:
    /// no date states the absence of a bound.
    pub(super) fn regular_scan_span(
        &self,
        time_dimension: &Rc<TimeDimensionSymbol>,
        rolling_window: &RollingWindow,
    ) -> Result<Option<SeriesSpan>, CubeError> {
        let Some(series) = self.rolling_series_bounds(time_dimension)? else {
            return Ok(None);
        };
        let tz = self.query_tools.query_tools().timezone();
        let shift = |bound: &String, interval: &Option<String>, backwards: bool| {
            shift_bound_wall_clock(tz, bound, interval, backwards)
                .map(|shifted| shifted.unwrap_or_else(|| bound.clone()))
        };
        Ok(Some(SeriesSpan {
            from: shift(&series.from, &rolling_window.trailing, true)?,
            to_aligned: shift(&series.to_aligned, &rolling_window.leading, false)?,
            to_stepped: shift(&series.to_stepped, &rolling_window.leading, false)?,
            predefined_granularity: series.predefined_granularity,
        }))
    }

    /// Span the base scan of a `to_date` window over `time_dimension` reads:
    /// from the start of the period the series opens in, to the series' own
    /// end, rendered by the filter as literals.
    ///
    /// `None` wherever the series itself is not derivable
    /// ([`Self::rolling_series_bounds`]), and for a period whose boundaries are
    /// rows of a calendar cube — no interval math reproduces those.
    pub(super) fn to_date_window_bounds(
        &self,
        time_dimension: &Rc<TimeDimensionSymbol>,
        period: &Granularity,
    ) -> Result<Option<SeriesSpan>, CubeError> {
        let Some(series) = self.rolling_series_bounds(time_dimension)? else {
            return Ok(None);
        };
        let Some(period_start) = self.period_start(period, &series.from)? else {
            return Ok(None);
        };
        Ok(Some(SeriesSpan {
            from: period_start,
            ..series
        }))
    }

    /// Start of the `granularity` period `from` falls in. `None` for a period
    /// whose boundaries are rows of a calendar cube.
    pub(super) fn to_date_period_start(
        &self,
        time_dimension: &Rc<TimeDimensionSymbol>,
        granularity: &str,
        from: &String,
    ) -> Result<Option<String>, CubeError> {
        let Some(period) = self.to_date_period_granularity(time_dimension, granularity)? else {
            return Ok(None);
        };
        self.period_start(&period, from)
    }

    /// Start of the `period` that `from` falls in. `None` for a period whose
    /// boundaries are rows of a calendar cube.
    fn period_start(
        &self,
        period: &Granularity,
        from: &String,
    ) -> Result<Option<String>, CubeError> {
        if period.calendar_sql().is_some() {
            return Ok(None);
        }
        if period.is_predefined_granularity() {
            return Ok(Some(QueryTimeSeries::period_start_predefined(
                period.granularity(),
                from,
                QueryTimeSeries::MILLISECOND_PRECISION,
            )?));
        }
        let tz = self.query_tools.query_tools().timezone();
        Ok(Some(
            period
                .align_date_to_origin(QueryDateTime::from_date_str(tz, from)?)?
                .default_format(),
        ))
    }

    /// The granularity of a `to_date` rolling window whose period boundary is
    /// defined by a calendar column. `None` for a boundary that interval math
    /// can compute on its own.
    pub(super) fn calendar_to_date_granularity(
        &self,
        rolling_window: &RollingWindow,
        time_dimension: &Rc<MemberSymbol>,
    ) -> Result<Option<String>, CubeError> {
        let Some(granularity) = self.get_to_date_rolling_granularity(rolling_window)? else {
            return Ok(None);
        };
        let time_dimension = time_dimension.as_time_dimension()?;
        let granularity_obj = self.to_date_period_granularity(&time_dimension, &granularity)?;

        Ok(granularity_obj
            .filter(|obj| obj.calendar_sql().is_some())
            .map(|_| granularity))
    }

    /// Returns the granularity of a `to_date` rolling window. Errors
    /// if the window is declared as `to_date` without a granularity,
    /// and returns `None` for window kinds that don't carry one.
    pub(super) fn get_to_date_rolling_granularity(
        &self,
        rolling_window: &RollingWindow,
    ) -> Result<Option<String>, CubeError> {
        let is_to_date = rolling_window
            .rolling_type
            .as_ref()
            .is_some_and(|tp| tp == "to_date");

        if is_to_date {
            if let Some(granularity) = &rolling_window.granularity {
                Ok(Some(granularity.clone()))
            } else {
                Err(CubeError::user(format!(
                    "Granularity required for to_date rolling window"
                )))
            }
        } else {
            Ok(None)
        }
    }
}
