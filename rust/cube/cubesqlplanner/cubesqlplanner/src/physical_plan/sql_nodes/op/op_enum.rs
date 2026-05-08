use crate::physical_plan::sql_nodes::{RenderReferences, SqlNode};
use crate::planner::planners::multi_stage::TimeShiftState;
use crate::planner::symbols::CalendarDimensionTimeShift;
use cubenativeutils::CubeError;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use super::{
    AutoPrefixOp, CalendarTimeShiftOp, CaseOp, DispatchByKindOp, EvaluateSymbolOp, FinalMeasureOp,
    FinalPreAggregationMeasureOp, GeoDimensionOp, LegacySqlNodeOp, MaskedOp, MeasureFilterOp,
    MultiStageRankOp, MultiStageWindowOp, OpCtx, OpExec, ParenthesizeOp, RenderReferencesOp,
    RollingWindowOp, TimeDimensionOp, TimeShiftOp, UngroupedMeasureOp,
    UngroupedQueryFinalMeasureOp,
};

/// All op variants that participate in pipeline rendering.
///
/// Adding a new op = new variant here + new dispatch arm in [`OpExec for Op`]
/// + (preferably) a constructor on `impl Op`. The compiler enforces
/// exhaustiveness on the dispatch — there is no central match with logic to
/// keep in sync; per-variant logic lives in its own struct's `OpExec` impl.
///
/// `LegacySqlNode` is a migration-only escape hatch that wraps an
/// `Rc<dyn SqlNode>`; it goes away once every legacy node has been migrated.
#[derive(Clone)]
pub enum Op {
    EvaluateSymbol(EvaluateSymbolOp),
    Parenthesize(ParenthesizeOp),
    AutoPrefix(AutoPrefixOp),
    GeoDimension(GeoDimensionOp),
    MeasureFilter(MeasureFilterOp),
    RenderReferences(RenderReferencesOp),
    Masked(MaskedOp),
    Case(CaseOp),
    DispatchByKind(DispatchByKindOp),
    FinalMeasure(FinalMeasureOp),
    FinalPreAggregationMeasure(FinalPreAggregationMeasureOp),
    UngroupedMeasure(UngroupedMeasureOp),
    UngroupedQueryFinalMeasure(UngroupedQueryFinalMeasureOp),
    TimeDimension(TimeDimensionOp),
    TimeShift(TimeShiftOp),
    CalendarTimeShift(CalendarTimeShiftOp),
    MultiStageRank(MultiStageRankOp),
    MultiStageWindow(MultiStageWindowOp),
    RollingWindow(RollingWindowOp),
    LegacySqlNode(LegacySqlNodeOp),
}

impl Op {
    pub fn evaluate_symbol() -> Self {
        Self::EvaluateSymbol(EvaluateSymbolOp)
    }

    pub fn parenthesize() -> Self {
        Self::Parenthesize(ParenthesizeOp)
    }

    pub fn auto_prefix(cube_references: HashMap<String, String>) -> Self {
        Self::AutoPrefix(AutoPrefixOp::new(cube_references))
    }

    pub fn geo_dimension() -> Self {
        Self::GeoDimension(GeoDimensionOp)
    }

    pub fn measure_filter() -> Self {
        Self::MeasureFilter(MeasureFilterOp)
    }

    pub fn render_references(references: RenderReferences) -> Self {
        Self::RenderReferences(RenderReferencesOp::new(references))
    }

    pub fn masked(ungrouped: bool) -> Self {
        Self::Masked(MaskedOp::new(ungrouped))
    }

    pub fn case() -> Self {
        Self::Case(CaseOp)
    }

    pub fn dispatch_by_kind(
        dimension: Vec<Op>,
        time_dimension: Vec<Op>,
        measure: Vec<Op>,
        default: Vec<Op>,
    ) -> Self {
        Self::DispatchByKind(DispatchByKindOp::new(
            dimension,
            time_dimension,
            measure,
            default,
        ))
    }

    pub fn final_measure(
        rendered_as_multiplied_measures: HashSet<String>,
        count_approx_as_state: bool,
    ) -> Self {
        Self::FinalMeasure(FinalMeasureOp::new(
            rendered_as_multiplied_measures,
            count_approx_as_state,
        ))
    }

    pub fn final_pre_aggregation_measure(references: RenderReferences) -> Self {
        Self::FinalPreAggregationMeasure(FinalPreAggregationMeasureOp::new(references))
    }

    pub fn ungrouped_measure() -> Self {
        Self::UngroupedMeasure(UngroupedMeasureOp)
    }

    pub fn ungrouped_query_final_measure() -> Self {
        Self::UngroupedQueryFinalMeasure(UngroupedQueryFinalMeasureOp)
    }

    pub fn time_dimension(dimensions_with_ignored_timezone: HashSet<String>) -> Self {
        Self::TimeDimension(TimeDimensionOp::new(dimensions_with_ignored_timezone))
    }

    pub fn time_shift(shifts: TimeShiftState) -> Self {
        Self::TimeShift(TimeShiftOp::new(shifts))
    }

    pub fn calendar_time_shift(shifts: HashMap<String, CalendarDimensionTimeShift>) -> Self {
        Self::CalendarTimeShift(CalendarTimeShiftOp::new(shifts))
    }

    pub fn multi_stage_rank(partition: Vec<String>) -> Self {
        Self::MultiStageRank(MultiStageRankOp::new(partition))
    }

    pub fn multi_stage_window(
        input_pipeline: Vec<Op>,
        else_pipeline: Vec<Op>,
        partition: Vec<String>,
    ) -> Self {
        Self::MultiStageWindow(MultiStageWindowOp::new(
            input_pipeline,
            else_pipeline,
            partition,
        ))
    }

    pub fn rolling_window(input_pipeline: Vec<Op>, default_pipeline: Vec<Op>) -> Self {
        Self::RollingWindow(RollingWindowOp::new(input_pipeline, default_pipeline))
    }

    pub fn legacy(node: Rc<dyn SqlNode>) -> Self {
        Self::LegacySqlNode(LegacySqlNodeOp::new(node))
    }
}

impl OpExec for Op {
    fn exec(&self, ctx: &mut OpCtx<'_>) -> Result<String, CubeError> {
        match self {
            Op::EvaluateSymbol(o) => o.exec(ctx),
            Op::Parenthesize(o) => o.exec(ctx),
            Op::AutoPrefix(o) => o.exec(ctx),
            Op::GeoDimension(o) => o.exec(ctx),
            Op::MeasureFilter(o) => o.exec(ctx),
            Op::RenderReferences(o) => o.exec(ctx),
            Op::Masked(o) => o.exec(ctx),
            Op::Case(o) => o.exec(ctx),
            Op::DispatchByKind(o) => o.exec(ctx),
            Op::FinalMeasure(o) => o.exec(ctx),
            Op::FinalPreAggregationMeasure(o) => o.exec(ctx),
            Op::UngroupedMeasure(o) => o.exec(ctx),
            Op::UngroupedQueryFinalMeasure(o) => o.exec(ctx),
            Op::TimeDimension(o) => o.exec(ctx),
            Op::TimeShift(o) => o.exec(ctx),
            Op::CalendarTimeShift(o) => o.exec(ctx),
            Op::MultiStageRank(o) => o.exec(ctx),
            Op::MultiStageWindow(o) => o.exec(ctx),
            Op::RollingWindow(o) => o.exec(ctx),
            Op::LegacySqlNode(o) => o.exec(ctx),
        }
    }
}
