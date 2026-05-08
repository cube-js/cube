use crate::physical_plan::sql_nodes::RenderReferences;
use crate::planner::planners::multi_stage::TimeShiftState;
use crate::planner::symbols::CalendarDimensionTimeShift;
use cubenativeutils::CubeError;
use std::collections::{HashMap, HashSet};

use super::{
    AutoPrefixOp, CalendarTimeShiftOp, CaseOp, DispatchByKindOp, EvaluateSymbolOp, FinalMeasureOp,
    FinalPreAggregationMeasureOp, GeoDimensionOp, MaskedOp, MeasureFilterOp, MultiStageRankOp,
    MultiStageWindowOp, OpCtx, OpExec, ParenthesizeOp, RenderReferencesOp, RollingWindowOp,
    TimeDimensionOp, TimeShiftOp, UngroupedMeasureOp, UngroupedQueryFinalMeasureOp,
};

/// All op variants that participate in pipeline rendering.
///
/// Adding a new op means three things: a new variant here, a new dispatch
/// arm in `impl OpExec for Op`, and (preferably) a constructor on `impl Op`.
/// The compiler enforces exhaustiveness on the dispatch — there is no
/// central match with logic to keep in sync; per-variant logic lives in its
/// own struct's `OpExec` impl.
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
        }
    }

    fn is_terminal(&self) -> bool {
        match self {
            Op::EvaluateSymbol(o) => o.is_terminal(),
            Op::Parenthesize(o) => o.is_terminal(),
            Op::AutoPrefix(o) => o.is_terminal(),
            Op::GeoDimension(o) => o.is_terminal(),
            Op::MeasureFilter(o) => o.is_terminal(),
            Op::RenderReferences(o) => o.is_terminal(),
            Op::Masked(o) => o.is_terminal(),
            Op::Case(o) => o.is_terminal(),
            Op::DispatchByKind(o) => o.is_terminal(),
            Op::FinalMeasure(o) => o.is_terminal(),
            Op::FinalPreAggregationMeasure(o) => o.is_terminal(),
            Op::UngroupedMeasure(o) => o.is_terminal(),
            Op::UngroupedQueryFinalMeasure(o) => o.is_terminal(),
            Op::TimeDimension(o) => o.is_terminal(),
            Op::TimeShift(o) => o.is_terminal(),
            Op::CalendarTimeShift(o) => o.is_terminal(),
            Op::MultiStageRank(o) => o.is_terminal(),
            Op::MultiStageWindow(o) => o.is_terminal(),
            Op::RollingWindow(o) => o.is_terminal(),
        }
    }
}

impl Op {
    /// Validate a pipeline: it must be non-empty, end with exactly one
    /// terminal op, and contain no terminals before that. Recurses into
    /// sub-pipelines carried by branching ops (`DispatchByKind`,
    /// `MultiStageWindow`, `RollingWindow`).
    pub fn validate_pipeline(ops: &[Op]) -> Result<(), CubeError> {
        if ops.is_empty() {
            return Err(CubeError::internal(
                "Op pipeline is empty — needs at least one terminal op".to_string(),
            ));
        }
        let last_idx = ops.len() - 1;
        for (i, op) in ops.iter().enumerate() {
            let terminal = op.is_terminal();
            if terminal && i != last_idx {
                return Err(CubeError::internal(format!(
                    "Terminal op at position {} of {}; ops after it would be unreachable",
                    i,
                    ops.len()
                )));
            }
            if !terminal && i == last_idx {
                return Err(CubeError::internal(
                    "Pipeline ends with a non-terminal op — render_tail will hit an empty tail at runtime".to_string(),
                ));
            }
            for sub in nested_pipelines(op) {
                Op::validate_pipeline(sub)?;
            }
        }
        Ok(())
    }
}

fn nested_pipelines(op: &Op) -> Vec<&[Op]> {
    match op {
        Op::DispatchByKind(o) => o.nested_pipelines().to_vec(),
        Op::MultiStageWindow(o) => o.nested_pipelines().to_vec(),
        Op::RollingWindow(o) => o.nested_pipelines().to_vec(),
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn terminal() -> Op {
        Op::evaluate_symbol()
    }

    fn non_terminal() -> Op {
        Op::parenthesize()
    }

    #[test]
    fn empty_pipeline_is_invalid() {
        assert!(Op::validate_pipeline(&[]).is_err());
    }

    #[test]
    fn pipeline_must_end_with_terminal() {
        assert!(Op::validate_pipeline(&[non_terminal()]).is_err());
        assert!(Op::validate_pipeline(&[non_terminal(), non_terminal()]).is_err());
    }

    #[test]
    fn terminal_in_the_middle_is_invalid() {
        assert!(Op::validate_pipeline(&[terminal(), terminal()]).is_err());
        assert!(Op::validate_pipeline(&[terminal(), non_terminal(), terminal()]).is_err());
    }

    #[test]
    fn valid_linear_pipeline() {
        assert!(Op::validate_pipeline(&[terminal()]).is_ok());
        assert!(Op::validate_pipeline(&[non_terminal(), terminal()]).is_ok());
        assert!(Op::validate_pipeline(&[non_terminal(), non_terminal(), terminal()]).is_ok());
    }

    #[test]
    fn nested_pipelines_are_validated_recursively() {
        // DispatchByKind itself is terminal, so it can stand alone — but its
        // four branches are independent pipelines that must each be valid.
        let bad_branch = Op::dispatch_by_kind(
            vec![non_terminal()], // missing terminal at the end
            vec![terminal()],
            vec![terminal()],
            vec![terminal()],
        );
        assert!(Op::validate_pipeline(&[bad_branch]).is_err());

        let good = Op::dispatch_by_kind(
            vec![non_terminal(), terminal()],
            vec![terminal()],
            vec![terminal()],
            vec![terminal()],
        );
        assert!(Op::validate_pipeline(&[good]).is_ok());
    }
}
