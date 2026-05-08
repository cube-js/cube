use crate::physical_plan::sql_nodes::{RenderReferences, SqlNode};
use cubenativeutils::CubeError;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use super::{
    AutoPrefixOp, CaseOp, DispatchByKindOp, EvaluateSymbolOp, FinalMeasureOp,
    FinalPreAggregationMeasureOp, GeoDimensionOp, LegacySqlNodeOp, MaskedOp, MeasureFilterOp,
    OpCtx, OpExec, ParenthesizeOp, RenderReferencesOp, UngroupedMeasureOp,
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
        Self::DispatchByKind(DispatchByKindOp {
            dimension,
            time_dimension,
            measure,
            default,
        })
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
            Op::LegacySqlNode(o) => o.exec(ctx),
        }
    }
}
