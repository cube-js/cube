use super::pretty_print::*;
use super::PreAggregationRewriteRole;
use crate::planner::MemberSymbol;
use std::rc::Rc;

/// Stage Calculation flavour — what operation the stage performs over its
/// `FullKeyAggregate`-of-CTE-refs source.
#[derive(Clone)]
pub enum StageKind {
    /// Re-aggregation (GROUP BY all dims + measure agg-wrap).
    Aggregation,
    /// `RANK()` window over the FK-of-CTE-refs.
    Rank { partition_by: Vec<Rc<MemberSymbol>> },
    /// Generic window function over the FK-of-CTE-refs.
    Window { partition_by: Vec<Rc<MemberSymbol>> },
    /// Computes a multi-stage dimension.
    DimensionCalc {
        multi_stage_dimension: Rc<MemberSymbol>,
    },
}

/// Raw-fact body flavour inside the aggregate-multiplied pipeline.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum FactKind {
    /// `SELECT DISTINCT outer_dims + pk_dims FROM join` — ex-KeysSubQuery.
    Keys,
    /// `SELECT pk_dims + raw measures FROM join` with `set_ungrouped_measure`
    /// — ex-MeasureSubquery.
    Measures,
}

/// Explicit role of a Query in the multi-stage pipeline. Variants carry
/// their own role-specific data — there is no longer a `source` /
/// `multistage_members` shape predicate to interpret.
#[derive(Clone, Default)]
pub enum QueryKind {
    /// Top-level / leaf-wrapper sitting over a non-empty FullKeyAggregate
    /// of CTE refs. Multi-stage CTE bodies live in
    /// `Query.multistage_members` (common to any Query flavour) — both
    /// the FK-of-CTE-refs members and the multi-stage-dim bodies are
    /// rendered there.
    TopLevelOverCtes,
    /// Multi-stage Stage Calculation; the nested `StageKind` picks the
    /// flavour and carries the partition / dimension members it needs.
    Stage(StageKind),
    /// Aggregate-multiplied subquery body — FullKeyAggregate joining a
    /// MeasureSubquery CTE to a KeysSubQuery CTE on pk dims. WholeSubtree
    /// rewrite.
    AggregateMultiplied,
    /// Plain aggregating leaf over a LogicalJoin source — top-level
    /// SimpleQuery and regular_measures_subquery bodies.
    #[default]
    LeafOverJoin,
    /// Raw fact body inside the aggregate-multiplied pipeline. `FactKind`
    /// picks Keys (distinct projection) or Measures (ungrouped raw
    /// columns). NoRewrite — the parent AggregateMultiplied is the
    /// rewrite unit.
    InternalFact(FactKind),
    /// Pre-aggregation-backed leaf — output of the pre-agg optimizer.
    PreAggregationLeaf,
}

impl QueryKind {
    /// How the pre-aggregation optimizer should treat this Query when
    /// walking a multi-stage tree.
    pub fn pre_agg_rewrite(&self) -> PreAggregationRewriteRole {
        match self {
            Self::TopLevelOverCtes | Self::LeafOverJoin | Self::PreAggregationLeaf => {
                PreAggregationRewriteRole::Leaf
            }
            Self::Stage(_) => PreAggregationRewriteRole::PassThrough,
            Self::AggregateMultiplied => PreAggregationRewriteRole::WholeSubtree,
            Self::InternalFact(_) => PreAggregationRewriteRole::NoRewrite,
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::TopLevelOverCtes => "TopLevelOverCtes",
            Self::Stage(StageKind::Aggregation) => "StageAggregation",
            Self::Stage(StageKind::Rank { .. }) => "StageRank",
            Self::Stage(StageKind::Window { .. }) => "StageWindow",
            Self::Stage(StageKind::DimensionCalc { .. }) => "StageDimensionCalc",
            Self::AggregateMultiplied => "AggregateMultiplied",
            Self::LeafOverJoin => "LeafOverJoin",
            Self::InternalFact(FactKind::Keys) => "InternalFact(Keys)",
            Self::InternalFact(FactKind::Measures) => "InternalFact(Measures)",
            Self::PreAggregationLeaf => "PreAggregationLeaf",
        }
    }
}

impl PrettyPrint for QueryKind {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println(&format!("kind: {}", self.label()), state);
    }
}
