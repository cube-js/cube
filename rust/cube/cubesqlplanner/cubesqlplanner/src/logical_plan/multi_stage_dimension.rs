use super::pretty_print::*;
use super::LogicalSchema;
use crate::planner::MemberSymbol;
use std::rc::Rc;

/// Lightweight reference to a top-level multi-stage CTE that materialises
/// a computed dimension. Unifies the former `DimensionSubQuery` (DSQ —
/// subquery-dim leaf body joined to a pk-cube by its primary keys) and
/// `StageDimensionCalc` (multi-stage dim body joined by outer
/// dimensions) under one descriptor.
///
/// The CTE body lives on the top-level `Query` as a normal
/// `LogicalMultiStageMember` (same publication path as KS/MS/AggMS-Query
/// bodies). This ref carries everything a consumer needs to wire the
/// CTE into its FROM and to resolve render references for the exposed
/// symbol — no body inside.
#[derive(Debug)]
pub struct MultiStageDimensionRef {
    /// Stable CTE name. Matches the `LogicalMultiStageMember.name` that
    /// holds the body on the top-level Query.
    pub name: String,
    /// Schema of the CTE body — used to resolve the column alias for
    /// `body_column` during render.
    pub schema: Rc<LogicalSchema>,
    /// How the consumer joins this CTE into its FROM.
    pub join: MultiStageDimensionJoin,
    /// The MemberSymbol exposed to the outer scope by this CTE. The
    /// outer SELECT substitutes `exposed.full_name()` with a reference
    /// to the column corresponding to `body_column` in this CTE.
    pub exposed: Rc<MemberSymbol>,
    /// The MemberSymbol that the body actually projects as the value
    /// column. For the ex-DSQ pattern this is the synthetic
    /// `measure_for_subquery_dimension` produced by the planner; for
    /// the multi-stage-dim pattern this is the dim's own symbol.
    pub body_column: Rc<MemberSymbol>,
}

/// How a `MultiStageDimensionRef` CTE is joined into the consumer's
/// FROM.
#[derive(Clone, Debug)]
pub enum MultiStageDimensionJoin {
    /// LEFT JOIN inside the cube-join chain, attached after `cube_name`
    /// is joined in. Used when the computed dim is keyed by the cube's
    /// own primary keys (the ex-DSQ pattern).
    OnPrimaryKeys {
        cube_name: String,
        pk_dimensions: Vec<Rc<MemberSymbol>>,
    },
    /// LEFT JOIN after the whole join chain / FullKeyAggregate output,
    /// keyed by the listed outer dimensions (the ex-multi-stage-dim
    /// pattern).
    OnOuterDimensions { dimensions: Vec<Rc<MemberSymbol>> },
}

impl MultiStageDimensionJoin {
    pub fn label(&self) -> &'static str {
        match self {
            Self::OnPrimaryKeys { .. } => "OnPrimaryKeys",
            Self::OnOuterDimensions { .. } => "OnOuterDimensions",
        }
    }
}

impl PrettyPrint for MultiStageDimensionRef {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println(
            &format!(
                "MultiStageDimensionRef `{}` -> {} ({})",
                self.name,
                self.exposed.full_name(),
                self.join.label()
            ),
            state,
        );
    }
}
