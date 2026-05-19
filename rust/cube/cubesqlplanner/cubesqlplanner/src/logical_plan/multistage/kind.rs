/// Classifies a `PlanNode` by its role when used as a multi-stage member body.
///
/// A multi-stage member body is either:
/// - **Leaf** — produces a CTE from base tables / joins / pre-aggregations.
///   Has no dependency on other multi-stage CTEs.
/// - **Stage** — composes the result by reading other multi-stage CTEs
///   (typically via `FullKeyAggregate` or named `MultiStageSubqueryRef`s).
///
/// Nodes that exist only as plan structure (`LogicalJoin`, `Cube`, etc.)
/// do not have a kind and are not valid as a multi-stage member body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MultiStageKind {
    Leaf,
    Stage,
}
