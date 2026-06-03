use super::TimeShiftState;
use crate::logical_plan::LogicalMultiStageMember;
use std::rc::Rc;

/// Plan-wide accumulator of CTEs: a monotonic counter for generated
/// CTE names (`cte_0`, `cte_1`, ...) and the flat list of
/// `LogicalMultiStageMember`s in definition order (dependencies
/// precede their dependents).
struct CteState {
    counter: usize,
    members: Vec<Rc<LogicalMultiStageMember>>,
}

impl CteState {
    fn new() -> Self {
        Self {
            counter: 0,
            members: Vec::new(),
        }
    }

    fn next_cte_name(&mut self) -> String {
        let name = format!("cte_{}", self.counter);
        self.counter += 1;
        name
    }

    fn add_member(&mut self, member: Rc<LogicalMultiStageMember>) {
        self.members.push(member);
    }

    fn into_members(self) -> Vec<Rc<LogicalMultiStageMember>> {
        self.members
    }
}

/// How values are evaluated within a multi-stage leaf scope: the
/// time basis of its dimensions and the measure evaluation shape
/// (mergeable state for aggregates-on-top, ungrouped evaluation).
#[derive(Clone, Default)]
pub struct EvaluationContext {
    pub time_shifts: TimeShiftState,
    pub measure_as_state: bool,
    pub measure_for_ungrouped: bool,
}

/// Ambient state of logical planning, threaded through every planner
/// — including nested planning scopes (multi-stage leaves, dimension
/// subqueries). Composes the plan-wide `CteState` accumulator with
/// the `EvaluationContext` of the scope currently being planned, so
/// names stay unique, every CTE ends up in the root `WITH` list, and
/// CTEs hoisted out of a leaf capture how that leaf evaluates its
/// values. Which CTEs a particular assembly joins over is tracked
/// separately via `MultiStageSubqueryRef`s returned by the planners.
pub struct PlanningScope {
    cte_state: CteState,
    // `Some` while planning inside a multi-stage leaf scope; `None`
    // at the top level — there the ambient build context applies.
    evaluation_context: Option<EvaluationContext>,
}

impl PlanningScope {
    pub fn new() -> Self {
        Self {
            cte_state: CteState::new(),
            evaluation_context: None,
        }
    }

    /// Generates the next unique CTE name (`cte_0`, `cte_1`, ...).
    pub fn next_cte_name(&mut self) -> String {
        self.cte_state.next_cte_name()
    }

    pub fn add_member(&mut self, member: Rc<LogicalMultiStageMember>) {
        self.cte_state.add_member(member);
    }

    /// Consumes the scope, returning the accumulated CTE members in
    /// definition order.
    pub fn into_members(self) -> Vec<Rc<LogicalMultiStageMember>> {
        self.cte_state.into_members()
    }

    pub fn evaluation_context(&self) -> &Option<EvaluationContext> {
        &self.evaluation_context
    }

    /// Runs `f` with `evaluation_context` set for the scope, restoring
    /// the previous one afterwards.
    pub fn with_evaluation_context<T>(
        &mut self,
        evaluation_context: EvaluationContext,
        f: impl FnOnce(&mut Self) -> T,
    ) -> T {
        let saved = std::mem::replace(&mut self.evaluation_context, Some(evaluation_context));
        let result = f(self);
        self.evaluation_context = saved;
        result
    }
}
