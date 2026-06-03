use super::TimeShiftState;
use crate::logical_plan::LogicalMultiStageMember;
use std::rc::Rc;

/// Render context a CTE inherits from the planning scope it was
/// created in. CTEs hoisted out of a multi-stage leaf must render
/// with the leaf's time shifts and measure-rendering flags — exactly
/// what the leaf itself would have applied if they stayed nested.
#[derive(Clone, Default)]
pub struct CteRenderContext {
    pub time_shifts: TimeShiftState,
    pub render_measure_as_state: bool,
    pub render_measure_for_ungrouped: bool,
}

/// Plan-wide accumulator of CTEs: a monotonic counter for generated
/// CTE names (`cte_0`, `cte_1`, ...) and the flat list of
/// `LogicalMultiStageMember`s. A single instance is shared across
/// the whole plan — including nested planning scopes (multi-stage
/// leaf queries, dimension subqueries) — so names stay unique and
/// every CTE ends up in the root `WITH` list. Which CTEs a
/// particular assembly joins over is tracked separately via
/// `MultiStageSubqueryRef`s returned by the planners.
pub struct CteState {
    counter: usize,
    members: Vec<Rc<LogicalMultiStageMember>>,
    // `Some` while planning inside a multi-stage leaf scope; CTEs
    // hoisted out of that scope capture it so they render the way
    // they would have nested. `None` at the top level — there the
    // ambient build context applies.
    render_context: Option<CteRenderContext>,
}

impl CteState {
    pub fn new() -> Self {
        Self {
            counter: 0,
            members: Vec::new(),
            render_context: None,
        }
    }

    pub fn render_context(&self) -> &Option<CteRenderContext> {
        &self.render_context
    }

    /// Runs `f` with `render_context` set as the ambient context for
    /// CTEs created inside, restoring the previous one afterwards.
    pub fn with_render_context<T>(
        &mut self,
        render_context: CteRenderContext,
        f: impl FnOnce(&mut Self) -> T,
    ) -> T {
        let saved = std::mem::replace(&mut self.render_context, Some(render_context));
        let result = f(self);
        self.render_context = saved;
        result
    }

    /// Generates the next unique CTE name (`cte_0`, `cte_1`, ...).
    pub fn next_cte_name(&mut self) -> String {
        let name = format!("cte_{}", self.counter);
        self.counter += 1;
        name
    }

    pub fn add_member(&mut self, member: Rc<LogicalMultiStageMember>) {
        self.members.push(member);
    }

    /// Consumes the state, returning the accumulated CTE members in
    /// definition order (dependencies precede their dependents).
    pub fn into_members(self) -> Vec<Rc<LogicalMultiStageMember>> {
        self.members
    }
}
