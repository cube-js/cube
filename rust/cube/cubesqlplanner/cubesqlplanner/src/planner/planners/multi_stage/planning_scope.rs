use super::TimeShiftState;
use crate::logical_plan::{
    LogicalSchema, MultiStageDimensionInput, Subquery, SubqueryBody, SubqueryRef,
};
use crate::planner::MemberId;
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::HashMap;
use std::rc::Rc;

/// Plan-wide accumulator of CTEs: a monotonic counter for generated
/// CTE names (`cte_0`, `cte_1`, ...) and the flat list of
/// `Subquery`s in definition order (dependencies
/// precede their dependents).
struct CteState {
    counter: usize,
    members: Vec<Rc<Subquery>>,
    // Keyed by the dimensions a calculation resolves; the latest one wins,
    // so a query reads the last such CTE defined before it.
    multi_stage_dimensions: HashMap<Vec<MemberId>, MultiStageDimensionInput>,
}

impl CteState {
    fn new() -> Self {
        Self {
            counter: 0,
            members: Vec::new(),
            multi_stage_dimensions: HashMap::new(),
        }
    }

    fn next_cte_name(&mut self) -> String {
        let name = format!("cte_{}", self.counter);
        self.counter += 1;
        name
    }

    fn add_member(&mut self, member: Rc<Subquery>) -> Result<(), CubeError> {
        if let SubqueryBody::DimensionCalculation(calculation) = &member.body {
            let cte = SubqueryRef::builder()
                .name(member.name.clone())
                .schema(calculation.schema().clone())
                .build();
            self.multi_stage_dimensions.insert(
                calculation.resolved_dimensions()?,
                MultiStageDimensionInput {
                    subquery: Rc::new(cte),
                    keys: calculation.join_dimensions()?,
                },
            );
        }
        self.members.push(member);
        Ok(())
    }

    fn into_members(self) -> Vec<Rc<Subquery>> {
        self.members
    }
}

/// How values are evaluated within a multi-stage leaf scope: the time
/// basis of its dimensions and whether measures are evaluated at row
/// grain for an enclosing aggregation.
#[derive(Clone, Default)]
pub struct EvaluationContext {
    pub time_shifts: TimeShiftState,
    pub measure_for_ungrouped: bool,
}

/// Ambient state of logical planning, threaded through every planner
/// — including nested planning scopes (multi-stage leaves, dimension
/// subqueries). Composes the plan-wide `CteState` accumulator with
/// the `EvaluationContext` of the scope currently being planned, so
/// names stay unique, every CTE ends up in the root `WITH` list, and
/// CTEs hoisted out of a leaf capture how that leaf evaluates its
/// values. Which CTEs a particular assembly joins over is tracked
/// separately via `SubqueryRef`s returned by the planners.
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

    pub fn add_member(&mut self, member: Rc<Subquery>) -> Result<(), CubeError> {
        self.cte_state.add_member(member)
    }

    /// The CTE computing the multi-stage dimensions selected in `schema`.
    pub fn multi_stage_dimension_input(
        &self,
        schema: &LogicalSchema,
    ) -> Result<Option<MultiStageDimensionInput>, CubeError> {
        let dimensions = schema
            .multi_stage_dimensions()?
            .into_iter()
            .map(|member| member.resolve_reference_chain().id().clone())
            .sorted()
            .dedup()
            .collect_vec();
        if dimensions.is_empty() {
            return Ok(None);
        }
        match self.cte_state.multi_stage_dimensions.get(&dimensions) {
            Some(input) => Ok(Some(input.clone())),
            None => Err(CubeError::internal(format!(
                "Cannot find source for resolve multi stage dimensions {}",
                dimensions.iter().join(", ")
            ))),
        }
    }

    /// Consumes the scope, returning the accumulated CTE members in
    /// definition order.
    pub fn into_members(self) -> Vec<Rc<Subquery>> {
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
        let saved = self.evaluation_context.replace(evaluation_context);
        let result = f(self);
        self.evaluation_context = saved;
        result
    }
}
