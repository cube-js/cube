use super::TimeShiftState;
use crate::logical_plan::{
    DimensionInput, DimensionInputKeys, LogicalSchema, Subquery, SubqueryBody, SubqueryRef,
};
use crate::planner::{InputId, MemberId};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::HashMap;
use std::rc::Rc;

/// Plan-wide accumulator of subqueries: monotonic counters for generated
/// subquery names (`cte_0`, `cte_1`, ...) and input ids, and the flat list
/// of `Subquery`s in definition order (dependencies precede their
/// dependents).
struct SubqueryState {
    counter: usize,
    // Separate from `counter`: fixed-name subqueries (`time_series*`) take ids
    // without a `cte_N` name, so `cte_N` numbering must not depend on them.
    input_counter: usize,
    members: Vec<Rc<Subquery>>,
    // Keyed by the dimensions a calculation resolves; the latest one wins,
    // so a query reads the last such subquery defined before it.
    multi_stage_dimensions: HashMap<Vec<MemberId>, DimensionInput>,
}

impl SubqueryState {
    fn new() -> Self {
        Self {
            counter: 0,
            input_counter: 0,
            members: Vec::new(),
            multi_stage_dimensions: HashMap::new(),
        }
    }

    fn next_subquery_name(&mut self) -> String {
        let name = format!("cte_{}", self.counter);
        self.counter += 1;
        name
    }

    fn next_input_id(&mut self) -> InputId {
        let id = InputId::new(self.input_counter);
        self.input_counter += 1;
        id
    }

    fn add_member(&mut self, member: Rc<Subquery>) -> Result<(), CubeError> {
        if let SubqueryBody::MultiStageDimensionCalculation(calculation) = &member.body {
            let subquery = SubqueryRef::builder()
                .id(member.id)
                .name(member.name.clone())
                .schema(calculation.schema().clone())
                .build();
            self.multi_stage_dimensions.insert(
                calculation.resolved_dimensions()?,
                DimensionInput {
                    subquery: Rc::new(subquery),
                    keys: calculation.join_dimensions()?,
                    keys_kind: DimensionInputKeys::Grid,
                },
            );
        }
        self.members.push(member);
        Ok(())
    }

    fn into_members(self) -> Vec<Rc<Subquery>> {
        self.members
    }

    // Subqueries registered since `planned_from` are inputs of the replacement,
    // so they move ahead of it.
    fn replace_member(
        &mut self,
        member: Rc<Subquery>,
        planned_from: usize,
    ) -> Result<(), CubeError> {
        let new_inputs = self.members.split_off(planned_from);
        let Some(position) = self.members.iter().position(|m| m.id == member.id) else {
            return Err(CubeError::internal(format!(
                "Subquery {} to replace is not defined",
                member.id
            )));
        };
        self.members[position] = member;
        self.members.splice(position..position, new_inputs);
        Ok(())
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
/// subqueries). Composes the plan-wide `SubqueryState` accumulator with
/// the `EvaluationContext` of the scope currently being planned, so
/// names stay unique, every subquery ends up in the root list, and
/// subqueries hoisted out of a leaf capture how that leaf evaluates its
/// values. Which subqueries a particular assembly joins over is tracked
/// separately via `SubqueryRef`s returned by the planners.
pub struct PlanningScope {
    subquery_state: SubqueryState,
    // `Some` while planning inside a multi-stage leaf scope; `None`
    // at the top level — there the ambient build context applies.
    evaluation_context: Option<EvaluationContext>,
}

impl PlanningScope {
    pub fn new() -> Self {
        Self {
            subquery_state: SubqueryState::new(),
            evaluation_context: None,
        }
    }

    /// The input id and the unique name (`cte_0`, `cte_1`, ...) of the next
    /// subquery.
    pub fn next_subquery(&mut self) -> (InputId, String) {
        let id = self.subquery_state.next_input_id();
        (id, self.subquery_state.next_subquery_name())
    }

    pub fn next_input_id(&mut self) -> InputId {
        self.subquery_state.next_input_id()
    }

    pub fn add_member(&mut self, member: Rc<Subquery>) -> Result<(), CubeError> {
        self.subquery_state.add_member(member)
    }

    /// Replaces the subquery `id` with the one `plan` builds, in place. What
    /// `plan` registers is placed ahead of it, so inputs still precede readers.
    pub fn replan_member(
        &mut self,
        id: InputId,
        plan: impl FnOnce(&mut Self) -> Result<Rc<Subquery>, CubeError>,
    ) -> Result<(), CubeError> {
        let planned_from = self.subquery_state.members.len();
        let member = plan(self)?;
        if member.id != id {
            return Err(CubeError::internal(format!(
                "Subquery {id} was replanned as {}",
                member.id
            )));
        }
        self.subquery_state.replace_member(member, planned_from)
    }

    /// Adds `body` as a subquery its readers render in place; `schema` is
    /// what they may read from it.
    pub fn add_inline(
        &mut self,
        name: String,
        body: impl Into<SubqueryBody>,
        schema: Rc<LogicalSchema>,
    ) -> Result<Rc<SubqueryRef>, CubeError> {
        let id = self.next_input_id();
        self.add_member(Rc::new(Subquery::new_inline(id, name.clone(), body.into())))?;
        Ok(Rc::new(
            SubqueryRef::builder()
                .id(id)
                .name(name)
                .schema(schema)
                .build(),
        ))
    }

    /// The subquery computing the multi-stage dimensions selected in `schema`.
    pub fn multi_stage_dimension_input(
        &self,
        schema: &LogicalSchema,
    ) -> Result<Option<DimensionInput>, CubeError> {
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
        match self.subquery_state.multi_stage_dimensions.get(&dimensions) {
            Some(input) => Ok(Some(input.clone())),
            None => Err(CubeError::internal(format!(
                "Cannot find source for resolve multi stage dimensions {}",
                dimensions.iter().join(", ")
            ))),
        }
    }

    /// Consumes the scope, returning the accumulated subqueries in
    /// definition order.
    pub fn into_members(self) -> Vec<Rc<Subquery>> {
        self.subquery_state.into_members()
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
