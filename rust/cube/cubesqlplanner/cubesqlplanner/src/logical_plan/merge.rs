use super::*;
use crate::planner::InputId;
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::HashMap;
use std::rc::Rc;
use typed_builder::TypedBuilder;

/// Reference to a multi-stage subquery by name, with the schema it exposes.
#[derive(Clone, TypedBuilder)]
pub struct SubqueryRef {
    id: InputId,
    name: String,
    schema: Rc<LogicalSchema>,
}

impl SubqueryRef {
    pub fn id(&self) -> InputId {
        self.id
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn schema(&self) -> &Rc<LogicalSchema> {
        &self.schema
    }

    /// The ref `forwards` replaces this one with, this ref otherwise.
    pub fn forwarded(self: &Rc<Self>, forwards: &InputForwards) -> Rc<Self> {
        forwards
            .target(self.id)
            .cloned()
            .unwrap_or_else(|| self.clone())
    }
}

impl PrettyPrint for SubqueryRef {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println(
            &format!("SubqueryRef: {} {}", self.name(), self.id()),
            state,
        );
        let state = state.new_level();
        result.println(
            &format!("measures: {}", print_symbols(&self.schema.measures)),
            &state,
        );
    }
}

/// Top-level aggregating source that stitches together several
/// multi-stage / multi-fact subqueries at one grain into one keyed result.
/// The physical builder picks a join strategy from `subqueries` and the dialect.
#[derive(Clone, TypedBuilder)]
pub struct Merge {
    schema: Rc<LogicalSchema>,
    #[builder(default)]
    subqueries: Vec<Rc<SubqueryRef>>,
}

impl Merge {
    pub fn schema(&self) -> &Rc<LogicalSchema> {
        &self.schema
    }

    pub fn subqueries(&self) -> &Vec<Rc<SubqueryRef>> {
        &self.subqueries
    }

    pub fn is_empty(&self) -> bool {
        self.subqueries.is_empty()
    }
}

impl LogicalNode for Merge {
    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::Merge(self.clone())
    }

    fn inputs(&self) -> Vec<PlanNode> {
        vec![]
    }

    fn with_inputs(self: Rc<Self>, inputs: Vec<PlanNode>) -> Result<Rc<Self>, CubeError> {
        check_inputs_len(&inputs, 0, self.node_name())?;

        Ok(Rc::new(
            Self::builder()
                .schema(self.schema().clone())
                .subqueries(self.subqueries().clone())
                .build(),
        ))
    }

    fn referenced_inputs(&self) -> Vec<InputId> {
        self.subqueries.iter().map(|r| r.id()).collect()
    }

    fn with_forwarded_input(
        self: Rc<Self>,
        forwards: &InputForwards,
    ) -> Result<Rc<Self>, CubeError> {
        if !forwards.replaces_any(&self.referenced_inputs()) {
            return Ok(self);
        }
        Ok(Rc::new(
            Self::builder()
                .schema(self.schema.clone())
                .subqueries(forward_refs(&self.subqueries, forwards))
                .build(),
        ))
    }

    fn node_name(&self) -> &'static str {
        "Merge"
    }
    fn try_from_plan_node(plan_node: PlanNode) -> Result<Rc<Self>, CubeError> {
        if let PlanNode::Merge(item) = plan_node {
            Ok(item)
        } else {
            Err(cast_error(&plan_node, "Merge"))
        }
    }
}

impl PrettyPrint for Merge {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println("Merge: ", state);
        let state = state.new_level();
        let details_state = state.new_level();
        result.println(&format!("schema:"), &state);
        self.schema().pretty_print(result, &details_state);
        if !self.subqueries().is_empty() {
            result.println("subqueries:", &state);
            for subquery_ref in self.subqueries().iter() {
                subquery_ref.pretty_print(result, &details_state);
            }
        }
    }
}

// The refs after `forwards`. A ref already reading a target is replaced too,
// so the refs collapsing onto it keep the schema the forward brings, and they
// are deduplicated.
pub(super) fn forward_refs(
    refs: &[Rc<SubqueryRef>],
    forwards: &InputForwards,
) -> Vec<Rc<SubqueryRef>> {
    refs.iter()
        .map(|r| forwards.collapsed(r))
        .unique_by(|r| r.id())
        .collect_vec()
}

/// Subqueries replaced by others, applied to a plan in one pass so it never
/// holds some replacements without the rest.
#[derive(Clone, Default)]
pub struct InputForwards {
    targets: HashMap<InputId, Rc<SubqueryRef>>,
}

impl InputForwards {
    pub fn single(from: InputId, to: &Rc<SubqueryRef>) -> Self {
        let mut forwards = Self::default();
        forwards.add(from, to);
        forwards
    }

    /// Replaces `from` by `to`, following the replacements already added.
    pub fn add(&mut self, from: InputId, to: &Rc<SubqueryRef>) {
        let to = self.target(to.id()).cloned().unwrap_or_else(|| to.clone());
        for target in self.targets.values_mut() {
            if target.id() == from {
                *target = to.clone();
            }
        }
        self.targets.insert(from, to);
    }

    pub fn is_empty(&self) -> bool {
        self.targets.is_empty()
    }

    pub fn target(&self, from: InputId) -> Option<&Rc<SubqueryRef>> {
        self.targets.get(&from)
    }

    /// Whether any of `inputs` is replaced.
    pub fn replaces_any(&self, inputs: &[InputId]) -> bool {
        inputs.iter().any(|id| self.targets.contains_key(id))
    }

    /// The ref `input` becomes: its target, or the target ref when it already
    /// reads a target.
    pub fn collapsed(&self, input: &Rc<SubqueryRef>) -> Rc<SubqueryRef> {
        if let Some(to) = self.target(input.id()) {
            return to.clone();
        }
        self.targets
            .values()
            .find(|to| to.id() == input.id())
            .cloned()
            .unwrap_or_else(|| input.clone())
    }
}
