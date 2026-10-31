use super::pretty_print::*;
use super::*;
use crate::planner::{InputId, MemberSymbol};
use cubenativeutils::CubeError;
use std::rc::Rc;

/// How a reader's NULL key matches the input's.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyMatch {
    /// A NULL key matches nothing: the keys identify an entity, and a reader
    /// row without one has no value.
    Equal,
    /// A NULL key matches a NULL key: the keys are group values, NULL among them.
    NullSafe,
}

/// A subquery computing dimensions the reader selects, joined to the reader
/// by `keys`. The ref's schema is what the reader may read from it.
#[derive(Clone)]
pub struct DimensionInput {
    pub subquery: Rc<SubqueryRef>,
    pub keys: Vec<Rc<MemberSymbol>>,
    /// What the keys mean, not how the join renders: `NullSafe` may still render
    /// as `=` for a key that cannot be NULL.
    pub key_match: KeyMatch,
}

impl DimensionInput {
    pub fn with_forwarded_input(&self, from: InputId, to: &Rc<SubqueryRef>) -> Self {
        if self.subquery.id() == from {
            Self {
                subquery: to.clone(),
                ..self.clone()
            }
        } else {
            self.clone()
        }
    }
}

impl PrettyPrint for DimensionInput {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println(
            &format!(
                "{} {} by {}",
                self.subquery.name(),
                self.subquery.id(),
                print_symbols(&self.keys)
            ),
            state,
        );
    }
}

pub(super) fn forward_dimension_inputs(
    inputs: &[DimensionInput],
    from: InputId,
    to: &Rc<SubqueryRef>,
) -> Vec<DimensionInput> {
    inputs
        .iter()
        .map(|input| input.with_forwarded_input(from, to))
        .collect()
}

pub(super) fn pretty_print_dimension_inputs(
    inputs: &[DimensionInput],
    result: &mut PrettyPrintResult,
    state: &PrettyPrintState,
) {
    if inputs.is_empty() {
        return;
    }
    result.println("dimension_inputs:", state);
    let details_state = state.new_level();
    for input in inputs.iter() {
        input.pretty_print(result, &details_state);
    }
}

/// The input a query reads, with the dimension subqueries joined to it.
#[derive(Clone)]
pub struct QuerySource {
    base: InputBase,
    dimension_inputs: Vec<DimensionInput>,
}

impl QuerySource {
    pub fn new(base: impl Into<InputBase>) -> Rc<Self> {
        Rc::new(Self {
            base: base.into(),
            dimension_inputs: vec![],
        })
    }

    pub fn base(&self) -> &InputBase {
        &self.base
    }

    pub fn dimension_inputs(&self) -> &Vec<DimensionInput> {
        &self.dimension_inputs
    }

    pub fn add_dimension_inputs(&self, inputs: Vec<DimensionInput>) -> Rc<Self> {
        let mut dimension_inputs = self.dimension_inputs.clone();
        dimension_inputs.extend(inputs);
        Rc::new(Self {
            dimension_inputs,
            ..self.clone()
        })
    }
}

impl LogicalNode for QuerySource {
    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::QuerySource(self.clone())
    }

    fn inputs(&self) -> Vec<PlanNode> {
        vec![self.base.as_plan_node()]
    }

    fn with_inputs(self: Rc<Self>, inputs: Vec<PlanNode>) -> Result<Rc<Self>, CubeError> {
        check_inputs_len(&inputs, 1, self.node_name())?;
        Ok(Rc::new(Self {
            base: self.base.with_plan_node(inputs[0].clone())?,
            dimension_inputs: self.dimension_inputs.clone(),
        }))
    }

    fn referenced_inputs(&self) -> Vec<InputId> {
        self.dimension_inputs
            .iter()
            .map(|input| input.subquery.id())
            .collect()
    }

    fn with_forwarded_input(self: Rc<Self>, from: InputId, to: &Rc<SubqueryRef>) -> Rc<Self> {
        if !self.referenced_inputs().contains(&from) {
            return self;
        }
        Rc::new(Self {
            dimension_inputs: forward_dimension_inputs(&self.dimension_inputs, from, to),
            base: self.base.clone(),
        })
    }

    fn node_name(&self) -> &'static str {
        "QuerySource"
    }

    fn try_from_plan_node(plan_node: PlanNode) -> Result<Rc<Self>, CubeError> {
        if let PlanNode::QuerySource(item) = plan_node {
            Ok(item)
        } else {
            Err(cast_error(&plan_node, "QuerySource"))
        }
    }
}

impl PrettyPrint for QuerySource {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        self.base.pretty_print(result, state);
        pretty_print_dimension_inputs(&self.dimension_inputs, result, state);
    }
}
