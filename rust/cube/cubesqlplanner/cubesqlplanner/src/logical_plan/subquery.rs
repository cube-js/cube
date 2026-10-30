use crate::logical_plan::*;
use crate::planner::InputId;
use cubenativeutils::CubeError;
use std::rc::Rc;

// What a `Subquery` computes. A `Query` body is a multi-stage leaf or the
// subquery of a sub-query dimension.
logical_source_enum!(
    SubqueryBody,
    [
        Query,
        AggregateMultipliedSubquery,
        MultiStageMeasureCalculation,
        MultiStageDimensionCalculation,
        MultiStageGetDateRange,
        MultiStageTimeSeries,
        MultiStageRollingWindow,
    ]
);

/// How readers joining a subquery by keys match a NULL key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyMatch {
    /// A NULL key matches nothing: the keys identify an entity, and a reader
    /// row without one has no value.
    Equal,
    /// A NULL key matches a NULL key: the keys are group values, NULL among them.
    NullSafe,
}

/// How a subquery is rendered and how readers join it. Optimizers may tune these
/// per warehouse, but only in ways that keep the query's result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SubqueryOptions {
    /// Rendered in place of its refs instead of as a `WITH` entry.
    pub inline: bool,
    pub key_match: KeyMatch,
}

/// A named subquery of the plan, read through `SubqueryRef`s. `RootQuery`
/// holds the flat list of them.
pub struct Subquery {
    pub id: InputId,
    pub name: String,
    pub body: SubqueryBody,
    pub options: SubqueryOptions,
}

impl Subquery {
    /// A `WITH` entry whose readers match keys null-safely.
    pub fn new(id: InputId, name: String, body: SubqueryBody) -> Self {
        Self {
            id,
            name,
            body,
            options: SubqueryOptions {
                inline: false,
                key_match: KeyMatch::NullSafe,
            },
        }
    }

    pub fn with_options(self, options: SubqueryOptions) -> Self {
        Self { options, ..self }
    }

    pub fn with_body(&self, body: SubqueryBody) -> Rc<Self> {
        Rc::new(Self {
            id: self.id,
            name: self.name.clone(),
            body,
            options: self.options,
        })
    }
}

impl LogicalNode for Subquery {
    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::Subquery(self.clone())
    }

    fn inputs(&self) -> Vec<PlanNode> {
        vec![self.body.as_plan_node()]
    }

    fn with_inputs(self: Rc<Self>, inputs: Vec<PlanNode>) -> Result<Rc<Self>, CubeError> {
        check_inputs_len(&inputs, 1, self.node_name())?;
        let input = inputs[0].clone();

        Ok(self.with_body(self.body.with_plan_node(input)?))
    }

    fn node_name(&self) -> &'static str {
        "Subquery"
    }

    fn try_from_plan_node(plan_node: PlanNode) -> Result<Rc<Self>, CubeError> {
        if let PlanNode::Subquery(item) = plan_node {
            Ok(item)
        } else {
            Err(cast_error(&plan_node, "Subquery"))
        }
    }
}

impl PrettyPrint for Subquery {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        let inline = if self.options.inline { " (inline)" } else { "" };
        result.println(
            &format!("Subquery `{}` {}{}: ", self.name, self.id, inline),
            state,
        );
        let details_state = state.new_level();
        self.body.pretty_print(result, &details_state);
    }
}
