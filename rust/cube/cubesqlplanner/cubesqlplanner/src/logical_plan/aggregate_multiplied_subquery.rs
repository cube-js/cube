use super::pretty_print::*;
use super::*;
use crate::planner::planners::multi_stage::EvaluationContext;
use crate::planner::{BaseCube, InputId, MemberSymbol};
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Aggregates multiplied measures over the deduplicated keys of `pk_cube`,
/// reading the values from `pk_cube` joined back by its primary keys, or from
/// `measure_values` when the measures reach other cubes.
#[derive(Clone)]
pub struct AggregateMultipliedSubquery {
    pub schema: Rc<LogicalSchema>,
    pub keys: Rc<SubqueryRef>,
    pub pk_cube: Rc<BaseCube>,
    pub primary_keys_dimensions: Vec<Rc<MemberSymbol>>,
    pub measure_values: Option<Rc<SubqueryRef>>,
    pub dimension_inputs: Vec<DimensionInput>,
    // `Some` when this subquery was hoisted out of a multi-stage leaf
    // scope: the leaf's evaluation, whose time shifts it renders
    // under. `None` for top-level multiplied measures — the ambient
    // build context applies.
    pub evaluation_context: Option<EvaluationContext>,
}

fn forward_ref(subquery: &Rc<SubqueryRef>, from: InputId, to: &Rc<SubqueryRef>) -> Rc<SubqueryRef> {
    if subquery.id() == from {
        to.clone()
    } else {
        subquery.clone()
    }
}

impl LogicalNode for AggregateMultipliedSubquery {
    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::AggregateMultipliedSubquery(self.clone())
    }

    fn inputs(&self) -> Vec<PlanNode> {
        vec![]
    }

    fn with_inputs(self: Rc<Self>, inputs: Vec<PlanNode>) -> Result<Rc<Self>, CubeError> {
        check_inputs_len(&inputs, 0, self.node_name())?;
        Ok(self)
    }

    fn referenced_inputs(&self) -> Vec<InputId> {
        std::iter::once(self.keys.id())
            .chain(self.measure_values.iter().map(|values| values.id()))
            .chain(
                self.dimension_inputs
                    .iter()
                    .map(|input| input.subquery.id()),
            )
            .collect()
    }

    fn with_forwarded_input(self: Rc<Self>, from: InputId, to: &Rc<SubqueryRef>) -> Rc<Self> {
        if !self.referenced_inputs().contains(&from) {
            return self;
        }
        Rc::new(Self {
            keys: forward_ref(&self.keys, from, to),
            measure_values: self
                .measure_values
                .as_ref()
                .map(|values| forward_ref(values, from, to)),
            dimension_inputs: forward_dimension_inputs(&self.dimension_inputs, from, to),
            ..(*self).clone()
        })
    }

    fn node_name(&self) -> &'static str {
        "AggregateMultipliedSubquery"
    }
    fn try_from_plan_node(plan_node: PlanNode) -> Result<Rc<Self>, CubeError> {
        if let PlanNode::AggregateMultipliedSubquery(item) = plan_node {
            Ok(item)
        } else {
            Err(cast_error(&plan_node, "AggregateMultipliedSubquery"))
        }
    }
}

impl PrettyPrint for AggregateMultipliedSubquery {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println("AggregateMultipliedSubquery: ", state);
        let state = state.new_level();
        let details_state = state.new_level();
        result.println("schema:", &state);
        self.schema.pretty_print(result, &details_state);
        result.println(&format!("pk_cube: {}", self.pk_cube.cube_id()), &state);
        result.println(
            &format!(
                "primary_keys_dimensions: {}",
                print_symbols(&self.primary_keys_dimensions)
            ),
            &state,
        );
        result.println(
            &format!("keys: {} {}", self.keys.name(), self.keys.id()),
            &state,
        );
        if let Some(values) = &self.measure_values {
            result.println(
                &format!("measure_values: {} {}", values.name(), values.id()),
                &state,
            );
        }
        pretty_print_dimension_inputs(&self.dimension_inputs, result, &state);
    }
}
