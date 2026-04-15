use super::*;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct ResolveMultipliedMeasures {
    pub schema: Rc<LogicalSchema>,
    pub filter: Rc<LogicalFilter>,
    pub aggregate_multiplied_subqueries: Vec<Rc<AggregateMultipliedSubquery>>,
}

impl LogicalNode for ResolveMultipliedMeasures {
    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::ResolveMultipliedMeasures(self.clone())
    }

    fn inputs(&self) -> Vec<PlanNode> {
        self.aggregate_multiplied_subqueries
            .iter()
            .map(|i| i.as_plan_node())
            .collect()
    }

    fn with_inputs(self: Rc<Self>, inputs: Vec<PlanNode>) -> Result<Rc<Self>, CubeError> {
        check_inputs_len(
            &inputs,
            self.aggregate_multiplied_subqueries.len(),
            self.node_name(),
        )?;

        let aggregate_multiplied_subqueries = inputs
            .iter()
            .map(|i| AggregateMultipliedSubquery::try_from_plan_node(i.clone()))
            .collect::<Result<_, CubeError>>()?;

        Ok(Rc::new(Self {
            schema: self.schema.clone(),
            filter: self.filter.clone(),
            aggregate_multiplied_subqueries,
        }))
    }

    fn node_name(&self) -> &'static str {
        "ResolveMultipliedMeasures"
    }
    fn try_from_plan_node(plan_node: PlanNode) -> Result<Rc<Self>, CubeError> {
        if let PlanNode::ResolveMultipliedMeasures(item) = plan_node {
            Ok(item)
        } else {
            Err(cast_error(&plan_node, "ResolveMultipliedMeasures"))
        }
    }
}

impl PrettyPrint for ResolveMultipliedMeasures {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println("ResolveMultipliedMeasures: ", state);
        let state = state.new_level();
        let details_state = state.new_level();
        result.println("schema:", &state);
        self.schema.pretty_print(result, &details_state);
        result.println("filter:", &state);
        self.filter.pretty_print(result, &details_state);
        result.println("aggregate_multiplied_subqueries:", &state);
        for subquery in self.aggregate_multiplied_subqueries.iter() {
            subquery.pretty_print(result, &details_state);
        }
    }
}
