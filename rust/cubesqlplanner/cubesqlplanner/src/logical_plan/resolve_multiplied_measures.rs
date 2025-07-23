use super::*;
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::rc::Rc;

pub struct ResolveMultipliedMeasures {
    pub schema: Rc<LogicalSchema>,
    pub filter: Rc<LogicalFilter>,
    pub regular_measure_subqueries: Vec<Rc<Query>>,
    pub aggregate_multiplied_subqueries: Vec<Rc<AggregateMultipliedSubquery>>,
}

impl LogicalNode for ResolveMultipliedMeasures {
    type InputsType = ResolveMultipliedMeasuresInput;

    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::ResolveMultipliedMeasures(self.clone())
    }

    fn inputs(&self) -> Self::InputsType {
        let regular_measure_subqueries = self
            .regular_measure_subqueries
            .iter()
            .map(|i| i.as_plan_node())
            .collect_vec();
        let aggregate_multiplied_subqueries = self
            .aggregate_multiplied_subqueries
            .iter()
            .map(|i| i.as_plan_node())
            .collect_vec();
        ResolveMultipliedMeasuresInput::new(
            regular_measure_subqueries,
            aggregate_multiplied_subqueries,
        )
    }

    fn with_inputs(self: Rc<Self>, inputs: Self::InputsType) -> Result<Rc<Self>, CubeError> {
        let (regular_measure_subqueries, aggregate_multiplied_subqueries) = inputs.unpack();

        let regular_measure_subqueries = regular_measure_subqueries
            .into_iter()
            .map(|i| Query::try_from_plan_node(i))
            .collect::<Result<_, CubeError>>()?;

        let aggregate_multiplied_subqueries = aggregate_multiplied_subqueries
            .into_iter()
            .map(|i| AggregateMultipliedSubquery::try_from_plan_node(i))
            .collect::<Result<_, CubeError>>()?;

        Ok(Rc::new(Self {
            schema: self.schema.clone(),
            filter: self.filter.clone(),
            regular_measure_subqueries,
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

pub struct ResolveMultipliedMeasuresInput {
    regular_measure_subqueries: Vec<PlanNode>,
    aggregate_multiplied_subqueries: Vec<PlanNode>,
}

impl ResolveMultipliedMeasuresInput {
    pub fn new(
        regular_measure_subqueries: Vec<PlanNode>,
        aggregate_multiplied_subqueries: Vec<PlanNode>,
    ) -> Self {
        Self {
            regular_measure_subqueries,
            aggregate_multiplied_subqueries,
        }
    }

    pub fn unpack(self) -> (Vec<PlanNode>, Vec<PlanNode>) {
        let Self {
            regular_measure_subqueries,
            aggregate_multiplied_subqueries,
        } = self;
        (regular_measure_subqueries, aggregate_multiplied_subqueries)
    }
}

impl NodeInputs for ResolveMultipliedMeasuresInput {
    fn iter(&self) -> Box<dyn Iterator<Item = &PlanNode> + '_> {
        Box::new(
            self.regular_measure_subqueries
                .iter()
                .chain(self.aggregate_multiplied_subqueries.iter()),
        )
    }

    fn iter_mut(&mut self) -> Box<dyn Iterator<Item = &mut PlanNode> + '_> {
        Box::new(
            self.regular_measure_subqueries
                .iter_mut()
                .chain(self.aggregate_multiplied_subqueries.iter_mut()),
        )
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
        result.println("regular_measure_subqueries:", &state);
        for subquery in self.regular_measure_subqueries.iter() {
            subquery.pretty_print(result, &details_state);
        }
        result.println("aggregate_multiplied_subqueries:", &state);
        for subquery in self.aggregate_multiplied_subqueries.iter() {
            subquery.pretty_print(result, &details_state);
        }
    }
}
