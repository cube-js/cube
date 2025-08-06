use super::*;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct ResolveMultipliedMeasures {
    pub schema: Rc<LogicalSchema>,
    pub filter: Rc<LogicalFilter>,
    pub regular_measure_subqueries: Vec<Rc<Query>>,
    pub aggregate_multiplied_subqueries: Vec<Rc<AggregateMultipliedSubquery>>,
}

impl LogicalNode for ResolveMultipliedMeasures {
    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::ResolveMultipliedMeasures(self.clone())
    }

    fn inputs(&self) -> Vec<PlanNode> {
        ResolveMultipliedMeasuresInputPacker::pack(self)
    }

    fn with_inputs(self: Rc<Self>, inputs: Vec<PlanNode>) -> Result<Rc<Self>, CubeError> {
        let ResolveMultipliedMeasuresInputUnPacker {
            regular_measure_subqueries,
            aggregate_multiplied_subqueries,
        } = ResolveMultipliedMeasuresInputUnPacker::new(&self, &inputs)?;

        let regular_measure_subqueries = regular_measure_subqueries
            .iter()
            .map(|i| Query::try_from_plan_node(i.clone()))
            .collect::<Result<_, CubeError>>()?;

        let aggregate_multiplied_subqueries = aggregate_multiplied_subqueries
            .iter()
            .map(|i| AggregateMultipliedSubquery::try_from_plan_node(i.clone()))
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

pub struct ResolveMultipliedMeasuresInputPacker;

impl ResolveMultipliedMeasuresInputPacker {
    pub fn pack(resolve: &ResolveMultipliedMeasures) -> Vec<PlanNode> {
        let mut result = vec![];
        result.extend(
            resolve
                .regular_measure_subqueries
                .iter()
                .map(|i| i.as_plan_node()),
        );
        result.extend(
            resolve
                .aggregate_multiplied_subqueries
                .iter()
                .map(|i| i.as_plan_node()),
        );
        result
    }
}

pub struct ResolveMultipliedMeasuresInputUnPacker<'a> {
    regular_measure_subqueries: &'a [PlanNode],
    aggregate_multiplied_subqueries: &'a [PlanNode],
}

impl<'a> ResolveMultipliedMeasuresInputUnPacker<'a> {
    pub fn new(
        resolve: &ResolveMultipliedMeasures,
        inputs: &'a Vec<PlanNode>,
    ) -> Result<Self, CubeError> {
        check_inputs_len(&inputs, Self::inputs_len(resolve), resolve.node_name())?;

        let regular_end = resolve.regular_measure_subqueries.len();
        let regular_measure_subqueries = &inputs[0..regular_end];
        let aggregate_multiplied_subqueries = &inputs[regular_end..];

        Ok(Self {
            regular_measure_subqueries,
            aggregate_multiplied_subqueries,
        })
    }

    fn inputs_len(resolve: &ResolveMultipliedMeasures) -> usize {
        resolve.regular_measure_subqueries.len() + resolve.aggregate_multiplied_subqueries.len()
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
