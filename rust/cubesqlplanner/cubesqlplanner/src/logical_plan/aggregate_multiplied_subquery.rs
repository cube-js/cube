use super::pretty_print::*;
use super::*;
use cubenativeutils::CubeError;
use std::rc::Rc;

logical_source_enum!(AggregateMultipliedSubquerySource, [Cube, MeasureSubquery]);

pub struct AggregateMultipliedSubquery {
    pub schema: Rc<LogicalSchema>,
    pub keys_subquery: Rc<KeysSubQuery>,
    pub source: AggregateMultipliedSubquerySource,
    pub dimension_subqueries: Vec<Rc<DimensionSubQuery>>,
    // When Some, physical builder short-circuits to this query instead of
    // rendering the native multiplied-subquery SELECT. Set by the pre-aggregation
    // optimizer when a matching pre-aggregation replaces this CTE.
    pub pre_aggregation_override: Option<Rc<Query>>,
}

impl LogicalNode for AggregateMultipliedSubquery {
    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::AggregateMultipliedSubquery(self.clone())
    }

    fn inputs(&self) -> Vec<PlanNode> {
        AggregateMultipliedSubqueryInputPacker::pack(self)
    }

    fn with_inputs(self: Rc<Self>, inputs: Vec<PlanNode>) -> Result<Rc<Self>, CubeError> {
        let AggregateMultipliedSubqueryInputUnPacker {
            keys_subquery,
            source,
            dimension_subqueries,
            pre_aggregation_override,
        } = AggregateMultipliedSubqueryInputUnPacker::new(&self, &inputs)?;

        let result = Self {
            schema: self.schema.clone(),
            keys_subquery: keys_subquery.clone().into_logical_node()?,
            source: self.source.with_plan_node(source.clone())?,
            dimension_subqueries: dimension_subqueries
                .iter()
                .map(|itm| itm.clone().into_logical_node())
                .collect::<Result<Vec<_>, _>>()?,
            pre_aggregation_override: match pre_aggregation_override {
                Some(node) => Some(node.clone().into_logical_node()?),
                None => None,
            },
        };

        Ok(Rc::new(result))
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

pub struct AggregateMultipliedSubqueryInputPacker;

impl AggregateMultipliedSubqueryInputPacker {
    pub fn pack(aggregate: &AggregateMultipliedSubquery) -> Vec<PlanNode> {
        let mut result = vec![];
        result.push(aggregate.keys_subquery.as_plan_node());
        result.push(aggregate.source.as_plan_node());
        result.extend(
            aggregate
                .dimension_subqueries
                .iter()
                .map(|itm| itm.as_plan_node()),
        );
        if let Some(override_query) = &aggregate.pre_aggregation_override {
            result.push(override_query.as_plan_node());
        }
        result
    }
}

pub struct AggregateMultipliedSubqueryInputUnPacker<'a> {
    keys_subquery: &'a PlanNode,
    source: &'a PlanNode,
    dimension_subqueries: &'a [PlanNode],
    pre_aggregation_override: Option<&'a PlanNode>,
}

impl<'a> AggregateMultipliedSubqueryInputUnPacker<'a> {
    pub fn new(
        aggregate: &AggregateMultipliedSubquery,
        inputs: &'a Vec<PlanNode>,
    ) -> Result<Self, CubeError> {
        check_inputs_len(&inputs, Self::inputs_len(aggregate), aggregate.node_name())?;

        let keys_subquery = &inputs[0];
        let source = &inputs[1];
        let dim_end = 2 + aggregate.dimension_subqueries.len();
        let dimension_subqueries = &inputs[2..dim_end];
        let pre_aggregation_override = if aggregate.pre_aggregation_override.is_some() {
            Some(&inputs[dim_end])
        } else {
            None
        };

        Ok(Self {
            keys_subquery,
            source,
            dimension_subqueries,
            pre_aggregation_override,
        })
    }

    fn inputs_len(aggregate: &AggregateMultipliedSubquery) -> usize {
        2 + aggregate.dimension_subqueries.len()
            + if aggregate.pre_aggregation_override.is_some() {
                1
            } else {
                0
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
        result.println("keys_subquery:", &state);
        self.keys_subquery.pretty_print(result, &details_state);
        result.println("source:", &state);
        self.source.pretty_print(result, &details_state);
        if !self.dimension_subqueries.is_empty() {
            result.println("dimension_subqueries:", &state);
            let details_state = state.new_level();
            for subquery in self.dimension_subqueries.iter() {
                subquery.pretty_print(result, &details_state);
            }
        }
        if let Some(override_query) = &self.pre_aggregation_override {
            result.println("pre_aggregation_override:", &state);
            override_query.pretty_print(result, &details_state);
        }
    }
}
