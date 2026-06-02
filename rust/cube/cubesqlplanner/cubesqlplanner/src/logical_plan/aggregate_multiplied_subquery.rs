use super::pretty_print::*;
use super::*;
use cubenativeutils::CubeError;
use std::rc::Rc;

logical_source_enum!(AggregateMultipliedSubquerySource, [Cube, MeasureSubquery]);

/// Subquery that aggregates a multiplied measure: a `keys_subquery`
/// produces the unique key set, a `source` (cube or
/// `MeasureSubquery`) supplies the values, optional
/// `dimension_subqueries` materialise sub-query dimensions, and
/// `pre_aggregation_override` lets a matched pre-aggregation
/// short-circuit the whole CTE.
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

        // When pre_aggregation_override is set, the override is the only
        // rendered branch and `inputs()` packs only it — `keys_subquery`,
        // `source`, `dimension_subqueries` are reused unchanged from `self`.
        let (keys_subquery, source, dimension_subqueries) = if pre_aggregation_override.is_some() {
            (
                self.keys_subquery.clone(),
                self.source.clone(),
                self.dimension_subqueries.clone(),
            )
        } else {
            (
                keys_subquery.unwrap().clone().into_logical_node()?,
                self.source.with_plan_node(source.unwrap().clone())?,
                dimension_subqueries
                    .unwrap()
                    .iter()
                    .map(|itm| itm.clone().into_logical_node())
                    .collect::<Result<Vec<_>, _>>()?,
            )
        };

        let result = Self {
            schema: self.schema.clone(),
            keys_subquery,
            source,
            dimension_subqueries,
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
        // When a pre-aggregation matched this multiplied subquery, the override
        // is the only branch ever rendered. Pack only it so that plan walkers
        // (cube-name collection, future rewriters, visitors) don't follow the
        // dead `keys_subquery` / `source` / `dimension_subqueries` branches —
        // those still reference raw cube tables and would leak `cube.table`
        // identifiers into engines that don't have them (e.g. CubeStore).
        if let Some(override_query) = &aggregate.pre_aggregation_override {
            return vec![override_query.as_plan_node()];
        }
        let mut result = vec![];
        result.push(aggregate.keys_subquery.as_plan_node());
        result.push(aggregate.source.as_plan_node());
        result.extend(
            aggregate
                .dimension_subqueries
                .iter()
                .map(|itm| itm.as_plan_node()),
        );
        result
    }
}

pub struct AggregateMultipliedSubqueryInputUnPacker<'a> {
    keys_subquery: Option<&'a PlanNode>,
    source: Option<&'a PlanNode>,
    dimension_subqueries: Option<&'a [PlanNode]>,
    pre_aggregation_override: Option<&'a PlanNode>,
}

impl<'a> AggregateMultipliedSubqueryInputUnPacker<'a> {
    pub fn new(
        aggregate: &AggregateMultipliedSubquery,
        inputs: &'a Vec<PlanNode>,
    ) -> Result<Self, CubeError> {
        check_inputs_len(&inputs, Self::inputs_len(aggregate), aggregate.node_name())?;

        if aggregate.pre_aggregation_override.is_some() {
            return Ok(Self {
                keys_subquery: None,
                source: None,
                dimension_subqueries: None,
                pre_aggregation_override: Some(&inputs[0]),
            });
        }

        let keys_subquery = &inputs[0];
        let source = &inputs[1];
        let dim_end = 2 + aggregate.dimension_subqueries.len();
        let dimension_subqueries = &inputs[2..dim_end];

        Ok(Self {
            keys_subquery: Some(keys_subquery),
            source: Some(source),
            dimension_subqueries: Some(dimension_subqueries),
            pre_aggregation_override: None,
        })
    }

    fn inputs_len(aggregate: &AggregateMultipliedSubquery) -> usize {
        if aggregate.pre_aggregation_override.is_some() {
            1
        } else {
            2 + aggregate.dimension_subqueries.len()
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
