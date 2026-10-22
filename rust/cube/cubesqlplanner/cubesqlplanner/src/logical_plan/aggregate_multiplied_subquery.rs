use super::pretty_print::*;
use super::*;
use crate::planner::planners::multi_stage::EvaluationContext;
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;
use std::rc::Rc;

logical_source_enum!(AggregateMultipliedSubquerySource, [Cube, Query]);

/// Subquery that aggregates a multiplied measure: a `keys_subquery`
/// produces the unique key set of `pk_cube` (its `primary_keys_dimensions`
/// plus the query dimensions), a `source` (the cube, or a query
/// projecting row-level measure values per key) supplies the values, optional
/// `dimension_subqueries` materialise sub-query dimensions, and
/// `pre_aggregation_override` lets a matched pre-aggregation
/// short-circuit the whole CTE.
#[derive(Clone)]
pub struct AggregateMultipliedSubquery {
    pub schema: Rc<LogicalSchema>,
    pub keys_subquery: Rc<Query>,
    pub pk_cube: Rc<Cube>,
    pub primary_keys_dimensions: Vec<Rc<MemberSymbol>>,
    pub source: AggregateMultipliedSubquerySource,
    pub dimension_subqueries: Vec<Rc<DimensionSubQuery>>,
    // `Some` when this CTE was hoisted out of a multi-stage leaf
    // scope: the leaf's evaluation, whose time shifts it renders
    // under. `None` for top-level multiplied measures — the ambient
    // build context applies.
    pub evaluation_context: Option<EvaluationContext>,
    // When Some, physical builder short-circuits to this query instead of
    // rendering the native multiplied-subquery SELECT. Set by the pre-aggregation
    // optimizer when a matching pre-aggregation replaces this CTE.
    pub pre_aggregation_override: Option<Rc<Query>>,
}

impl AggregateMultipliedSubquery {
    pub fn with_pre_aggregation_override(&self, query: Rc<Query>) -> Rc<Self> {
        Rc::new(Self {
            pre_aggregation_override: Some(query),
            ..self.clone()
        })
    }
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
            pk_cube: self.pk_cube.clone(),
            primary_keys_dimensions: self.primary_keys_dimensions.clone(),
            source: self.source.with_plan_node(source.clone())?,
            dimension_subqueries: dimension_subqueries
                .iter()
                .map(|itm| itm.clone().into_logical_node())
                .collect::<Result<Vec<_>, _>>()?,
            evaluation_context: self.evaluation_context.clone(),
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
        result.println(&format!("pk_cube: {}", self.pk_cube.cube_id()), &state);
        result.println(
            &format!(
                "primary_keys_dimensions: {}",
                print_symbols(&self.primary_keys_dimensions)
            ),
            &state,
        );
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
