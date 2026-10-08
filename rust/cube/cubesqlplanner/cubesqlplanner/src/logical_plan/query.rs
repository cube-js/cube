use super::*;
use crate::planner::filter::FilterItem;
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;
use std::rc::Rc;
use typed_builder::TypedBuilder;

/// Body of a query in the logical plan: the data `source`
/// (join / aggregate / pre-aggregation), its output `schema`,
/// `filter` tree and query-level `modifers` (limit / offset / order /
/// ungrouped). CTEs the source may reference by name live on the
/// top-level `RootQuery` node, never here — so a `Query` can be
/// embedded anywhere (CTE member, dimension subquery) without
/// producing nested `WITH` clauses.
#[derive(Clone, TypedBuilder)]
pub struct Query {
    schema: Rc<LogicalSchema>,
    filter: Rc<LogicalFilter>,
    modifers: Rc<LogicalQueryModifiers>,
    source: QuerySource,
    /// Schema measures rendered as their base measure aggregated over the
    /// rows a condition keeps, instead of from their own definition. They
    /// let one scan stand in for several that differ only in which rows they
    /// read; the query's filter keeps the rows any of them needs.
    #[builder(default)]
    conditional_measures: Vec<ConditionalMeasure>,
}

#[derive(Clone, Debug)]
pub struct ConditionalMeasure {
    pub measure: Rc<MemberSymbol>,
    pub base: Rc<MemberSymbol>,
    pub condition: FilterItem,
}

impl Query {
    pub fn schema(&self) -> &Rc<LogicalSchema> {
        &self.schema
    }
    pub fn filter(&self) -> &Rc<LogicalFilter> {
        &self.filter
    }
    pub fn modifers(&self) -> &Rc<LogicalQueryModifiers> {
        &self.modifers
    }
    pub fn conditional_measures(&self) -> &Vec<ConditionalMeasure> {
        &self.conditional_measures
    }
    pub fn source(&self) -> &QuerySource {
        &self.source
    }
    pub fn set_source(&mut self, source: QuerySource) {
        self.source = source;
    }
}

impl LogicalNode for Query {
    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::Query(self.clone())
    }

    fn inputs(&self) -> Vec<PlanNode> {
        vec![self.source.as_plan_node()]
    }

    fn with_inputs(self: Rc<Self>, inputs: Vec<PlanNode>) -> Result<Rc<Self>, CubeError> {
        check_inputs_len(&inputs, 1, self.node_name())?;
        let source = &inputs[0];

        Ok(Rc::new(Self {
            schema: self.schema.clone(),
            filter: self.filter.clone(),
            modifers: self.modifers.clone(),
            source: self.source.with_plan_node(source.clone())?,
            conditional_measures: self.conditional_measures.clone(),
        }))
    }

    fn node_name(&self) -> &'static str {
        "Query"
    }
    fn try_from_plan_node(plan_node: PlanNode) -> Result<Rc<Self>, CubeError> {
        if let PlanNode::Query(query) = plan_node {
            Ok(query)
        } else {
            Err(cast_error(&plan_node, "Query"))
        }
    }
}

impl PrettyPrint for Query {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println("Query: ", state);
        let state = state.new_level();
        let details_state = state.new_level();

        result.println("schema:", &state);
        self.schema.pretty_print(result, &details_state);
        result.println("filters:", &state);
        self.filter.pretty_print(result, &details_state);
        for conditional in self.conditional_measures.iter() {
            result.println(
                &format!(
                    "conditional measure: {} = {} where:",
                    conditional.measure.full_name(),
                    conditional.base.full_name(),
                ),
                &state,
            );
            pretty_print_filter_item(result, &details_state, &conditional.condition);
        }
        self.modifers.pretty_print(result, &state);

        result.println("source:", &state);
        self.source.pretty_print(result, &details_state);
    }
}
