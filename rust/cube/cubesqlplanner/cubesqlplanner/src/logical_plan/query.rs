use super::*;
use crate::planner::filter::FilterItem;
use crate::planner::planners::multi_stage::EvaluationContext;
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
    /// Schema measures rendered as `base` aggregated over the rows `condition`
    /// keeps; the query's filter must keep every row any condition needs.
    #[builder(default)]
    conditional_measures: Vec<ConditionalMeasure>,
    /// How this query's values are evaluated when it is a multi-stage leaf.
    /// `None` inherits the evaluation of the enclosing query.
    #[builder(default)]
    evaluation_context: Option<EvaluationContext>,
    /// Filters an enclosing select applies on this query's behalf. They are
    /// not a WHERE here; the `FILTER_PARAMS` and `FILTER_GROUP` bindings of
    /// the sources resolve against them.
    #[builder(default)]
    filter_params_filters: Option<Rc<LogicalFilter>>,
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
    pub fn with_source(self: &Rc<Self>, source: QuerySource) -> Rc<Self> {
        Rc::new(Self {
            source,
            ..(**self).clone()
        })
    }
    pub fn with_schema_filter_and_modifers(
        self: &Rc<Self>,
        schema: Rc<LogicalSchema>,
        filter: Rc<LogicalFilter>,
        modifers: Rc<LogicalQueryModifiers>,
    ) -> Rc<Self> {
        Rc::new(Self {
            schema,
            filter,
            modifers,
            ..(**self).clone()
        })
    }
    pub fn filter_params_filters(&self) -> &Option<Rc<LogicalFilter>> {
        &self.filter_params_filters
    }
    pub fn evaluation_context(&self) -> &Option<EvaluationContext> {
        &self.evaluation_context
    }
    pub fn with_evaluation_context(
        self: &Rc<Self>,
        evaluation_context: Option<EvaluationContext>,
    ) -> Rc<Self> {
        Rc::new(Self {
            evaluation_context,
            ..(**self).clone()
        })
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
            evaluation_context: self.evaluation_context.clone(),
            filter_params_filters: self.filter_params_filters.clone(),
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

        if let Some(evaluation_context) = &self.evaluation_context {
            evaluation_context.pretty_print(result, &state);
        }

        result.println("schema:", &state);
        self.schema.pretty_print(result, &details_state);
        result.println("filters:", &state);
        self.filter.pretty_print(result, &details_state);
        if let Some(filter_params_filters) = &self.filter_params_filters {
            result.println("filter_params_filters:", &state);
            filter_params_filters.pretty_print(result, &details_state);
        }
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

impl PrettyPrint for EvaluationContext {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        if self.measure_for_ungrouped {
            result.println("measure_for_ungrouped: true", state);
        }
        if !self.time_shifts.is_empty() {
            result.println("time_shifts:", state);
            let details_state = state.new_level();
            for time_shift in self.time_shifts.dimensions_shifts.values() {
                result.println(
                    &format!(
                        "- {}: {}",
                        time_shift.dimension.full_name(),
                        if let Some(interval) = &time_shift.interval {
                            interval.to_sql()
                        } else if let Some(name) = &time_shift.name {
                            format!("{} (named)", name.to_string())
                        } else {
                            "None".to_string()
                        }
                    ),
                    &details_state,
                );
            }
        }
    }
}
