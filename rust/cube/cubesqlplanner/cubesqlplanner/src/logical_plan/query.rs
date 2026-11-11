use super::*;
use crate::planner::filter::{map_filter_items, FilterItem};
use crate::planner::planners::multi_stage::EvaluationContext;
use crate::planner::symbols::transforms::inline_input_refs;
use crate::planner::{InputId, MemberSymbol};
use cubenativeutils::CubeError;
use std::rc::Rc;
use typed_builder::TypedBuilder;

/// Body of a query in the logical plan: the data `source`
/// (join / aggregate / pre-aggregation), its output `schema`,
/// `filter` tree and query-level `modifers` (limit / offset / order /
/// ungrouped). Subqueries the source may reference by name live on the
/// top-level `RootQuery` node, never here — so a `Query` can be
/// embedded anywhere (subquery body, dimension subquery) without
/// producing nested `WITH` clauses.
#[derive(Clone, TypedBuilder)]
pub struct Query {
    schema: Rc<LogicalSchema>,
    filter: Rc<LogicalFilter>,
    modifers: Rc<LogicalQueryModifiers>,
    source: Rc<QuerySource>,
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
    /// The request's filters the subqueries this query reads have applied; a
    /// source replacing those subqueries has to apply them itself.
    #[builder(default)]
    inputs_filter: Option<Rc<LogicalFilter>>,
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
    pub fn with_source(self: &Rc<Self>, source: Rc<QuerySource>) -> Rc<Self> {
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
    pub fn source(&self) -> &Rc<QuerySource> {
        &self.source
    }
    pub fn set_source(&mut self, source: Rc<QuerySource>) {
        self.source = source;
    }

    // All the filters of the request, wherever they are applied.
    fn request_filter(&self) -> Rc<LogicalFilter> {
        match &self.inputs_filter {
            Some(inputs_filter) => Rc::new(LogicalFilter {
                dimensions_filters: inputs_filter.dimensions_filters.clone(),
                time_dimensions_filters: inputs_filter.time_dimensions_filters.clone(),
                segments: inputs_filter.segments.clone(),
                measures_filter: self.filter.measures_filter.clone(),
            }),
            None => self.filter.clone(),
        }
    }

    /// The query reading its members and applying its filters itself, for a
    /// source replacing its inputs.
    pub fn without_input_refs(&self) -> Result<Self, CubeError> {
        let query = self.map_symbols(&inline_input_refs)?;
        Ok(Self {
            filter: query.request_filter(),
            inputs_filter: None,
            ..query
        })
    }
}

impl InputReader for Query {
    // Only a query over subqueries reads inputs; one over cubes reads the cubes.
    fn read_inputs(&self) -> Vec<InputId> {
        match self.source.base() {
            InputBase::Merge(merge) => merge.subqueries().iter().map(|r| r.id()).collect(),
            _ => vec![],
        }
    }

    fn map_symbols(
        &self,
        f: &dyn Fn(&Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError>,
    ) -> Result<Self, CubeError> {
        let conditional_measures = self
            .conditional_measures
            .iter()
            .map(|c| -> Result<_, CubeError> {
                Ok(ConditionalMeasure {
                    measure: f(&c.measure)?,
                    base: f(&c.base)?,
                    condition: c.condition.map_symbols(&f)?,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let filter_params_filters = match &self.filter_params_filters {
            Some(filter) => Some(Rc::new(filter.map_symbols(&f)?)),
            None => None,
        };
        let modifers = LogicalQueryModifiers {
            order_by: map_order_by(&self.modifers.order_by, &f)?,
            ..(*self.modifers).clone()
        };
        Ok(Self {
            schema: Rc::new(self.schema.map_symbols(&f)?),
            filter: Rc::new(self.filter.map_symbols(&f)?),
            modifers: Rc::new(modifers),
            source: self.source.clone(),
            conditional_measures,
            evaluation_context: self.evaluation_context.clone(),
            filter_params_filters,
            inputs_filter: match &self.inputs_filter {
                Some(filter) => Some(Rc::new(filter.map_symbols(&f)?)),
                None => None,
            },
        })
    }

    // The filters on dimensions move to `inputs_filter`: the subqueries apply them.
    fn with_input_refs(&self, placement: &InputPlacement) -> Result<Self, CubeError> {
        // Neither is rendered through references, so a query reading subqueries carries none.
        if !self.conditional_measures.is_empty() || self.filter_params_filters.is_some() {
            return Err(CubeError::internal(
                "Query over subqueries cannot carry conditional measures or filter params filters"
                    .to_string(),
            ));
        }
        let filter = LogicalFilter {
            measures_filter: map_filter_items(&self.filter.measures_filter, &|s| {
                placement.place(s)
            })?,
            ..Default::default()
        };
        let inputs_filter = LogicalFilter {
            dimensions_filters: self.filter.dimensions_filters.clone(),
            time_dimensions_filters: self.filter.time_dimensions_filters.clone(),
            segments: self.filter.segments.clone(),
            ..Default::default()
        };
        let modifers = LogicalQueryModifiers {
            order_by: map_order_by(&self.modifers.order_by, &|s| placement.place(s))?,
            ..(*self.modifers).clone()
        };
        Ok(Self {
            schema: Rc::new(self.schema.map_symbols(&|s| placement.place(s))?),
            filter: Rc::new(filter),
            modifers: Rc::new(modifers),
            inputs_filter: Some(Rc::new(inputs_filter)),
            ..self.clone()
        })
    }

    fn visit_rendered_symbols(
        &self,
        f: &mut dyn FnMut(&Rc<MemberSymbol>) -> Result<(), CubeError>,
    ) -> Result<(), CubeError> {
        self.schema.visit_symbols(&mut |s| f(s))?;
        self.filter.visit_symbols(&mut |s| f(s))?;
        visit_rendered_order_by(&self.modifers.order_by, &self.schema, f)
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
            source: source.clone().into_logical_node()?,
            conditional_measures: self.conditional_measures.clone(),
            evaluation_context: self.evaluation_context.clone(),
            filter_params_filters: self.filter_params_filters.clone(),
            inputs_filter: self.inputs_filter.clone(),
        }))
    }

    fn with_forwarded_input(
        self: Rc<Self>,
        from: InputId,
        to: &Rc<SubqueryRef>,
    ) -> Result<Rc<Self>, CubeError> {
        self.forward_input_refs(from, to)
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
        if let Some(inputs_filter) = &self.inputs_filter {
            result.println("inputs_filter:", &state);
            inputs_filter.pretty_print(result, &details_state);
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
