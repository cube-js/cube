use super::*;
use crate::planner::filter::FilterItem;
use crate::planner::planners::multi_stage::EvaluationContext;
use crate::planner::{JoinTree, MemberSymbol};
use cubenativeutils::CubeError;
use std::rc::Rc;
use typed_builder::TypedBuilder;

/// Body of a query in the logical plan: the rows it reads, its output and filters.
/// The subqueries it reads live on `RootQuery`, so a `Query` nests without `WITH`.
#[derive(Clone, TypedBuilder)]
pub struct Query {
    schema: Rc<LogicalSchema>,
    filter: Rc<LogicalFilter>,
    modifers: Rc<LogicalQueryModifiers>,
    from: Rc<LogicalFrom>,
    /// The join of the query's cubes: which rows it means. An optimizer
    /// replacing `from` keeps it.
    #[builder(default)]
    join: Option<Rc<JoinTree>>,
    /// How its select applies `filter` when the subqueries it reads applied part of
    /// it. `None`: the select aggregates the rows it reads and applies all of it.
    #[builder(default)]
    placement: Option<Rc<FilterPlacement>>,
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
    /// The query reading the rows of `from` itself, so its select applies all of its filters.
    pub fn reading_rows_of(self: &Rc<Self>, from: Rc<LogicalFrom>) -> Rc<Self> {
        Rc::new(Self {
            from,
            placement: None,
            ..(**self).clone()
        })
    }
    pub fn with_schema_and_modifers(
        self: &Rc<Self>,
        schema: Rc<LogicalSchema>,
        modifers: Rc<LogicalQueryModifiers>,
    ) -> Rc<Self> {
        Rc::new(Self {
            schema,
            modifers,
            ..(**self).clone()
        })
    }
    /// The query with `f` applied to every symbol of its filters, the part its
    /// select applies included.
    pub fn map_filter_symbols(
        self: &Rc<Self>,
        f: &impl Fn(&Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError>,
    ) -> Result<Rc<Self>, CubeError> {
        let placement = match &self.placement {
            Some(placement) => Some(Rc::new(FilterPlacement {
                own: Rc::new(placement.own.map_symbols(f)?),
                ..placement.as_ref().clone()
            })),
            None => None,
        };
        Ok(Rc::new(Self {
            filter: Rc::new(self.filter.map_symbols(f)?),
            placement,
            ..(**self).clone()
        }))
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
    pub fn from(&self) -> &Rc<LogicalFrom> {
        &self.from
    }
    pub fn join(&self) -> &Option<Rc<JoinTree>> {
        &self.join
    }
    /// What its select applies of `filter`.
    pub fn applied_filter(&self) -> &Rc<LogicalFilter> {
        match &self.placement {
            Some(placement) => &placement.own,
            None => &self.filter,
        }
    }
    /// Whether its select applies all of `filter`, none of it left to its subqueries.
    pub fn applies_whole_filter(&self) -> bool {
        self.placement.is_none()
    }
    pub fn aggregates(&self) -> bool {
        self.placement
            .as_ref()
            .is_none_or(|placement| placement.aggregates)
    }
    pub fn unrenderable_filter(&self) -> Option<&String> {
        self.placement
            .as_ref()
            .and_then(|placement| placement.unrenderable.as_ref())
    }
}

impl LogicalNode for Query {
    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::Query(self.clone())
    }

    fn inputs(&self) -> Vec<PlanNode> {
        vec![self.from.as_plan_node()]
    }

    fn with_inputs(self: Rc<Self>, inputs: Vec<PlanNode>) -> Result<Rc<Self>, CubeError> {
        check_inputs_len(&inputs, 1, self.node_name())?;
        let from: Rc<LogicalFrom> = inputs[0].clone().into_logical_node()?;
        if !self.aggregates() && from.as_merge().is_none() {
            return Err(CubeError::internal(
                "Only a query over merged subqueries reads aggregated rows".to_string(),
            ));
        }
        Ok(Rc::new(Self {
            from,
            ..(*self).clone()
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
        if let Some(placement) = &self.placement {
            result.println("own filters:", &state);
            placement.own.pretty_print(result, &details_state);
            if !placement.aggregates {
                result.println("aggregates: false", &state);
            }
            if let Some(reason) = &placement.unrenderable {
                result.println(&format!("unrenderable filter: {}", reason), &state);
            }
        }
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

        if let Some(join) = &self.join {
            result.println(&format!("join root: {}", join.root().cube_id()), &state);
        }
        result.println("from:", &state);
        self.from.pretty_print(result, &details_state);
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

/// What a select reading subqueries applies of its query's filters; the
/// subqueries applied the rest.
#[derive(Clone)]
pub struct FilterPlacement {
    own: Rc<LogicalFilter>,
    aggregates: bool,
    unrenderable: Option<String>,
}

impl FilterPlacement {
    /// Over merged legs, which are aggregated already: the select applies their
    /// measure filters to the rows it reads.
    pub fn over_merged_legs(measures_filter: Vec<FilterItem>) -> Self {
        Self {
            own: Rc::new(LogicalFilter {
                measures_filter,
                ..Default::default()
            }),
            aggregates: false,
            unrenderable: None,
        }
    }

    /// Over subqueries the select aggregates, applying `own` itself.
    pub fn aggregating(own: LogicalFilter) -> Self {
        Self {
            own: Rc::new(own),
            aggregates: true,
            unrenderable: None,
        }
    }

    /// A select that can't apply its part of the filters, for `reason`; the
    /// error is raised only if it is built.
    pub fn unrenderable(reason: String) -> Self {
        Self {
            own: Rc::new(LogicalFilter::default()),
            aggregates: true,
            unrenderable: Some(reason),
        }
    }

    pub fn own(&self) -> &Rc<LogicalFilter> {
        &self.own
    }

    /// Whether the select aggregates the rows it reads.
    pub fn aggregates(&self) -> bool {
        self.aggregates
    }

    pub fn unrenderable_reason(&self) -> Option<&String> {
        self.unrenderable.as_ref()
    }
}
