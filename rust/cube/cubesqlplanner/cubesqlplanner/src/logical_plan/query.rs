use super::*;
use cubenativeutils::CubeError;
use std::rc::Rc;
use typed_builder::TypedBuilder;

/// Root node of a query in the logical plan: the data `source`
/// (join / aggregate / pre-aggregation), its output `schema`,
/// `filter` tree, query-level `modifers` (limit / offset / order /
/// ungrouped), and the multi-stage CTEs the source depends on.
#[derive(Clone, TypedBuilder)]
pub struct Query {
    /// Computed-dimension CTE references this Query consumes. Each ref
    /// carries its own join strategy (`OnPrimaryKeys` for the ex-DSQ
    /// pattern, `OnOuterDimensions` for the multi-stage-dim pattern).
    /// At render time the processor passes these into the source
    /// rendering context — they don't get embedded into `LogicalJoin` /
    /// `FullKeyAggregate`. Bodies live on the surrounding `LogicalPlan`.
    #[builder(default)]
    multi_stage_dimensions: Vec<Rc<MultiStageDimensionRef>>,
    schema: Rc<LogicalSchema>,
    #[builder(default)]
    filter: Rc<LogicalFilter>,
    #[builder(default)]
    modifers: Rc<LogicalQueryModifiers>,
    source: QuerySource,
    /// Explicit role of this Query in the multi-stage pipeline. Planner
    /// places set this at construction; consumers (QueryProcessor,
    /// pre-aggregation optimizer) match on it to pick the right render
    /// path. Role-specific data (partition_by, multi-stage dimension,
    /// etc.) lives inside the matching variant.
    #[builder(default)]
    kind: QueryKind,
}

impl Query {
    pub fn multi_stage_dimensions(&self) -> &Vec<Rc<MultiStageDimensionRef>> {
        &self.multi_stage_dimensions
    }
    pub fn schema(&self) -> &Rc<LogicalSchema> {
        &self.schema
    }
    pub fn filter(&self) -> &Rc<LogicalFilter> {
        &self.filter
    }
    pub fn modifers(&self) -> &Rc<LogicalQueryModifiers> {
        &self.modifers
    }
    pub fn source(&self) -> &QuerySource {
        &self.source
    }
    pub fn set_source(&mut self, source: QuerySource) {
        self.source = source;
    }
    pub fn kind(&self) -> &QueryKind {
        &self.kind
    }

    pub fn with_modifers(self: &Rc<Self>, modifers: Rc<LogicalQueryModifiers>) -> Rc<Self> {
        Rc::new(Self {
            multi_stage_dimensions: self.multi_stage_dimensions.clone(),
            schema: self.schema.clone(),
            filter: self.filter.clone(),
            modifers,
            source: self.source.clone(),
            kind: self.kind.clone(),
        })
    }

    /// Replace the published `multi_stage_dimensions` refs.
    pub fn with_multi_stage_dimensions(
        self: &Rc<Self>,
        multi_stage_dimensions: Vec<Rc<MultiStageDimensionRef>>,
    ) -> Rc<Self> {
        Rc::new(Self {
            multi_stage_dimensions,
            schema: self.schema.clone(),
            filter: self.filter.clone(),
            modifers: self.modifers.clone(),
            source: self.source.clone(),
            kind: self.kind.clone(),
        })
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
        Ok(Rc::new(Self {
            multi_stage_dimensions: self.multi_stage_dimensions.clone(),
            schema: self.schema.clone(),
            filter: self.filter.clone(),
            modifers: self.modifers.clone(),
            source: self.source.with_plan_node(inputs[0].clone())?,
            kind: self.kind.clone(),
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
        self.kind.pretty_print(result, &state);
        if !self.multi_stage_dimensions.is_empty() {
            result.println("multi_stage_dimensions:", &state);
            for msd in self.multi_stage_dimensions.iter() {
                msd.pretty_print(result, &details_state);
            }
        }

        result.println("schema:", &state);
        self.schema.pretty_print(result, &details_state);
        result.println("filters:", &state);
        self.filter.pretty_print(result, &details_state);
        self.modifers.pretty_print(result, &state);

        result.println("source:", &state);
        self.source.pretty_print(result, &details_state);
    }
}
