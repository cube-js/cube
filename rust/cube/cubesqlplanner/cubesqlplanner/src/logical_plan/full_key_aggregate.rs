use super::*;
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;
use std::rc::Rc;
use typed_builder::TypedBuilder;

/// Reference to a multi-stage CTE consumed by `FullKeyAggregate`:
/// the CTE's name plus the symbols it exposes.
#[derive(TypedBuilder)]
pub struct MultiStageSubqueryRef {
    name: String,
    #[builder(default)]
    symbols: Vec<Rc<MemberSymbol>>,
    schema: Rc<LogicalSchema>,
}

impl MultiStageSubqueryRef {
    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn symbols(&self) -> &Vec<Rc<MemberSymbol>> {
        &self.symbols
    }

    pub fn schema(&self) -> &Rc<LogicalSchema> {
        &self.schema
    }
}

impl PrettyPrint for MultiStageSubqueryRef {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println(&format!("MultiStageSubqueryRef: {}", self.name()), state);
        let state = state.new_level();
        result.println(
            &format!("symbols: {}", print_symbols(self.symbols())),
            &state,
        );
    }
}

/// Dim-grid source for the JOIN-based assembly: a set of CTE refs that
/// supply the keys grid plus the explicit list of dim symbols to use as
/// JOIN-keys against the measure-side `multi_stage_subquery_refs`.
#[derive(Clone, TypedBuilder)]
pub struct FullKeyAggregateKeysInput {
    #[builder(default)]
    refs: Vec<Rc<MultiStageSubqueryRef>>,
    #[builder(default)]
    keys: Vec<Rc<MemberSymbol>>,
}

impl FullKeyAggregateKeysInput {
    pub fn refs(&self) -> &Vec<Rc<MultiStageSubqueryRef>> {
        &self.refs
    }

    pub fn keys(&self) -> &Vec<Rc<MemberSymbol>> {
        &self.keys
    }

    pub fn is_empty(&self) -> bool {
        self.refs.is_empty() && self.keys.is_empty()
    }
}

impl PrettyPrint for FullKeyAggregateKeysInput {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println("FullKeyAggregateKeysInput:", state);
        let inner = state.new_level();
        if !self.refs.is_empty() {
            result.println("refs:", &inner);
            let details = inner.new_level();
            for r in self.refs.iter() {
                r.pretty_print(result, &details);
            }
        }
        if !self.keys.is_empty() {
            result.println(&format!("keys: {}", print_symbols(&self.keys)), &inner);
        }
    }
}

/// Top-level aggregating source that stitches together several
/// multi-stage / multi-fact CTEs into one keyed result. The
/// physical builder picks a join strategy from `multi_stage_subquery_refs`
/// and `use_full_join_and_coalesce`.
#[derive(Clone, TypedBuilder)]
pub struct FullKeyAggregate {
    schema: Rc<LogicalSchema>,
    use_full_join_and_coalesce: bool,
    #[builder(default)]
    multi_stage_subquery_refs: Vec<Rc<MultiStageSubqueryRef>>,
    /// Optional dim-grid input to LEFT JOIN measure-side refs against.
    /// `None` for the legacy window-based assembly; populated for the
    /// JOIN-based path (non-additive measures with reduce_by/group_by).
    #[builder(default)]
    keys_input: Option<Rc<FullKeyAggregateKeysInput>>,
}

impl FullKeyAggregate {
    pub fn schema(&self) -> &Rc<LogicalSchema> {
        &self.schema
    }

    /// When true, multi-fact branches are stitched together via a
    /// FULL OUTER JOIN over keys with COALESCE on dimension columns;
    /// otherwise an INNER JOIN is used.
    pub fn use_full_join_and_coalesce(&self) -> bool {
        self.use_full_join_and_coalesce
    }

    pub fn multi_stage_subquery_refs(&self) -> &Vec<Rc<MultiStageSubqueryRef>> {
        &self.multi_stage_subquery_refs
    }

    pub fn keys_input(&self) -> Option<&Rc<FullKeyAggregateKeysInput>> {
        self.keys_input.as_ref()
    }

    pub fn is_empty(&self) -> bool {
        self.multi_stage_subquery_refs.is_empty()
    }
}

impl LogicalNode for FullKeyAggregate {
    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::FullKeyAggregate(self.clone())
    }

    fn inputs(&self) -> Vec<PlanNode> {
        vec![]
    }

    fn with_inputs(self: Rc<Self>, inputs: Vec<PlanNode>) -> Result<Rc<Self>, CubeError> {
        check_inputs_len(&inputs, 0, self.node_name())?;

        Ok(Rc::new(
            Self::builder()
                .schema(self.schema().clone())
                .use_full_join_and_coalesce(self.use_full_join_and_coalesce())
                .multi_stage_subquery_refs(self.multi_stage_subquery_refs().clone())
                .keys_input(self.keys_input.clone())
                .build(),
        ))
    }

    fn node_name(&self) -> &'static str {
        "FullKeyAggregate"
    }
    fn try_from_plan_node(plan_node: PlanNode) -> Result<Rc<Self>, CubeError> {
        if let PlanNode::FullKeyAggregate(item) = plan_node {
            Ok(item)
        } else {
            Err(cast_error(&plan_node, "FullKeyAggregate"))
        }
    }
}

impl PrettyPrint for FullKeyAggregate {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println("FullKeyAggregate: ", state);
        let state = state.new_level();
        let details_state = state.new_level();
        result.println(&format!("schema:"), &state);
        self.schema().pretty_print(result, &details_state);
        result.println(
            &format!(
                "use_full_join_and_coalesce: {}",
                self.use_full_join_and_coalesce()
            ),
            &state,
        );
        if !self.multi_stage_subquery_refs().is_empty() {
            result.println("multi_stage_subquery_refs:", &state);
            for subquery_ref in self.multi_stage_subquery_refs().iter() {
                subquery_ref.pretty_print(result, &details_state);
            }
        }
        if let Some(keys_input) = self.keys_input() {
            result.println("keys_input:", &state);
            keys_input.pretty_print(result, &details_state);
        }
    }
}
