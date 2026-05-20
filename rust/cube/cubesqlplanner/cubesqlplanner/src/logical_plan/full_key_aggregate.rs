use super::*;
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;
use std::rc::Rc;
use typed_builder::TypedBuilder;

/// Reference to a multi-stage CTE consumed by `FullKeyAggregate`:
/// the CTE's name plus the symbols it exposes.
#[derive(Clone, TypedBuilder)]
pub struct MultiStageSubqueryRef {
    name: String,
    #[builder(default)]
    symbols: Vec<Rc<MemberSymbol>>,
    schema: Rc<LogicalSchema>,
    /// True when the CTE behind this ref projects measures as ungrouped raw
    /// columns (no aggregate wrap yet) — the consumer of this ref must
    /// register an `ungrouped_measure_reference` for each measure symbol,
    /// so its own outer SELECT wraps the column in the right aggregate.
    /// Used by the aggregate-multiplied subquery shape: its MeasureSubquery
    /// data input is ungrouped, while keys/regular-measure refs are not.
    #[builder(default)]
    is_ungrouped: bool,
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

    pub fn is_ungrouped(&self) -> bool {
        self.is_ungrouped
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

/// Top-level aggregating source that stitches together several
/// multi-stage / multi-fact CTEs into one keyed result. The physical
/// builder picks a join strategy from `data_inputs` and
/// `keys_subquery_ref` — when a keys CTE is present, joins go through
/// it on `join_keys`; otherwise data inputs are stitched directly.
#[derive(Clone, TypedBuilder)]
pub struct FullKeyAggregate {
    schema: Rc<LogicalSchema>,
    #[builder(default)]
    data_inputs: Vec<Rc<MultiStageSubqueryRef>>,
    #[builder(default)]
    keys_subquery_ref: Option<Rc<MultiStageSubqueryRef>>,
    // Members used as the JOIN keys when stitching `data_queries` onto the
    // keys source. When empty, defaults to `schema.all_dimensions()` —
    // historical behaviour for the multi-stage flow, where outer dimensions
    // are both projected and used as join columns. When non-empty,
    // decouples "what to project" (schema) from "what to join on" — needed
    // for the multiplied-measures flow where pk dimensions drive the join
    // while outer dimensions ride along as payload.
    #[builder(default)]
    join_keys: Vec<Rc<MemberSymbol>>,
}

impl FullKeyAggregate {
    pub fn schema(&self) -> &Rc<LogicalSchema> {
        &self.schema
    }

    pub fn data_inputs(&self) -> &Vec<Rc<MultiStageSubqueryRef>> {
        &self.data_inputs
    }

    pub fn keys_subquery_ref(&self) -> &Option<Rc<MultiStageSubqueryRef>> {
        &self.keys_subquery_ref
    }

    pub fn join_keys(&self) -> &Vec<Rc<MemberSymbol>> {
        &self.join_keys
    }

    pub fn is_empty(&self) -> bool {
        self.data_inputs.is_empty()
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
                .data_inputs(self.data_inputs().clone())
                .keys_subquery_ref(self.keys_subquery_ref().clone())
                .join_keys(self.join_keys().clone())
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
        if !self.data_inputs().is_empty() {
            result.println("data_inputs:", &state);
            for input in self.data_inputs().iter() {
                input.pretty_print(result, &details_state);
            }
        }
        if let Some(keys_ref) = self.keys_subquery_ref() {
            result.println("keys_subquery_ref:", &state);
            keys_ref.pretty_print(result, &details_state);
        }
        if !self.join_keys.is_empty() {
            result.println(
                &format!("join_keys: {}", print_symbols(self.join_keys())),
                &state,
            );
        }
    }
}
