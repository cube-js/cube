use super::*;
use cubenativeutils::CubeError;
use std::rc::Rc;
use typed_builder::TypedBuilder;

/// Reference to a multi-stage CTE by name, with the schema it exposes.
#[derive(Clone, TypedBuilder)]
pub struct MultiStageSubqueryRef {
    name: String,
    schema: Rc<LogicalSchema>,
}

impl MultiStageSubqueryRef {
    pub fn name(&self) -> &String {
        &self.name
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
            &format!("measures: {}", print_symbols(&self.schema.measures)),
            &state,
        );
    }
}

/// Top-level aggregating source that stitches together several
/// multi-stage / multi-fact CTEs into one keyed result. The
/// physical builder picks a join strategy from `multi_stage_subquery_refs`,
/// `keys_input` and the dialect.
#[derive(Clone, TypedBuilder)]
pub struct Merge {
    schema: Rc<LogicalSchema>,
    #[builder(default)]
    multi_stage_subquery_refs: Vec<Rc<MultiStageSubqueryRef>>,
    /// Refs supplying the key grid at a finer grain than the measure refs;
    /// empty when the keys are derived from the measure refs themselves.
    #[builder(default)]
    keys_input: Vec<Rc<MultiStageSubqueryRef>>,
}

impl Merge {
    pub fn schema(&self) -> &Rc<LogicalSchema> {
        &self.schema
    }

    pub fn multi_stage_subquery_refs(&self) -> &Vec<Rc<MultiStageSubqueryRef>> {
        &self.multi_stage_subquery_refs
    }

    pub fn keys_input(&self) -> &Vec<Rc<MultiStageSubqueryRef>> {
        &self.keys_input
    }

    pub fn is_empty(&self) -> bool {
        self.multi_stage_subquery_refs.is_empty()
    }
}

impl LogicalNode for Merge {
    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::Merge(self.clone())
    }

    fn inputs(&self) -> Vec<PlanNode> {
        vec![]
    }

    fn with_inputs(self: Rc<Self>, inputs: Vec<PlanNode>) -> Result<Rc<Self>, CubeError> {
        check_inputs_len(&inputs, 0, self.node_name())?;

        Ok(Rc::new(
            Self::builder()
                .schema(self.schema().clone())
                .multi_stage_subquery_refs(self.multi_stage_subquery_refs().clone())
                .keys_input(self.keys_input.clone())
                .build(),
        ))
    }

    fn referenced_cte_names(&self) -> Vec<String> {
        let mut result: Vec<String> = self
            .multi_stage_subquery_refs
            .iter()
            .map(|r| r.name().clone())
            .collect();
        result.extend(self.keys_input.iter().map(|r| r.name().clone()));
        result
    }

    fn node_name(&self) -> &'static str {
        "Merge"
    }
    fn try_from_plan_node(plan_node: PlanNode) -> Result<Rc<Self>, CubeError> {
        if let PlanNode::Merge(item) = plan_node {
            Ok(item)
        } else {
            Err(cast_error(&plan_node, "Merge"))
        }
    }
}

impl PrettyPrint for Merge {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println("Merge: ", state);
        let state = state.new_level();
        let details_state = state.new_level();
        result.println(&format!("schema:"), &state);
        self.schema().pretty_print(result, &details_state);
        if !self.multi_stage_subquery_refs().is_empty() {
            result.println("multi_stage_subquery_refs:", &state);
            for subquery_ref in self.multi_stage_subquery_refs().iter() {
                subquery_ref.pretty_print(result, &details_state);
            }
        }
        if !self.keys_input.is_empty() {
            result.println("keys_input:", &state);
            for keys_ref in self.keys_input.iter() {
                keys_ref.pretty_print(result, &details_state);
            }
        }
    }
}
