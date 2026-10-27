use super::*;
use cubenativeutils::CubeError;
use std::rc::Rc;
use typed_builder::TypedBuilder;

/// Reference to a multi-stage subquery by name, with the schema it exposes.
#[derive(Clone, TypedBuilder)]
pub struct SubqueryRef {
    name: String,
    schema: Rc<LogicalSchema>,
}

impl SubqueryRef {
    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn schema(&self) -> &Rc<LogicalSchema> {
        &self.schema
    }
}

impl PrettyPrint for SubqueryRef {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println(&format!("SubqueryRef: {}", self.name()), state);
        let state = state.new_level();
        result.println(
            &format!("measures: {}", print_symbols(&self.schema.measures)),
            &state,
        );
    }
}

/// Top-level aggregating source that stitches together several
/// multi-stage / multi-fact subqueries into one keyed result. The
/// physical builder picks a join strategy from `subqueries`,
/// `keys` and the dialect.
#[derive(Clone, TypedBuilder)]
pub struct Merge {
    schema: Rc<LogicalSchema>,
    #[builder(default)]
    subqueries: Vec<Rc<SubqueryRef>>,
    /// Refs supplying the key grid at a finer grain than the measure refs;
    /// empty when the keys are derived from the measure refs themselves.
    #[builder(default)]
    keys: Vec<Rc<SubqueryRef>>,
}

impl Merge {
    pub fn schema(&self) -> &Rc<LogicalSchema> {
        &self.schema
    }

    pub fn subqueries(&self) -> &Vec<Rc<SubqueryRef>> {
        &self.subqueries
    }

    pub fn keys(&self) -> &Vec<Rc<SubqueryRef>> {
        &self.keys
    }

    pub fn is_empty(&self) -> bool {
        self.subqueries.is_empty()
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
                .subqueries(self.subqueries().clone())
                .keys(self.keys.clone())
                .build(),
        ))
    }

    fn referenced_subqueries(&self) -> Vec<String> {
        let mut result: Vec<String> = self.subqueries.iter().map(|r| r.name().clone()).collect();
        result.extend(self.keys.iter().map(|r| r.name().clone()));
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
        if !self.subqueries().is_empty() {
            result.println("subqueries:", &state);
            for subquery_ref in self.subqueries().iter() {
                subquery_ref.pretty_print(result, &details_state);
            }
        }
        if !self.keys.is_empty() {
            result.println("keys:", &state);
            for keys_ref in self.keys.iter() {
                keys_ref.pretty_print(result, &details_state);
            }
        }
    }
}
