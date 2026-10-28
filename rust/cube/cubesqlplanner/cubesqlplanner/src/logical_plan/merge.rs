use super::*;
use crate::planner::InputId;
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::rc::Rc;
use typed_builder::TypedBuilder;

/// Reference to a multi-stage subquery by name, with the schema it exposes.
#[derive(Clone, TypedBuilder)]
pub struct SubqueryRef {
    id: InputId,
    name: String,
    schema: Rc<LogicalSchema>,
}

impl SubqueryRef {
    pub fn id(&self) -> InputId {
        self.id
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn schema(&self) -> &Rc<LogicalSchema> {
        &self.schema
    }
}

impl PrettyPrint for SubqueryRef {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println(
            &format!("SubqueryRef: {} {}", self.name(), self.id()),
            state,
        );
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

    fn referenced_inputs(&self) -> Vec<InputId> {
        self.subqueries
            .iter()
            .chain(self.keys.iter())
            .map(|r| r.id())
            .collect()
    }

    // In a list reading `from`, a ref already reading `to` is replaced too, so
    // the legs collapsing onto `to` keep the schema the forward brings. Lists
    // not reading `from` stay as they are.
    fn with_forwarded_input(self: Rc<Self>, from: InputId, to: &Rc<SubqueryRef>) -> Rc<Self> {
        let reads_from = |refs: &Vec<Rc<SubqueryRef>>| refs.iter().any(|r| r.id() == from);
        if !reads_from(&self.subqueries) && !reads_from(&self.keys) {
            return self;
        }
        let forward = |refs: &Vec<Rc<SubqueryRef>>| {
            if !reads_from(refs) {
                return refs.clone();
            }
            refs.iter()
                .map(|r| {
                    if r.id() == from || r.id() == to.id() {
                        to.clone()
                    } else {
                        r.clone()
                    }
                })
                .unique_by(|r| r.id())
                .collect_vec()
        };
        Rc::new(
            Self::builder()
                .schema(self.schema.clone())
                .subqueries(forward(&self.subqueries))
                .keys(forward(&self.keys))
                .build(),
        )
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
