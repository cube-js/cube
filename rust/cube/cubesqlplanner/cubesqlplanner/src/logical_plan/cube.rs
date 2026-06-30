use super::*;
use crate::planner::BaseCube;
use cubenativeutils::CubeError;
use std::rc::Rc;
use typed_builder::TypedBuilder;

/// Marker for an "original SQL" pre-aggregation attached to a cube
/// — the physical builder uses its name to substitute the cube's
/// table expression with the matching pre-aggregation source.
#[derive(Clone, TypedBuilder)]
pub struct OriginalSqlPreAggregation {
    name: String,
}

impl OriginalSqlPreAggregation {
    pub fn name(&self) -> &String {
        &self.name
    }
}

impl PrettyPrint for OriginalSqlPreAggregation {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println(
            &format!("OriginalSqlPreAggregation: {}", self.name()),
            state,
        );
    }
}

/// A cube referenced from the logical plan — wraps the planner's
/// `BaseCube` and optionally pins a matching "original SQL"
/// pre-aggregation as the cube's source.
#[derive(Clone, TypedBuilder)]
pub struct Cube {
    cube: Rc<BaseCube>,
    #[builder(default)]
    original_sql_pre_aggregation: Option<OriginalSqlPreAggregation>,
}

impl Cube {
    pub fn name(&self) -> &String {
        &self.cube.name()
    }

    pub fn cube(&self) -> &Rc<BaseCube> {
        &self.cube
    }

    pub fn original_sql_pre_aggregation(&self) -> &Option<OriginalSqlPreAggregation> {
        &self.original_sql_pre_aggregation
    }
}

impl PrettyPrint for Cube {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println(&format!("Cube: {}", self.name()), state);
        if let Some(original_sql_pre_aggregation) = self.original_sql_pre_aggregation() {
            original_sql_pre_aggregation.pretty_print(result, state);
        }
    }
}

impl Cube {
    pub fn new(cube: Rc<BaseCube>) -> Rc<Self> {
        Rc::new(Self::builder().cube(cube).build())
    }

    pub fn with_original_sql_pre_aggregation(
        self: Rc<Self>,
        original_sql_pre_aggregation: OriginalSqlPreAggregation,
    ) -> Rc<Self> {
        Rc::new(
            Self::builder()
                .cube(self.cube().clone())
                .original_sql_pre_aggregation(Some(original_sql_pre_aggregation))
                .build(),
        )
    }
}

impl LogicalNode for Cube {
    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::Cube(self.clone())
    }

    fn inputs(&self) -> Vec<PlanNode> {
        vec![] // Cube has no inputs
    }

    fn with_inputs(self: Rc<Self>, inputs: Vec<PlanNode>) -> Result<Rc<Self>, CubeError> {
        check_inputs_len(&inputs, 0, self.node_name())?;
        Ok(self)
    }

    fn node_name(&self) -> &'static str {
        "Cube"
    }
    fn try_from_plan_node(plan_node: PlanNode) -> Result<Rc<Self>, CubeError> {
        if let PlanNode::Cube(item) = plan_node {
            Ok(item)
        } else {
            Err(cast_error(&plan_node, "Cube"))
        }
    }
}
