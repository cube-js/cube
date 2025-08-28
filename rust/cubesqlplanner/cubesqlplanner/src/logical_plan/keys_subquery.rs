use super::*;
use crate::planner::sql_evaluator::MemberSymbol;
use cubenativeutils::CubeError;
use std::rc::Rc;

#[derive(Clone)]
pub struct KeysSubQuery {
    pub pk_cube: Rc<Cube>,
    pub schema: Rc<LogicalSchema>,
    pub primary_keys_dimensions: Vec<Rc<MemberSymbol>>,
    pub filter: Rc<LogicalFilter>,
    pub source: Rc<LogicalJoin>,
}

impl LogicalNode for KeysSubQuery {
    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::KeysSubQuery(self.clone())
    }

    fn inputs(&self) -> Vec<PlanNode> {
        vec![self.pk_cube.as_plan_node(), self.source.as_plan_node()]
    }

    fn with_inputs(self: Rc<Self>, inputs: Vec<PlanNode>) -> Result<Rc<Self>, CubeError> {
        check_inputs_len(&inputs, 2, self.node_name())?;
        let pk_cube = &inputs[0];
        let source = &inputs[1];

        let res = Self {
            pk_cube: pk_cube.clone().into_logical_node()?,
            schema: self.schema.clone(),
            primary_keys_dimensions: self.primary_keys_dimensions.clone(),
            filter: self.filter.clone(),
            source: source.clone().into_logical_node()?,
        };
        Ok(Rc::new(res))
    }

    fn node_name(&self) -> &'static str {
        "KeysSubQuery"
    }
    fn try_from_plan_node(plan_node: PlanNode) -> Result<Rc<Self>, CubeError> {
        if let PlanNode::KeysSubQuery(query) = plan_node {
            Ok(query)
        } else {
            Err(cast_error(&plan_node, "KeysSubQuery"))
        }
    }
}

impl PrettyPrint for KeysSubQuery {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println("KeysSubQuery: ", state);
        let state = state.new_level();
        let details_state = state.new_level();
        result.println(&format!("pk_cube: {}", self.pk_cube.cube.name()), &state);

        result.println("schema:", &state);
        self.schema.pretty_print(result, &details_state);
        result.println(
            &format!(
                "-primary_keys_dimensions: {}",
                print_symbols(&self.primary_keys_dimensions)
            ),
            &state,
        );
        result.println("filters:", &state);
        self.filter.pretty_print(result, &details_state);
        result.println("source:", &state);
        self.source.pretty_print(result, &details_state);
    }
}
