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
    type InputsType = KeysSubQueryInputs;

    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::KeysSubQuery(self.clone())
    }

    fn inputs(&self) -> Self::InputsType {
        KeysSubQueryInputs {
            pk_cube: self.pk_cube.as_plan_node(),
            source: self.source.as_plan_node(),
        }
    }

    fn with_inputs(self: Rc<Self>, inputs: Self::InputsType) -> Result<Rc<Self>, CubeError> {
        let KeysSubQueryInputs { pk_cube, source } = inputs;

        let res = Self {
            pk_cube: pk_cube.into_logical_node()?,
            schema: self.schema.clone(),
            primary_keys_dimensions: self.primary_keys_dimensions.clone(),
            filter: self.filter.clone(),
            source: source.into_logical_node()?,
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

pub struct KeysSubQueryInputs {
    pub pk_cube: PlanNode,
    pub source: PlanNode,
}

impl NodeInputs for KeysSubQueryInputs {
    fn iter(&self) -> Box<dyn Iterator<Item = &PlanNode> + '_> {
        Box::new(std::iter::once(&self.pk_cube).chain(std::iter::once(&self.source)))
    }

    fn iter_mut(&mut self) -> Box<dyn Iterator<Item = &mut PlanNode> + '_> {
        Box::new(std::iter::once(&mut self.pk_cube).chain(std::iter::once(&mut self.source)))
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
