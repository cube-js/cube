use super::pretty_print::*;
use super::*;
use crate::planner::sql_evaluator::MemberSymbol;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct DimensionSubQuery {
    pub query: Rc<Query>,
    pub primary_keys_dimensions: Vec<Rc<MemberSymbol>>,
    pub subquery_dimension: Rc<MemberSymbol>,
    pub measure_for_subquery_dimension: Rc<MemberSymbol>,
}

impl LogicalNode for DimensionSubQuery {
    type InputsType = SingleNodeInput;

    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::DimensionSubQuery(self.clone())
    }

    fn inputs(&self) -> Self::InputsType {
        SingleNodeInput::new(self.query.as_plan_node())
    }

    fn with_inputs(self: Rc<Self>, inputs: Self::InputsType) -> Result<Rc<Self>, CubeError> {
        let query = inputs.unpack();
        Ok(Rc::new(Self {
            query: query.into_logical_node()?,
            primary_keys_dimensions: self.primary_keys_dimensions.clone(),
            subquery_dimension: self.subquery_dimension.clone(),
            measure_for_subquery_dimension: self.measure_for_subquery_dimension.clone(),
        }))
    }

    fn node_name() -> &'static str {
        "DimensionSubQuery"
    }
    fn try_from_plan_node(plan_node: PlanNode) -> Result<Rc<Self>, CubeError> {
        if let PlanNode::DimensionSubQuery(query) = plan_node {
            Ok(query)
        } else {
            Err(cast_error::<Self>(&plan_node))
        }
    }
}

impl PrettyPrint for DimensionSubQuery {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println("DimensionSubQuery: ", state);
        let state = state.new_level();
        let details_state = state.new_level();
        result.println(&format!("query: "), &state);
        self.query.pretty_print(result, &details_state);
        result.println(
            &format!(
                "-primary_keys_dimensions: {}",
                print_symbols(&self.primary_keys_dimensions)
            ),
            &state,
        );
        result.println(
            &format!(
                "-subquery_dimension: {}",
                self.subquery_dimension.full_name()
            ),
            &state,
        );
        result.println(
            &format!(
                "-measure_for_subquery_dimension: {}",
                self.measure_for_subquery_dimension.full_name()
            ),
            &state,
        );
    }
}
