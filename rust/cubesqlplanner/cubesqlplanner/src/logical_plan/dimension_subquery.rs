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
    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::DimensionSubQuery(self.clone())
    }

    fn inputs(&self) -> Vec<PlanNode> {
        vec![self.query.as_plan_node()]
    }

    fn with_inputs(self: Rc<Self>, inputs: Vec<PlanNode>) -> Result<Rc<Self>, CubeError> {
        check_inputs_len(&inputs, 1, self.node_name())?;
        let query = &inputs[0];
        Ok(Rc::new(Self {
            query: query.clone().into_logical_node()?,
            primary_keys_dimensions: self.primary_keys_dimensions.clone(),
            subquery_dimension: self.subquery_dimension.clone(),
            measure_for_subquery_dimension: self.measure_for_subquery_dimension.clone(),
        }))
    }

    fn node_name(&self) -> &'static str {
        "DimensionSubQuery"
    }
    fn try_from_plan_node(plan_node: PlanNode) -> Result<Rc<Self>, CubeError> {
        if let PlanNode::DimensionSubQuery(query) = plan_node {
            Ok(query)
        } else {
            Err(cast_error(&plan_node, "DimensionSubQuery"))
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
