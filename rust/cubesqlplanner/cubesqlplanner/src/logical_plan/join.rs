use super::pretty_print::*;
use super::*;
use crate::planner::sql_evaluator::SqlCall;
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::rc::Rc;

#[derive(Clone)]
pub struct LogicalJoinItem {
    pub cube: Rc<Cube>,
    pub on_sql: Rc<SqlCall>,
}

impl PrettyPrint for LogicalJoinItem {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println(&format!("CubeJoinItem: "), state);
        let details_state = state.new_level();
        self.cube.pretty_print(result, &details_state);
    }
}

#[derive(Clone)]
pub struct LogicalJoin {
    pub root: Rc<Cube>,
    pub joins: Vec<LogicalJoinItem>,
    pub dimension_subqueries: Vec<Rc<DimensionSubQuery>>,
}

impl LogicalNode for LogicalJoin {
    type InputsType = LogicalJoinInput;

    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::LogicalJoin(self.clone())
    }

    fn inputs(&self) -> Self::InputsType {
        let root = self.root.as_plan_node();
        let joins = self
            .joins
            .iter()
            .map(|itm| itm.cube.as_plan_node())
            .collect_vec();
        let dimension_subqueries = self
            .dimension_subqueries
            .iter()
            .map(|itm| itm.as_plan_node())
            .collect_vec();
        LogicalJoinInput {
            root,
            joins,
            dimension_subqueries,
        }
    }

    fn with_inputs(self: Rc<Self>, inputs: Self::InputsType) -> Result<Rc<Self>, CubeError> {
        let LogicalJoinInput {
            root,
            joins,
            dimension_subqueries,
        } = inputs;

        check_inputs_len::<Self>("joins", &joins, self.joins.len())?;

        check_inputs_len::<Self>(
            "dimension_subqueries",
            &dimension_subqueries,
            self.dimension_subqueries.len(),
        )?;

        let joins = self
            .joins
            .iter()
            .zip(joins.into_iter())
            .map(|(self_item, item)| -> Result<_, CubeError> {
                Ok(LogicalJoinItem {
                    cube: item.into_logical_node()?,
                    on_sql: self_item.on_sql.clone(),
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let result = Self {
            root: root.into_logical_node()?,
            joins,
            dimension_subqueries: dimension_subqueries
                .into_iter()
                .map(|itm| itm.into_logical_node())
                .collect::<Result<Vec<_>, _>>()?,
        };

        Ok(Rc::new(result))
    }

    fn node_name() -> &'static str {
        "LogicalJoin"
    }
    fn try_from_plan_node(plan_node: PlanNode) -> Result<Rc<Self>, CubeError> {
        if let PlanNode::LogicalJoin(item) = plan_node {
            Ok(item)
        } else {
            Err(cast_error::<Self>(&plan_node))
        }
    }
}

pub struct LogicalJoinInput {
    pub root: PlanNode,
    pub joins: Vec<PlanNode>,
    pub dimension_subqueries: Vec<PlanNode>,
}

impl NodeInputs for LogicalJoinInput {
    fn iter(&self) -> Box<dyn Iterator<Item = &PlanNode> + '_> {
        Box::new(
            std::iter::once(&self.root)
                .chain(self.joins.iter())
                .chain(self.dimension_subqueries.iter()),
        )
    }
}

impl PrettyPrint for LogicalJoin {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println(&format!("Join: "), state);
        let state = state.new_level();
        let details_state = state.new_level();
        result.println(&format!("root: "), &state);
        self.root.pretty_print(result, &details_state);
        result.println(&format!("joins: "), &state);
        let state = state.new_level();
        for join in self.joins.iter() {
            join.pretty_print(result, &state);
        }
        if !self.dimension_subqueries.is_empty() {
            result.println("dimension_subqueries:", &state);
            let details_state = state.new_level();
            for subquery in self.dimension_subqueries.iter() {
                subquery.pretty_print(result, &details_state);
            }
        }
    }
}
