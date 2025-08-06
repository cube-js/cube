use super::pretty_print::*;
use super::*;
use crate::planner::sql_evaluator::SqlCall;
use cubenativeutils::CubeError;
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
    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::LogicalJoin(self.clone())
    }

    fn inputs(&self) -> Vec<PlanNode> {
        LogicalJoinInputPacker::pack(self)
    }

    fn with_inputs(self: Rc<Self>, inputs: Vec<PlanNode>) -> Result<Rc<Self>, CubeError> {
        let LogicalJoinInputUnPacker {
            root,
            joins,
            dimension_subqueries,
        } = LogicalJoinInputUnPacker::new(&self, &inputs)?;

        let joins = self
            .joins
            .iter()
            .zip(joins.iter())
            .map(|(self_item, item)| -> Result<_, CubeError> {
                Ok(LogicalJoinItem {
                    cube: item.clone().into_logical_node()?,
                    on_sql: self_item.on_sql.clone(),
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let result = Self {
            root: root.clone().into_logical_node()?,
            joins,
            dimension_subqueries: dimension_subqueries
                .iter()
                .map(|itm| itm.clone().into_logical_node())
                .collect::<Result<Vec<_>, _>>()?,
        };

        Ok(Rc::new(result))
    }

    fn node_name(&self) -> &'static str {
        "LogicalJoin"
    }
    fn try_from_plan_node(plan_node: PlanNode) -> Result<Rc<Self>, CubeError> {
        if let PlanNode::LogicalJoin(item) = plan_node {
            Ok(item)
        } else {
            Err(cast_error(&plan_node, "LogicalJoin"))
        }
    }
}

pub struct LogicalJoinInputPacker;

impl LogicalJoinInputPacker {
    pub fn pack(join: &LogicalJoin) -> Vec<PlanNode> {
        let mut result = vec![];
        result.push(join.root.as_plan_node());
        result.extend(join.joins.iter().map(|item| item.cube.as_plan_node()));
        result.extend(
            join.dimension_subqueries
                .iter()
                .map(|item| item.as_plan_node()),
        );
        result
    }
}

pub struct LogicalJoinInputUnPacker<'a> {
    root: &'a PlanNode,
    joins: &'a [PlanNode],
    dimension_subqueries: &'a [PlanNode],
}

impl<'a> LogicalJoinInputUnPacker<'a> {
    pub fn new(join: &LogicalJoin, inputs: &'a Vec<PlanNode>) -> Result<Self, CubeError> {
        check_inputs_len(&inputs, Self::inputs_len(join), join.node_name())?;

        let root = &inputs[0];
        let joins_start = 1;
        let joins_end = joins_start + join.joins.len();
        let joins = &inputs[joins_start..joins_end];
        let dimension_subqueries = &inputs[joins_end..];

        Ok(Self {
            root,
            joins,
            dimension_subqueries,
        })
    }

    fn inputs_len(join: &LogicalJoin) -> usize {
        1 + join.joins.len() + join.dimension_subqueries.len()
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
