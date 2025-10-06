use super::pretty_print::*;
use super::*;
use crate::planner::sql_evaluator::SqlCall;
use cubenativeutils::CubeError;
use std::rc::Rc;
use typed_builder::TypedBuilder;

#[derive(Clone, TypedBuilder)]
pub struct LogicalJoinItem {
    cube: Rc<Cube>,
    on_sql: Rc<SqlCall>,
}

impl LogicalJoinItem {
    pub fn cube(&self) -> &Rc<Cube> {
        &self.cube
    }

    pub fn on_sql(&self) -> &Rc<SqlCall> {
        &self.on_sql
    }
}

impl PrettyPrint for LogicalJoinItem {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println(&format!("CubeJoinItem: "), state);
        let details_state = state.new_level();
        self.cube().pretty_print(result, &details_state);
    }
}

#[derive(Clone, TypedBuilder)]
pub struct LogicalJoin {
    #[builder(default)]
    root: Option<Rc<Cube>>,
    #[builder(default)]
    joins: Vec<LogicalJoinItem>,
    #[builder(default)]
    dimension_subqueries: Vec<Rc<DimensionSubQuery>>,
}

impl LogicalJoin {
    pub fn root(&self) -> &Option<Rc<Cube>> {
        &self.root
    }

    pub fn joins(&self) -> &Vec<LogicalJoinItem> {
        &self.joins
    }

    pub fn dimension_subqueries(&self) -> &Vec<Rc<DimensionSubQuery>> {
        &self.dimension_subqueries
    }
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

        let root = if let Some(r) = root {
            Some(r.clone().into_logical_node()?)
        } else {
            None
        };

        let joins = self
            .joins()
            .iter()
            .zip(joins.iter())
            .map(|(self_item, item)| -> Result<_, CubeError> {
                Ok(LogicalJoinItem::builder()
                    .cube(item.clone().into_logical_node()?)
                    .on_sql(self_item.on_sql().clone())
                    .build())
            })
            .collect::<Result<Vec<_>, _>>()?;

        let result = Self::builder()
            .root(root)
            .joins(joins)
            .dimension_subqueries(
                dimension_subqueries
                    .iter()
                    .map(|itm| itm.clone().into_logical_node())
                    .collect::<Result<Vec<_>, _>>()?,
            )
            .build();

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
        if let Some(root) = join.root() {
            result.push(root.as_plan_node());
        }
        result.extend(join.joins().iter().map(|item| item.cube().as_plan_node()));
        result.extend(
            join.dimension_subqueries()
                .iter()
                .map(|item| item.as_plan_node()),
        );
        result
    }
}

pub struct LogicalJoinInputUnPacker<'a> {
    root: Option<&'a PlanNode>,
    joins: &'a [PlanNode],
    dimension_subqueries: &'a [PlanNode],
}

impl<'a> LogicalJoinInputUnPacker<'a> {
    pub fn new(join: &LogicalJoin, inputs: &'a Vec<PlanNode>) -> Result<Self, CubeError> {
        check_inputs_len(&inputs, Self::inputs_len(join), join.node_name())?;

        let mut joins_start = 0;
        let root = if join.root.is_some() {
            joins_start = 1;
            Some(&inputs[0])
        } else {
            None
        };

        let joins_end = joins_start + join.joins().len();
        let joins = &inputs[joins_start..joins_end];
        let dimension_subqueries = &inputs[joins_end..];

        Ok(Self {
            root,
            joins,
            dimension_subqueries,
        })
    }

    fn inputs_len(join: &LogicalJoin) -> usize {
        1 + join.joins().len() + join.dimension_subqueries().len()
    }
}

impl PrettyPrint for LogicalJoin {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        if let Some(root) = self.root() {
            result.println(&format!("Join: "), state);

            let state = state.new_level();
            let details_state = state.new_level();
            result.println(&format!("root: "), &state);
            root.pretty_print(result, &details_state);
            result.println(&format!("joins: "), &state);
            let state = state.new_level();
            for join in self.joins().iter() {
                join.pretty_print(result, &state);
            }
            if !self.dimension_subqueries().is_empty() {
                result.println("dimension_subqueries:", &state);
                let details_state = state.new_level();
                for subquery in self.dimension_subqueries().iter() {
                    subquery.pretty_print(result, &details_state);
                }
            }
        } else {
            result.println(&format!("Empty source"), state);
        }
    }
}
