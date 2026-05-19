use super::pretty_print::*;
use super::*;
use crate::planner::SqlCall;
use cubenativeutils::CubeError;
use std::rc::Rc;
use typed_builder::TypedBuilder;

/// One non-root cube of a `LogicalJoin`, with the SQL expression
/// that joins it to the rest of the tree.
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

/// Join of cubes that backs a query source: a `root` cube plus
/// non-root cubes (`joins`), optionally extended by sub-query
/// dimensions that contribute their own joined-in CTEs.
#[derive(Clone, TypedBuilder)]
pub struct LogicalJoin {
    #[builder(default)]
    root: Option<Rc<Cube>>,
    #[builder(default)]
    joins: Vec<LogicalJoinItem>,
}

impl LogicalJoin {
    pub fn root(&self) -> &Option<Rc<Cube>> {
        &self.root
    }

    pub fn joins(&self) -> &Vec<LogicalJoinItem> {
        &self.joins
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
        let LogicalJoinInputUnPacker { root, joins } =
            LogicalJoinInputUnPacker::new(&self, &inputs)?;

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

        let result = Self::builder().root(root).joins(joins).build();

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
        result
    }
}

pub struct LogicalJoinInputUnPacker<'a> {
    root: Option<&'a PlanNode>,
    joins: &'a [PlanNode],
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

        Ok(Self { root, joins })
    }

    fn inputs_len(join: &LogicalJoin) -> usize {
        let root_len = if join.root.is_some() { 1 } else { 0 };
        root_len + join.joins().len()
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
        } else {
            result.println(&format!("Empty source"), state);
        }
    }
}
