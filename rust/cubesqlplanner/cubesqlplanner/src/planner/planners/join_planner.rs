use super::CommonUtils;
use crate::cube_bridge::join_definition::JoinDefinition;
use crate::cube_bridge::join_hints::JoinHintItem;
use crate::cube_bridge::join_item::JoinItem;
use crate::logical_plan::*;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::sql_evaluator::SqlCall;
use cubenativeutils::CubeError;
use std::rc::Rc;

#[derive(Clone, Debug)]
pub struct ResolvedJoinItem {
    pub original_from: String,
    pub original_to: String,
    pub from_members: Vec<Rc<MemberSymbol>>,
    pub to_members: Vec<Rc<MemberSymbol>>,
    pub on_sql: Rc<SqlCall>,
}

impl ResolvedJoinItem {
    pub fn is_same_as(&self, other: &Self) -> bool {
        self.original_from == other.original_from
            && self.original_to == other.original_to
            && self.from_members == other.from_members
            && self.to_members == other.to_members
    }
}

pub struct JoinPlanner {
    utils: CommonUtils,
    query_tools: Rc<QueryTools>,
}

impl JoinPlanner {
    pub fn new(query_tools: Rc<QueryTools>) -> Self {
        Self {
            utils: CommonUtils::new(query_tools.clone()),
            query_tools,
        }
    }

    pub fn make_join_logical_plan_with_join_hints(
        &self,
        join_hints: Vec<JoinHintItem>,
        dimension_subqueries: Vec<Rc<DimensionSubQuery>>,
    ) -> Result<Rc<LogicalJoin>, CubeError> {
        let join = self.query_tools.join_graph().build_join(join_hints)?;
        self.make_join_logical_plan(join, dimension_subqueries)
    }

    pub fn make_join_logical_plan(
        &self,
        join: Rc<dyn JoinDefinition>,
        dimension_subqueries: Vec<Rc<DimensionSubQuery>>,
    ) -> Result<Rc<LogicalJoin>, CubeError> {
        let root_definition = self.utils.cube_from_path(join.static_data().root.clone())?;
        let root = Cube::new(root_definition);
        let joins_definitions = join.joins()?;
        let mut joins = vec![];
        for join_definition in joins_definitions.iter() {
            let cube_definition = self
                .utils
                .cube_from_path(join_definition.static_data().original_to.clone())?;
            let cube = Cube::new(cube_definition);
            let on_sql = self.compile_join_condition(join_definition.clone())?;
            joins.push(LogicalJoinItem { cube, on_sql });
        }

        Ok(Rc::new(LogicalJoin {
            root,
            joins,
            dimension_subqueries,
        }))
    }

    pub fn compile_join_condition(
        &self,
        join_item: Rc<dyn JoinItem>,
    ) -> Result<Rc<SqlCall>, CubeError> {
        let definition = join_item.join()?;
        let evaluator_compiler_cell = self.query_tools.evaluator_compiler().clone();
        let mut evaluator_compiler = evaluator_compiler_cell.borrow_mut();
        evaluator_compiler
            .compile_sql_call(&join_item.static_data().original_from, definition.sql()?)
    }

    pub fn resolve_join_members_by_hints(
        &self,
        join_hints: &Vec<JoinHintItem>,
    ) -> Result<Vec<ResolvedJoinItem>, CubeError> {
        let join = self
            .query_tools
            .join_graph()
            .build_join(join_hints.clone())?;
        self.resolve_join_members(join)
    }
    pub fn resolve_join_members(
        &self,
        join: Rc<dyn JoinDefinition>,
    ) -> Result<Vec<ResolvedJoinItem>, CubeError> {
        join.joins()?
            .into_iter()
            .map(|join_item| self.resolve_item_join_members(join_item))
            .collect::<Result<Vec<_>, _>>()
    }

    fn resolve_item_join_members(
        &self,
        join_item: Rc<dyn JoinItem>,
    ) -> Result<ResolvedJoinItem, CubeError> {
        let original_from = join_item.static_data().original_from.clone();
        let original_to = join_item.static_data().original_to.clone();
        let on_sql = self.compile_join_condition(join_item.clone())?;
        let mut from_members = vec![];
        let mut to_members = vec![];
        for member in on_sql.get_dependencies().into_iter() {
            if member.cube_name() == original_from {
                from_members.push(member);
            } else if member.cube_name() == original_to {
                to_members.push(member);
            } else {
                return Err(CubeError::user(format!(
                    "Member {} in join from '{}' to '{}' doesn't reference join cubes",
                    member.full_name(),
                    original_from,
                    original_to
                )));
            }
        }

        if from_members.is_empty() {
            return Err(CubeError::user(format!(
                "From members are not found in join from '{}' to '{}'",
                original_from, original_to
            )));
        }
        if to_members.is_empty() {
            return Err(CubeError::user(format!(
                "To members are not found in join from '{}' to '{}'",
                original_from, original_to
            )));
        }
        Ok(ResolvedJoinItem {
            original_from,
            original_to,
            from_members,
            to_members,
            on_sql,
        })
    }
}
