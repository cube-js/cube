use super::CommonUtils;
use crate::cube_bridge::join_definition::JoinDefinition;
use crate::cube_bridge::join_item::JoinItem;
use crate::logical_plan::*;
use crate::planner::join_hints::JoinHints;
use crate::planner::query_tools::QueryTools;
use crate::planner::MemberSymbol;
use crate::planner::SqlCall;
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Join item with its members resolved to `MemberSymbol`s on both
/// sides — produced from a `JoinDefinition` by `JoinPlanner` so the
/// rest of the planner can reason about the actual columns involved
/// without re-parsing the JS-side ON clause.
#[derive(Clone, Debug)]
pub struct ResolvedJoinItem {
    pub original_from: String,
    pub original_to: String,
    pub from_members: Vec<Rc<MemberSymbol>>,
    pub to_members: Vec<Rc<MemberSymbol>>,
    pub on_sql: Rc<SqlCall>,
}

impl ResolvedJoinItem {
    /// Equality on the cube pair and the resolved member sets — used
    /// to detect duplicates without comparing the compiled `on_sql`.
    pub fn is_same_as(&self, other: &Self) -> bool {
        self.original_from == other.original_from
            && self.original_to == other.original_to
            && self.from_members == other.from_members
            && self.to_members == other.to_members
    }
}

/// Builds `LogicalJoin` trees from `JoinDefinition`s (or join
/// hints), compiles each item's ON SQL into a `SqlCall`, and exposes
/// helpers for resolving the members each ON clause references.
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

    /// Builds a `LogicalJoin` from join hints, asking the join graph
    /// to materialise the matching `JoinDefinition`.
    pub fn make_join_logical_plan_with_join_hints(
        &self,
        join_hints: JoinHints,
        dimension_subqueries: Vec<Rc<DimensionSubQuery>>,
    ) -> Result<Rc<LogicalJoin>, CubeError> {
        let join = self
            .query_tools
            .join_graph()
            .build_join(join_hints.into_items())?;
        self.make_join_logical_plan(join, dimension_subqueries)
    }

    /// Empty `LogicalJoin` — used when the query needs no joins
    /// (e.g. it touches a single cube and pulls nothing extra).
    pub fn make_empty_join_logical_plan(&self) -> Rc<LogicalJoin> {
        Rc::new(LogicalJoin::builder().build())
    }

    /// Builds a `LogicalJoin` from an already-resolved
    /// `JoinDefinition`, compiling each item's ON SQL and attaching
    /// the given sub-query dimensions.
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
            joins.push(LogicalJoinItem::builder().cube(cube).on_sql(on_sql).build());
        }

        Ok(Rc::new(
            LogicalJoin::builder()
                .root(Some(root))
                .joins(joins)
                .dimension_subqueries(dimension_subqueries)
                .build(),
        ))
    }

    /// Compiles the ON SQL of a join item into a `SqlCall` rooted at
    /// the `from` cube.
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

    /// Materialises the join from `join_hints` and resolves the
    /// members each ON clause references.
    pub fn resolve_join_members_by_hints(
        &self,
        join_hints: &JoinHints,
    ) -> Result<Vec<ResolvedJoinItem>, CubeError> {
        let join = self
            .query_tools
            .join_graph()
            .build_join(join_hints.items().to_vec())?;
        self.resolve_join_members(join)
    }
    /// Resolves the members each ON clause references for an
    /// already-built `JoinDefinition`.
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
