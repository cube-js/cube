use crate::planner::BaseCube;
use crate::planner::SqlCall;
use std::collections::HashMap;
use std::rc::Rc;

/// One non-root cube of a `JoinTree` with its parent cube and the
/// compiled ON SQL connecting them. `original_from` is the parent in
/// the join graph; the cube itself is the `original_to`.
pub struct JoinTreeItem {
    cube: Rc<BaseCube>,
    original_from: String,
    on_sql: Rc<SqlCall>,
}

impl JoinTreeItem {
    pub fn new(cube: Rc<BaseCube>, original_from: String, on_sql: Rc<SqlCall>) -> Self {
        Self {
            cube,
            original_from,
            on_sql,
        }
    }

    pub fn cube(&self) -> &Rc<BaseCube> {
        &self.cube
    }

    pub fn original_from(&self) -> &str {
        &self.original_from
    }

    pub fn on_sql(&self) -> &Rc<SqlCall> {
        &self.on_sql
    }
}

/// A resolved join tree: the root cube plus its joined cubes with the
/// ON SQL already compiled into `SqlCall`. Built once from a
/// `JoinDefinition` so the rest of planning can assemble `LogicalJoin`
/// nodes (and collect sub-query dimensions) without recompiling the
/// join conditions on every use.
pub struct JoinTree {
    root: Rc<BaseCube>,
    joins: Vec<JoinTreeItem>,
    multiplication_factor: HashMap<String, bool>,
}

impl JoinTree {
    pub fn new(
        root: Rc<BaseCube>,
        joins: Vec<JoinTreeItem>,
        multiplication_factor: HashMap<String, bool>,
    ) -> Rc<Self> {
        Rc::new(Self {
            root,
            joins,
            multiplication_factor,
        })
    }

    pub fn root(&self) -> &Rc<BaseCube> {
        &self.root
    }

    pub fn joins(&self) -> &Vec<JoinTreeItem> {
        &self.joins
    }

    /// Whether joining `cube_name` into this tree multiplies its rows.
    pub fn is_multiplied(&self, cube_name: &str) -> bool {
        self.multiplication_factor
            .get(cube_name)
            .copied()
            .unwrap_or(false)
    }
}
