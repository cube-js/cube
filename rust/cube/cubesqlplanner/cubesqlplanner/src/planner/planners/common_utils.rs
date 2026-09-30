use crate::cube_bridge::join_item::JoinItem;
use crate::planner::state::State;
use crate::planner::MemberSymbol;
use crate::planner::SqlCall;
use crate::planner::{BaseCube, CubeId};
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Small helpers shared between planners — cube resolution, join-item
/// ON SQL compilation and the list of primary-key dimensions for a
/// given cube.
pub struct CommonUtils {
    query_tools: Rc<State>,
}

impl CommonUtils {
    pub fn new(query_tools: Rc<State>) -> Self {
        Self { query_tools }
    }

    /// Compiles the ON SQL of a join item into a `SqlCall` rooted at
    /// the `from` cube.
    pub fn compile_join_condition(
        &self,
        join_item: Rc<dyn JoinItem>,
    ) -> Result<Rc<SqlCall>, CubeError> {
        let definition = join_item.join()?;
        let evaluator_compiler_cell = self.query_tools.compiler().clone();
        let mut evaluator_compiler = evaluator_compiler_cell.borrow_mut();
        let from = CubeId::cube(join_item.static_data().original_from.clone());
        evaluator_compiler.compile_sql_call(&from, definition.sql()?)
    }

    /// Resolves the planner-level `BaseCube` for the given cube path.
    pub fn cube_from_path(&self, cube: &CubeId) -> Result<Rc<BaseCube>, CubeError> {
        let evaluator_compiler_cell = self.query_tools.compiler().clone();
        let mut evaluator_compiler = evaluator_compiler_cell.borrow_mut();

        let evaluator = evaluator_compiler.add_cube_table_evaluator(cube.clone(), vec![])?;
        BaseCube::try_new(
            cube.clone(),
            self.query_tools.query_tools().clone(),
            evaluator,
        )
    }

    /// Primary-key dimensions of `cube_name` as planner
    /// `MemberSymbol`s.
    pub fn primary_keys_dimensions(
        &self,
        cube_name: &CubeId,
    ) -> Result<Vec<Rc<MemberSymbol>>, CubeError> {
        let evaluator_compiler_cell = self.query_tools.compiler().clone();
        let mut evaluator_compiler = evaluator_compiler_cell.borrow_mut();
        let primary_keys = self
            .query_tools
            .cube_evaluator()
            .static_data()
            .primary_keys
            .get(cube_name.target())
            .cloned()
            .unwrap_or_else(|| vec![]);

        let dims = primary_keys
            .iter()
            .map(|d| -> Result<_, CubeError> {
                let full_name = format!("{}.{}", cube_name, d);
                let symbol = evaluator_compiler.add_dimension_evaluator(full_name.clone())?;
                Ok(symbol)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(dims)
    }
}
