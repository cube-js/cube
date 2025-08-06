use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::BaseCube;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct CommonUtils {
    query_tools: Rc<QueryTools>,
}

impl CommonUtils {
    pub fn new(query_tools: Rc<QueryTools>) -> Self {
        Self { query_tools }
    }

    pub fn cube_from_path(&self, cube_path: String) -> Result<Rc<BaseCube>, CubeError> {
        let evaluator_compiler_cell = self.query_tools.evaluator_compiler().clone();
        let mut evaluator_compiler = evaluator_compiler_cell.borrow_mut();

        let evaluator = evaluator_compiler.add_cube_table_evaluator(cube_path.to_string())?;
        BaseCube::try_new(cube_path.to_string(), self.query_tools.clone(), evaluator)
    }

    pub fn primary_keys_dimensions(
        &self,
        cube_name: &String,
    ) -> Result<Vec<Rc<MemberSymbol>>, CubeError> {
        let evaluator_compiler_cell = self.query_tools.evaluator_compiler().clone();
        let mut evaluator_compiler = evaluator_compiler_cell.borrow_mut();
        let primary_keys = self
            .query_tools
            .cube_evaluator()
            .static_data()
            .primary_keys
            .get(cube_name)
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
