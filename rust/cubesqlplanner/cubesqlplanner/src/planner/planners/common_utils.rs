use crate::planner::query_tools::QueryTools;
use crate::planner::{BaseCube, BaseDimension, BaseMember};
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
    ) -> Result<Vec<Rc<dyn BaseMember>>, CubeError> {
        let evaluator_compiler_cell = self.query_tools.evaluator_compiler().clone();
        let mut evaluator_compiler = evaluator_compiler_cell.borrow_mut();
        let primary_keys = self
            .query_tools
            .cube_evaluator()
            .static_data()
            .primary_keys
            .get(cube_name)
            .unwrap();

        let dims = primary_keys
            .iter()
            .map(|d| -> Result<_, CubeError> {
                let full_name = format!("{}.{}", cube_name, d);
                let evaluator = evaluator_compiler.add_dimension_evaluator(full_name.clone())?;
                let dim = BaseDimension::try_new_required(evaluator, self.query_tools.clone())?;
                Ok(dim.as_base_member())
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(dims)
    }
}
