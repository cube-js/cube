use crate::cube_bridge::cube_definition::CubeDefinition;
use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::planner::utils::escape_column_name;
use cubenativeutils::CubeError;
use std::rc::Rc;
pub struct BaseCube {
    cube_evaluator: Rc<dyn CubeEvaluator>,
    cube_definition: Rc<dyn CubeDefinition>,
}
impl BaseCube {
    pub fn new(
        cube_evaluator: Rc<dyn CubeEvaluator>,
        cube_definition: Rc<dyn CubeDefinition>,
    ) -> Rc<Self> {
        Rc::new(Self {
            cube_evaluator,
            cube_definition,
        })
    }

    pub fn to_sql(&self) -> Result<String, CubeError> {
        let cube_sql = self.cube_definition.sql_table()?;
        let cube_alias = self.cube_alias()?;
        let as_syntax_join = "AS"; //FIXME should be from JS BaseQuery

        Ok(format!("{} {} {}", cube_sql, as_syntax_join, cube_alias))
    }

    fn cube_alias(&self) -> Result<String, CubeError> {
        Ok(escape_column_name(&self.cube_definition.static_data().name))
    }
}
