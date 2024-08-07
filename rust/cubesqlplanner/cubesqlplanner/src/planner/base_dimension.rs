use super::query_tools::QueryTools;
use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::planner::utils::escape_column_name;
use convert_case::{Case, Casing};
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct BaseDimension {
    dimension: String,
    query_tools: Rc<QueryTools>,
}

impl BaseDimension {
    pub fn new(dimension: String, query_tools: Rc<QueryTools>) -> Rc<Self> {
        Rc::new(Self {
            dimension,
            query_tools,
        })
    }
    pub fn to_sql(&self) -> Result<String, CubeError> {
        self.sql()
    }

    fn path(&self) -> Result<Vec<String>, CubeError> {
        self.query_tools
            .cube_evaluator()
            .parse_path("dimensions".to_string(), self.dimension.clone())
    }

    fn sql(&self) -> Result<String, CubeError> {
        let path = self.path()?;
        let cube_name = &path[0];

        //Ok(format!("dff{}", cube_name))
        let dimension_definition = self
            .query_tools
            .cube_evaluator()
            .dimension_by_path(self.dimension.clone())?;

        let sql = dimension_definition.sql()?;

        let alias_name = escape_column_name(&self.alias_name()?);

        Ok(format!(
            "{}.{} {}",
            escape_column_name(cube_name),
            sql,
            alias_name
        ))
    }

    fn alias_name(&self) -> Result<String, CubeError> {
        Ok(self.dimension.to_case(Case::Snake).replace(".", "__"))
    }
}
