use super::query_tools::QueryTools;
use super::BaseField;
use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::planner::utils::escape_column_name;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct BaseDimension {
    dimension: String,
    query_tools: Rc<QueryTools>,
    index: usize,
}

impl BaseField for BaseDimension {
    fn to_sql(&self) -> Result<String, CubeError> {
        self.sql()
    }

    fn index(&self) -> usize {
        self.index
    }
}

impl BaseDimension {
    pub fn new(dimension: String, query_tools: Rc<QueryTools>, index: usize) -> Rc<Self> {
        Rc::new(Self {
            dimension,
            query_tools,
            index,
        })
    }

    pub fn dimension(&self) -> &String {
        &self.dimension
    }

    fn path(&self) -> Result<Vec<String>, CubeError> {
        self.query_tools
            .cube_evaluator()
            .parse_path("dimensions".to_string(), self.dimension.clone())
    }

    //FIXME May be should be part of BaseField Trait
    pub fn alias_name(&self) -> Result<String, CubeError> {
        self.query_tools.alias_name(&self.dimension)
    }

    pub fn dimension_sql(&self) -> Result<String, CubeError> {
        let path = self.path()?;
        let cube_name = &path[0];
        //Ok(format!("dff{}", cube_name))
        let dimension_definition = self
            .query_tools
            .cube_evaluator()
            .dimension_by_path(self.dimension.clone())?;

        let sql = dimension_definition.sql()?;
        Ok(format!("{}.{}", escape_column_name(cube_name), sql))
    }

    fn sql(&self) -> Result<String, CubeError> {
        let alias_name = escape_column_name(&self.alias_name()?);

        Ok(format!("{} {}", self.dimension_sql()?, alias_name))
    }
}
