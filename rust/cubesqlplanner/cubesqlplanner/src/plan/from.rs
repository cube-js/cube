use super::Join;
use super::QueryPlan;
use crate::planner::{BaseCube, Context};
use cubenativeutils::CubeError;
use std::rc::Rc;

#[derive(Clone)]
pub enum FromSource {
    Empty,
    Cube(Rc<BaseCube>),
    Join(Rc<Join>),
    Subquery(Rc<QueryPlan>, String),
}

#[derive(Clone)]
pub struct From {
    pub source: FromSource,
}

impl From {
    pub fn new(source: FromSource) -> Self {
        Self { source }
    }

    pub fn new_from_cube(cube: Rc<BaseCube>) -> Self {
        Self::new(FromSource::Cube(cube))
    }

    pub fn new_from_join(join: Rc<Join>) -> Self {
        Self::new(FromSource::Join(join))
    }

    pub fn new_from_subquery(plan: Rc<QueryPlan>, alias: String) -> Self {
        Self::new(FromSource::Subquery(plan, alias))
    }

    pub fn to_sql(&self, context: Rc<Context>) -> Result<String, CubeError> {
        let sql = match &self.source {
            FromSource::Empty => format!(""),
            FromSource::Cube(cube) => {
                let cubesql = cube.to_sql(context.clone())?;
                format!("      {} ", cubesql)
            }
            FromSource::Join(j) => {
                format!("{}", j.to_sql(context.clone())?)
            }
            FromSource::Subquery(s, alias) => {
                format!("({}) AS {}", s.to_sql()?, alias)
            }
        };
        Ok(sql)
    }
}
