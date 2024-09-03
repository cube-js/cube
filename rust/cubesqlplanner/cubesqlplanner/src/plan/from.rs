use super::Join;
use super::Select;
use crate::planner::{BaseCube, Context};
use cubenativeutils::CubeError;
use std::fmt;
use std::rc::Rc;

#[derive(Clone)]
pub enum FromSource {
    Empty,
    Cube(Rc<BaseCube>),
    Join(Rc<Join>),
    Subquery(Rc<Select>, String),
}

#[derive(Clone)]
pub struct From {
    pub source: FromSource,
}

impl From {
    pub fn new(source: FromSource) -> Self {
        Self { source }
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
