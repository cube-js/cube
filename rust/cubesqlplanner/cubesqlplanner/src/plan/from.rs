use super::{Join, QueryPlan, Subquery};
use crate::planner::{BaseCube, VisitorContext};
use cubenativeutils::CubeError;
use std::rc::Rc;

#[derive(Clone)]
pub enum FromSource {
    Empty,
    Cube(Rc<BaseCube>),
    Join(Rc<Join>),
    Subquery(Subquery),
    TableReference(String, Option<String>),
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

    pub fn new_from_table_reference(reference: String, alias: Option<String>) -> Self {
        Self::new(FromSource::TableReference(reference, alias))
    }

    pub fn new_from_join(join: Rc<Join>) -> Self {
        Self::new(FromSource::Join(join))
    }

    pub fn new_from_subquery(plan: Rc<QueryPlan>, alias: String) -> Self {
        Self::new(FromSource::Subquery(Subquery::new(plan, alias)))
    }

    pub fn to_sql(&self, context: Rc<VisitorContext>) -> Result<String, CubeError> {
        let sql = match &self.source {
            FromSource::Empty => format!(""),
            FromSource::Cube(cube) => {
                let cubesql = cube.to_sql(context.clone())?;
                format!("      {} ", cubesql)
            }
            FromSource::Join(j) => {
                format!("{}", j.to_sql(context.clone())?)
            }
            FromSource::Subquery(s) => s.to_sql()?,
            FromSource::TableReference(r, alias) => {
                if let Some(alias) = alias {
                    format!(" {} as {} ", r, alias)
                } else {
                    format!(" {} ", r)
                }
            }
        };
        Ok(sql)
    }
}
