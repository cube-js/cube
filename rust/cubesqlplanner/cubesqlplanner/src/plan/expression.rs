use crate::planner::{Context, IndexedMember};
use cubenativeutils::CubeError;
use std::fmt;
use std::rc::Rc;

pub enum Expr {
    Field(Rc<dyn IndexedMember>),
    Reference(String, String),
}

impl Expr {
    pub fn to_sql(&self, context: Rc<Context>) -> Result<String, CubeError> {
        match self {
            Expr::Field(field) => field.to_sql(context),
            Expr::Reference(cube_alias, field_alias) => {
                Ok(format!("{}.{}", cube_alias, field_alias))
            }
        }
    }
}
