use super::Expr;
use cubenativeutils::CubeError;
use std::fmt;

pub struct OrderBy {
    pub expr: Expr,
    pub asc: bool,
}

impl OrderBy {
    pub fn new(expr: Expr, asc: bool) -> OrderBy {
        OrderBy { expr, asc }
    }

    /* pub fn to_sql(&self) -> Result<String, CubeError> {
        let index = match &self.expr {
            Expr::Field(f) => f.index(),
        };
        let asc_str = if self.asc { "ASC" } else { "DESC" };

        Ok(format!("{} {}", index, asc_str))
    } */

    pub fn asc_str(&self) -> &str {
        if self.asc {
            "ASC"
        } else {
            "DESC"
        }
    }
}
