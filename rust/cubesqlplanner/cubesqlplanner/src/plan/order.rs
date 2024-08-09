use super::Expr;
use std::fmt;

pub struct OrderBy {
    pub expr: Expr,
    pub asc: bool,
}

impl OrderBy {
    pub fn new(expr: Expr, asc: bool) -> OrderBy {
        OrderBy { expr, asc }
    }
}

impl fmt::Display for OrderBy {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let index = match &self.expr {
            Expr::Field(f) => f.index(),
        };

        let asc_str = if self.asc { "ASC" } else { "DESC" };

        write!(f, "{} {}", index, asc_str)
    }
}
