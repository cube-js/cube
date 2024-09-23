use super::Expr;

pub struct OrderBy {
    pub expr: Expr,
    pub asc: bool,
}

impl OrderBy {
    pub fn new(expr: Expr, asc: bool) -> OrderBy {
        OrderBy { expr, asc }
    }

    pub fn asc_str(&self) -> &str {
        if self.asc {
            "ASC"
        } else {
            "DESC"
        }
    }
}
