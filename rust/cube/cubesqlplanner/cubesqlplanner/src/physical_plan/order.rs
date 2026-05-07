use super::Expr;

#[derive(Clone)]
pub struct OrderBy {
    pub expr: Expr,
    pub pos: usize,
    pub desc: bool,
}

impl OrderBy {
    pub fn new(expr: Expr, pos: usize, desc: bool) -> OrderBy {
        OrderBy { expr, pos, desc }
    }

    pub fn asc_str(&self) -> &str {
        if self.desc {
            "DESC"
        } else {
            "ASC"
        }
    }
}
