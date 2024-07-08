use super::filter::Filter;
use super::select::Select;
use datafusion::logical_expr::Expr;

pub struct Aggregation {
    select: Select,
    group_by: Vec<Expr>,
    aggregates: Vec<Expr>,
    having: Option<Filter>,
}

pub struct Join {
    select: Select,
    group_by: Vec<Expr>,
    having: Option<Filter>,
}
