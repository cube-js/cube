use crate::planner::{BaseDimension, BaseMeasure};
use std::fmt;
use std::rc::Rc;

pub enum Expr {
    Measure(Rc<BaseMeasure>),
    Dimension(Rc<BaseDimension>),
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::Measure(measure) => {
                let sql = measure.to_sql().map_err(|_| fmt::Error).unwrap();
                write!(f, "{}", sql)
            }
            Expr::Dimension(_) => write!(f, "dim"),
        }
    }
}
