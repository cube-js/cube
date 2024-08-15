use crate::planner::IndexedMember;
use std::fmt;
use std::rc::Rc;

pub enum Expr {
    Field(Rc<dyn IndexedMember>),
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::Field(field) => {
                let sql = field.to_sql().map_err(|_| fmt::Error).unwrap();
                write!(f, "{}", sql)
            }
        }
    }
}
