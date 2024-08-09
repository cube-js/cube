use itertools::Itertools;

use super::{Expr, From, OrderBy};
use crate::planner::BaseField;
use std::fmt;
use std::rc::Rc;

pub struct Select {
    pub projection: Vec<Expr>,
    pub from: From,
    pub group_by: Vec<Rc<dyn BaseField>>,
    pub order_by: Vec<OrderBy>,
}

impl fmt::Display for Select {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "SELECT")?;
        write!(f, "      ");
        for expr in self.projection.iter().take(1) {
            write!(f, "{}", expr)?;
        }
        for expr in self.projection.iter().skip(1) {
            write!(f, ", {}", expr)?;
        }

        writeln!(f, "")?;
        write!(f, "{}", self.from)?;

        if !self.group_by.is_empty() {
            let str = self
                .group_by
                .iter()
                .map(|d| format!("{}", d.index()))
                .join(", ");
            write!(f, " GROUP BY {}", str)?;
        }

        if !self.order_by.is_empty() {
            write!(f, " ORDER BY ")?;
            for order in self.order_by.iter().take(1) {
                write!(f, "{}", order)?;
            }
            for order in self.order_by.iter().skip(1) {
                write!(f, ", {}", order)?;
            }
        }
        Ok(())
    }
}
