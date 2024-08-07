use itertools::Itertools;

use super::expression::Expr;
use super::from::From;
use crate::planner::BaseDimension;
use std::fmt;
use std::rc::Rc;

pub struct Select {
    pub projection: Vec<Expr>,
    pub from: From,
    pub group_by: Vec<Rc<BaseDimension>>,
}

impl fmt::Display for Select {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "SELECT")?;
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
                .enumerate()
                .map(|(i, _)| format!("{}", i + 1))
                .join(",");
            write!(f, " GROUP BY {}", str)?;
        }
        Ok(())
    }
}
