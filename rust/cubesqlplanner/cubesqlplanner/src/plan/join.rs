use super::From;
use crate::planner::BaseJoinCondition;

use std::fmt;
use std::rc::Rc;

pub struct JoinItem {
    pub from: From,
    pub on: Rc<BaseJoinCondition>,
}

pub struct Join {
    pub root: From,
    pub joins: Vec<JoinItem>,
}

impl fmt::Display for JoinItem {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let on_sql = self.on.to_sql().map_err(|_| fmt::Error)?;
        writeln!(f, "LEFT JOIN {} ON {}", self.from, on_sql);

        Ok(())
    }
}
impl fmt::Display for Join {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "{}", self.root);
        for join in self.joins.iter() {
            write!(f, "{}", join)?;
        }

        Ok(())
    }
}
