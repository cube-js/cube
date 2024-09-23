use super::From;
use crate::planner::{BaseJoinCondition, Context};
use cubenativeutils::CubeError;

use std::rc::Rc;
pub struct JoinItem {
    pub from: From,
    pub on: Rc<dyn BaseJoinCondition>,
    pub is_inner: bool,
}

pub struct Join {
    pub root: From,
    pub joins: Vec<JoinItem>,
}

impl JoinItem {
    pub fn to_sql(&self, context: Rc<Context>) -> Result<String, CubeError> {
        let operator = if self.is_inner { "INNER" } else { "LEFT" };
        let on_sql = self.on.to_sql(context.clone())?;
        Ok(format!(
            "{} JOIN {} ON {}",
            operator,
            self.from.to_sql(context)?,
            on_sql
        ))
    }
}

impl Join {
    pub fn to_sql(&self, context: Rc<Context>) -> Result<String, CubeError> {
        let joins_sql = self
            .joins
            .iter()
            .map(|j| j.to_sql(context.clone()))
            .collect::<Result<Vec<_>, _>>()?;
        let res = format!(
            "{}\n{}",
            self.root.to_sql(context.clone())?,
            joins_sql.join("\n")
        );
        Ok(res)
    }
}
