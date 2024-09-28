use itertools::Itertools;

use super::{Expr, Filter, From, OrderBy};
use crate::planner::{IndexedMember, VisitorContext};
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct Select {
    pub projection: Vec<Expr>,
    pub from: From,
    pub filter: Option<Filter>,
    pub group_by: Vec<Rc<dyn IndexedMember>>,
    pub having: Option<Filter>,
    pub order_by: Vec<OrderBy>,
    pub context: Rc<VisitorContext>,
    pub is_distinct: bool,
}

impl Select {
    pub fn to_sql(&self) -> Result<String, CubeError> {
        let projection = self
            .projection
            .iter()
            .map(|p| p.to_sql(self.context.clone()))
            .collect::<Result<Vec<_>, _>>()?
            .join(", ");
        let where_condition = if let Some(filter) = &self.filter {
            format!(" WHERE {}", filter.to_sql(self.context.clone())?)
        } else {
            format!("")
        };

        let group_by = if !self.group_by.is_empty() {
            let str = self
                .group_by
                .iter()
                .enumerate()
                .map(|(i, _)| format!("{}", i + 1))
                .join(", ");
            format!(" GROUP BY {}", str)
        } else {
            format!("")
        };

        let having = if let Some(having) = &self.having {
            format!(" HAVING {}", having.to_sql(self.context.clone())?)
        } else {
            format!("")
        };

        let order_by = if !self.order_by.is_empty() {
            let order_sql = self
                .order_by
                .iter()
                .enumerate()
                .map(|(i, itm)| format!("{} {}", i + 1, itm.asc_str()))
                .collect::<Vec<_>>()
                .join(", ");
            format!(" ORDER BY {}", order_sql)
        } else {
            format!("")
        };

        let distinct = if self.is_distinct { "DISTINCT " } else { "" };

        let res = format!(
            "SELECT\
            \n      {}{}\
            \n    FROM\
            \n{}{}{}{}{}",
            distinct,
            projection,
            self.from.to_sql(self.context.clone())?,
            where_condition,
            group_by,
            having,
            order_by
        );
        Ok(res)
    }
}
