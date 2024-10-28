use itertools::Itertools;

use super::{Expr, Filter, From, OrderBy, Subquery};
use crate::planner::VisitorContext;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct Select {
    pub projection: Vec<Expr>,
    pub from: From,
    pub filter: Option<Filter>,
    pub group_by: Vec<Expr>,
    pub having: Option<Filter>,
    pub order_by: Vec<OrderBy>,
    pub context: Rc<VisitorContext>,
    pub ctes: Vec<Rc<Subquery>>,
    pub is_distinct: bool,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
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

        let ctes = if !self.ctes.is_empty() {
            let ctes_sql = self
                .ctes
                .iter()
                .map(|cte| -> Result<_, CubeError> {
                    Ok(format!(" {} as ({})", cte.alias(), cte.query().to_sql()?))
                })
                .collect::<Result<Vec<_>, _>>()?
                .join(",\n");
            format!("WITH\n{ctes_sql}\n")
        } else {
            "".to_string()
        };

        let order_by = if !self.order_by.is_empty() {
            let order_sql = self
                .order_by
                .iter()
                .map(|itm| format!("{} {}", itm.pos, itm.asc_str()))
                .collect::<Vec<_>>()
                .join(", ");
            format!(" ORDER BY {}", order_sql)
        } else {
            format!("")
        };

        let distinct = if self.is_distinct { "DISTINCT " } else { "" };
        let from = self.from.to_sql(self.context.clone())?;
        let limit = if let Some(limit) = self.limit {
            format!(" LIMIT {limit}")
        } else {
            format!("")
        };
        let offset = if let Some(offset) = self.offset {
            format!(" OFFSET {offset}")
        } else {
            format!("")
        };

        let res = format!(
            "{ctes}SELECT\
            \n      {distinct}{projection}\
            \n    FROM\
            \n{from}{where_condition}{group_by}{having}{order_by}{limit}{offset}",
        );
        Ok(res)
    }
}
