use itertools::Itertools;

use super::{Cte, Expr, Filter, From, OrderBy, Schema, SchemaColumn};
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::VisitorContext;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct AliasedExpr {
    pub expr: Expr,
    pub alias: String,
}

impl AliasedExpr {
    pub fn new(expr: Expr, alias: String) -> Self {
        Self { expr, alias }
    }

    pub fn to_sql(
        &self,
        templates: &PlanSqlTemplates,
        context: Rc<VisitorContext>,
        schema: Rc<Schema>,
    ) -> Result<String, CubeError> {
        Ok(format!(
            "{}  {}",
            self.expr.to_sql(templates, context, schema)?,
            templates.quote_identifier(&self.alias)?
        ))
    }
}

pub struct Select {
    pub(super) projection_columns: Vec<AliasedExpr>,
    pub(super) from: From,
    pub(super) filter: Option<Filter>,
    pub(super) group_by: Vec<Expr>,
    pub(super) having: Option<Filter>,
    pub(super) order_by: Vec<OrderBy>,
    pub(super) context: Rc<VisitorContext>,
    pub(super) ctes: Vec<Rc<Cte>>,
    pub(super) is_distinct: bool,
    pub(super) limit: Option<usize>,
    pub(super) offset: Option<usize>,
}

impl Select {
    pub fn make_schema(&self, self_alias: Option<String>) -> Schema {
        if self.projection_columns.is_empty() {
            if let Some(self_alias) = self_alias {
                self.from.schema.move_to_source(&self_alias)
            } else {
                Schema::empty() //FIXME
            }
        } else {
            let mut schema = Schema::empty();
            for col in self.projection_columns.iter() {
                match &col.expr {
                    Expr::Member(member) => {
                        let schema_col = SchemaColumn::new(
                            self_alias.clone(),
                            col.alias.clone(),
                            member.member.full_name(),
                        );
                        schema.add_column(schema_col);
                    }
                }
            }
            schema
        }
    }
    pub fn to_sql(&self, templates: &PlanSqlTemplates) -> Result<String, CubeError> {
        let schema = self.from.schema.clone();
        let projection = if !self.projection_columns.is_empty() {
            self.projection_columns
                .iter()
                .map(|p| p.to_sql(templates, self.context.clone(), schema.clone()))
                .collect::<Result<Vec<_>, _>>()?
                .join(", ")
        } else {
            format!(" * ")
        };

        let where_condition = if let Some(filter) = &self.filter {
            format!(
                " WHERE {}",
                filter.to_sql(self.context.clone(), schema.clone())?
            )
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
            format!(
                " HAVING {}",
                having.to_sql(self.context.clone(), schema.clone())?
            )
        } else {
            format!("")
        };

        let ctes = if !self.ctes.is_empty() {
            let ctes_sql = self
                .ctes
                .iter()
                .map(|cte| -> Result<_, CubeError> {
                    Ok(format!(
                        " {} as ({})",
                        cte.name(),
                        cte.query().to_sql(templates)?
                    ))
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
        let from = self.from.to_sql(templates, self.context.clone())?;
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
