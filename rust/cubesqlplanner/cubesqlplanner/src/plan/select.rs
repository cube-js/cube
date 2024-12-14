use super::{Cte, Expr, Filter, From, OrderBy, Schema};
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::sql_templates::{
    TemplateGroupByColumn, TemplateOrderByColumn, TemplateProjectionColumn,
};
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
    ) -> Result<TemplateProjectionColumn, CubeError> {
        let expr = self.expr.to_sql(templates, context)?;
        let aliased = templates.column_aliased(&expr, &self.alias)?;
        Ok(TemplateProjectionColumn {
            expr,
            alias: self.alias.clone(),
            aliased,
        })
    }
}

pub struct Select {
    pub(super) projection_columns: Vec<AliasedExpr>,
    pub(super) from: Rc<From>,
    pub(super) filter: Option<Filter>,
    pub(super) group_by: Vec<Expr>,
    pub(super) having: Option<Filter>,
    pub(super) order_by: Vec<OrderBy>,
    pub(super) context: Rc<VisitorContext>,
    pub(super) ctes: Vec<Rc<Cte>>,
    pub(super) is_distinct: bool,
    pub(super) limit: Option<usize>,
    pub(super) offset: Option<usize>,
    pub(super) schema: Rc<Schema>,
}

impl Select {
    pub fn schema(&self) -> Rc<Schema> {
        self.schema.clone()
    }

    pub fn to_sql(&self, templates: &PlanSqlTemplates) -> Result<String, CubeError> {
        let projection = if !self.projection_columns.is_empty() {
            self.projection_columns
                .iter()
                .map(|p| p.to_sql(templates, self.context.clone()))
                .collect::<Result<Vec<_>, _>>()?
        } else {
            vec![TemplateProjectionColumn {
                expr: format!("*"),
                alias: format!(""),
                aliased: format!("*"),
            }]
        };

        let where_condition = if let Some(filter) = &self.filter {
            Some(filter.to_sql(templates, self.context.clone())?)
        } else {
            None
        };

        let group_by = self
            .group_by
            .iter()
            .enumerate()
            .map(|(i, expr)| -> Result<_, CubeError> {
                let expr = expr.to_sql(templates, self.context.clone())?;
                Ok(TemplateGroupByColumn { expr, index: i + 1 })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let having = if let Some(having) = &self.having {
            Some(having.to_sql(templates, self.context.clone())?)
        } else {
            None
        };

        let ctes = self
            .ctes
            .iter()
            .map(|cte| -> Result<_, CubeError> {
                templates.cte(&cte.query().to_sql(templates)?, &cte.name().clone())
            })
            .collect::<Result<Vec<_>, _>>()?;

        let order_by = self
            .order_by
            .iter()
            .map(|itm| -> Result<_, CubeError> {
                let expr = templates.order_by(
                    &itm.expr.to_sql(templates, self.context.clone())?,
                    Some(itm.pos),
                    !itm.desc,
                )?;
                Ok(TemplateOrderByColumn { expr })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let from = self.from.to_sql(templates, self.context.clone())?;

        let result = templates.select(
            ctes,
            &from,
            projection,
            where_condition,
            group_by,
            having,
            order_by,
            self.limit,
            self.offset,
            self.is_distinct,
        )?;

        /* let res = format!(
            "{ctes}SELECT\
            \n      {distinct}{projection}\
            \n    FROM\
            \n{from}{where_condition}{group_by}{having}{order_by}{limit}{offset}",
        ); */
        Ok(result)
    }
}
