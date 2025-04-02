use super::{Expr, SingleAliasedSource};
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::{BaseJoinCondition, VisitorContext};
use cubenativeutils::CubeError;
use lazy_static::lazy_static;

use std::rc::Rc;

pub struct RegularRollingWindowJoinCondition {
    time_series_source: String,
    trailing_interval: Option<String>,
    leading_interval: Option<String>,
    offset: String,
    time_dimension: Expr,
}

impl RegularRollingWindowJoinCondition {
    pub fn new(
        time_series_source: String,
        trailing_interval: Option<String>,
        leading_interval: Option<String>,
        offset: String,
        time_dimension: Expr,
    ) -> Self {
        Self {
            time_series_source,
            trailing_interval,
            leading_interval,
            offset,
            time_dimension,
        }
    }

    pub fn to_sql(
        &self,
        templates: &PlanSqlTemplates,
        context: Rc<VisitorContext>,
    ) -> Result<String, CubeError> {
        let mut conditions = vec![];
        let date_column = self.time_dimension.to_sql(templates, context)?;

        lazy_static! {
            static ref UNBOUNDED: Option<String> = Some("unbounded".to_string());
        }

        if self.trailing_interval != *UNBOUNDED {
            let start_date = if self.offset == "start" {
                templates.column_reference(&Some(self.time_series_source.clone()), "date_from")?
            } else {
                templates.column_reference(&Some(self.time_series_source.clone()), "date_to")?
            };

            let trailing_start = if let Some(trailing_interval) = &self.trailing_interval {
                format!("{start_date} - interval '{trailing_interval}'")
            } else {
                start_date
            };

            let sign = if self.offset == "start" { ">=" } else { ">" };

            conditions.push(format!("{date_column} {sign} {trailing_start}"));
        }

        if self.leading_interval != *UNBOUNDED {
            let end_date = if self.offset == "end" {
                templates.column_reference(&Some(self.time_series_source.clone()), "date_to")?
            } else {
                templates.column_reference(&Some(self.time_series_source.clone()), "date_from")?
            };

            let leading_end = if let Some(leading_interval) = &self.leading_interval {
                format!("{end_date} + interval '{leading_interval}'")
            } else {
                end_date
            };

            let sign = if self.offset == "end" { "<=" } else { "<" };

            conditions.push(format!("{date_column} {sign} {leading_end}"));
        }
        let result = if conditions.is_empty() {
            templates.always_true()?
        } else {
            conditions.join(" AND ")
        };
        Ok(result)
    }
}

pub struct RollingTotalJoinCondition {
    time_series_source: String,
    time_dimension: Expr,
}

impl RollingTotalJoinCondition {
    pub fn new(time_series_source: String, time_dimension: Expr) -> Self {
        Self {
            time_series_source,
            time_dimension,
        }
    }

    pub fn to_sql(
        &self,
        templates: &PlanSqlTemplates,
        context: Rc<VisitorContext>,
    ) -> Result<String, CubeError> {
        let date_column = self.time_dimension.to_sql(templates, context)?;
        let date_to =
            templates.column_reference(&Some(self.time_series_source.clone()), "date_to")?;
        let result = format!("{date_column} <= {date_to}");
        Ok(result)
    }
}
pub struct ToDateRollingWindowJoinCondition {
    time_series_source: String,
    granularity: String,
    time_dimension: Expr,
    query_tools: Rc<QueryTools>,
}

impl ToDateRollingWindowJoinCondition {
    pub fn new(
        time_series_source: String,
        granularity: String,
        time_dimension: Expr,
        query_tools: Rc<QueryTools>,
    ) -> Self {
        Self {
            time_series_source,
            granularity,
            time_dimension,
            query_tools,
        }
    }

    pub fn to_sql(
        &self,
        templates: &PlanSqlTemplates,
        context: Rc<VisitorContext>,
    ) -> Result<String, CubeError> {
        let date_column = self.time_dimension.to_sql(templates, context)?;

        let date_from =
            templates.column_reference(&Some(self.time_series_source.clone()), "date_to")?;
        let date_to =
            templates.column_reference(&Some(self.time_series_source.clone()), "date_from")?;
        let grouped_from = self
            .query_tools
            .base_tools()
            .time_grouped_column(self.granularity.clone(), date_from)?;
        let result = format!("{date_column} >= {grouped_from} and {date_column} <= {date_to}");
        Ok(result)
    }
}

pub struct DimensionJoinCondition {
    // AND (... OR ...)
    conditions: Vec<Vec<(Expr, Expr)>>,
    null_check: bool,
}

impl DimensionJoinCondition {
    pub fn new(conditions: Vec<Vec<(Expr, Expr)>>, null_check: bool) -> Self {
        Self {
            conditions,
            null_check,
        }
    }

    pub fn to_sql(
        &self,
        templates: &PlanSqlTemplates,
        context: Rc<VisitorContext>,
    ) -> Result<String, CubeError> {
        let result = if self.conditions.is_empty() {
            format!("1 = 1")
        } else {
            self.conditions
                .iter()
                .map(|or_conditions| -> Result<_, CubeError> {
                    Ok(format!(
                        "({})",
                        or_conditions
                            .iter()
                            .map(|(left, right)| -> Result<String, CubeError> {
                                self.dimension_condition(templates, context.clone(), left, right)
                            })
                            .collect::<Result<Vec<_>, _>>()?
                            .join(" OR ")
                    ))
                })
                .collect::<Result<Vec<_>, _>>()?
                .join(" AND ")
        };
        Ok(result)
    }

    fn dimension_condition(
        &self,
        templates: &PlanSqlTemplates,
        context: Rc<VisitorContext>,
        left_expr: &Expr,
        right_expr: &Expr,
    ) -> Result<String, CubeError> {
        let left_sql = left_expr.to_sql(templates, context.clone())?;
        let right_sql = right_expr.to_sql(templates, context.clone())?;
        templates.join_by_dimension_conditions(&left_sql, &right_sql, self.null_check)
    }
}

pub enum JoinCondition {
    DimensionJoinCondition(DimensionJoinCondition),
    BaseJoinCondition(Rc<dyn BaseJoinCondition>),
    RegularRollingWindowJoinCondition(RegularRollingWindowJoinCondition),
    ToDateRollingWindowJoinCondition(ToDateRollingWindowJoinCondition),
    RollingTotalJoinCondition(RollingTotalJoinCondition),
}

impl JoinCondition {
    pub fn new_dimension_join(conditions: Vec<Vec<(Expr, Expr)>>, null_check: bool) -> Self {
        Self::DimensionJoinCondition(DimensionJoinCondition::new(conditions, null_check))
    }

    pub fn new_regular_rolling_join(
        time_series_source: String,
        trailing_interval: Option<String>,
        leading_interval: Option<String>,
        offset: String,
        time_dimension: Expr,
    ) -> Self {
        Self::RegularRollingWindowJoinCondition(RegularRollingWindowJoinCondition::new(
            time_series_source,
            trailing_interval,
            leading_interval,
            offset,
            time_dimension,
        ))
    }

    pub fn new_to_date_rolling_join(
        time_series_source: String,
        granularity: String,
        time_dimension: Expr,
        query_tools: Rc<QueryTools>,
    ) -> Self {
        Self::ToDateRollingWindowJoinCondition(ToDateRollingWindowJoinCondition::new(
            time_series_source,
            granularity,
            time_dimension,
            query_tools,
        ))
    }

    pub fn new_rolling_total_join(time_series_source: String, time_dimension: Expr) -> Self {
        Self::RollingTotalJoinCondition(RollingTotalJoinCondition::new(
            time_series_source,
            time_dimension,
        ))
    }

    pub fn new_base_join(base: Rc<dyn BaseJoinCondition>) -> Self {
        Self::BaseJoinCondition(base)
    }

    pub fn to_sql(
        &self,
        templates: &PlanSqlTemplates,
        context: Rc<VisitorContext>,
    ) -> Result<String, CubeError> {
        match &self {
            JoinCondition::DimensionJoinCondition(cond) => cond.to_sql(templates, context),
            JoinCondition::BaseJoinCondition(cond) => cond.to_sql(context, templates),
            JoinCondition::RegularRollingWindowJoinCondition(cond) => {
                cond.to_sql(templates, context)
            }
            JoinCondition::ToDateRollingWindowJoinCondition(cond) => {
                cond.to_sql(templates, context)
            }
            JoinCondition::RollingTotalJoinCondition(cond) => cond.to_sql(templates, context),
        }
    }
}

pub struct JoinItem {
    pub from: SingleAliasedSource,
    pub on: JoinCondition,
    pub join_type: JoinType,
}

pub struct Join {
    pub root: SingleAliasedSource,
    pub joins: Vec<JoinItem>,
}

pub enum JoinType {
    Inner,
    Left,
    Full,
}

impl JoinItem {
    pub fn to_sql(
        &self,
        templates: &PlanSqlTemplates,
        context: Rc<VisitorContext>,
    ) -> Result<String, CubeError> {
        let on_sql = self.on.to_sql(templates, context.clone())?;
        let result = templates.join(
            &self.from.to_sql(templates, context)?,
            &on_sql,
            &self.join_type,
        )?;
        Ok(result)
    }
}

impl Join {
    pub fn to_sql(
        &self,
        templates: &PlanSqlTemplates,
        context: Rc<VisitorContext>,
    ) -> Result<String, CubeError> {
        let joins_sql = self
            .joins
            .iter()
            .map(|j| j.to_sql(templates, context.clone()))
            .collect::<Result<Vec<_>, _>>()?;
        let res = format!(
            "{}\n{}",
            self.root.to_sql(templates, context.clone())?,
            joins_sql.join("\n")
        );
        Ok(res)
    }
}
