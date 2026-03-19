use super::{FilterOperationSql, FilterSqlContext};
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::rc::Rc;

#[derive(Clone, Debug)]
pub struct EqualityOp {
    negated: bool,
    values: Vec<Option<String>>,
    member_type: Option<String>,
}

impl EqualityOp {
    pub fn new(negated: bool, values: Vec<Option<String>>, member_type: Option<String>) -> Self {
        Self {
            negated,
            values,
            member_type,
        }
    }

    fn first_param(
        &self,
        query_tools: &Rc<QueryTools>,
        plan_templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        if self.values.is_empty() {
            return Err(CubeError::user(
                "Expected one parameter but nothing found".to_string(),
            ));
        }
        match &self.values[0] {
            Some(value) => {
                let allocated = query_tools.allocate_param(value);
                Self::cast_param(&allocated, &self.member_type, plan_templates)
            }
            None => Ok("NULL".to_string()),
        }
    }

    fn allocate_and_cast_values(
        &self,
        query_tools: &Rc<QueryTools>,
        plan_templates: &PlanSqlTemplates,
    ) -> Result<Vec<String>, CubeError> {
        self.values
            .iter()
            .filter_map(|v| v.as_ref())
            .map(|v| {
                let allocated = query_tools.allocate_param(v);
                Self::cast_param(&allocated, &self.member_type, plan_templates)
            })
            .collect()
    }

    fn cast_param(
        value: &str,
        member_type: &Option<String>,
        plan_templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        match member_type.as_deref() {
            Some("boolean") => plan_templates.bool_param_cast(value),
            Some("number") => plan_templates.number_param_cast(value),
            _ => Ok(value.to_string()),
        }
    }
}

impl FilterOperationSql for EqualityOp {
    fn to_sql(&self, ctx: &FilterSqlContext) -> Result<String, CubeError> {
        let has_null = self.values.iter().any(|v| v.is_none());
        let need_null_check = if self.negated { !has_null } else { has_null };

        if self.values.len() > 1 {
            let allocated =
                self.allocate_and_cast_values(ctx.query_tools, ctx.plan_templates)?;
            if self.negated {
                ctx.plan_templates
                    .not_in_where(ctx.member_sql.to_string(), allocated, need_null_check)
            } else {
                ctx.plan_templates
                    .in_where(ctx.member_sql.to_string(), allocated, need_null_check)
            }
        } else if has_null {
            if self.negated {
                ctx.plan_templates.set_where(ctx.member_sql.to_string())
            } else {
                ctx.plan_templates.not_set_where(ctx.member_sql.to_string())
            }
        } else {
            let param = self.first_param(ctx.query_tools, ctx.plan_templates)?;
            if self.negated {
                ctx.plan_templates
                    .not_equals(ctx.member_sql.to_string(), param, need_null_check)
            } else {
                ctx.plan_templates
                    .equals(ctx.member_sql.to_string(), param, need_null_check)
            }
        }
    }
}
