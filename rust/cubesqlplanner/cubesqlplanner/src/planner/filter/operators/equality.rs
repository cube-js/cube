use super::{FilterOperationSql, FilterSqlContext};
use cubenativeutils::CubeError;

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
}

impl FilterOperationSql for EqualityOp {
    fn to_sql(&self, ctx: &FilterSqlContext) -> Result<String, CubeError> {
        let has_null = self.values.iter().any(|v| v.is_none());
        let need_null_check = if self.negated { !has_null } else { has_null };

        if self.values.len() > 1 {
            let allocated = ctx.allocate_and_cast_values(&self.values, &self.member_type)?;
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
            let param = match &self.values[0] {
                Some(value) => ctx.allocate_and_cast(value, &self.member_type)?,
                None => "NULL".to_string(),
            };
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
