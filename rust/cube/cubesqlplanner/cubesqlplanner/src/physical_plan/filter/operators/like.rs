use super::{FilterOperationSql, FilterSqlContext};
use crate::planner::filter::operators::like::LikeOp;
use cubenativeutils::CubeError;

impl FilterOperationSql for LikeOp {
    fn to_sql(&self, ctx: &FilterSqlContext) -> Result<String, CubeError> {
        let escape_char = ctx.plan_templates.like_escape_char()?;
        let allocated = self
            .values
            .iter()
            .map(|value| {
                let escaped = escape_char
                    .map(|character| escape_like_pattern(value, character))
                    .unwrap_or_else(|| value.clone());
                ctx.allocate_and_cast_str(&escaped, &self.member_type)
            })
            .collect::<Result<Vec<_>, _>>()?;

        let like_parts = allocated
            .into_iter()
            .map(|v| {
                ctx.plan_templates.ilike(
                    ctx.member_sql(),
                    &v,
                    self.start_wild,
                    self.end_wild,
                    self.negated,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;

        let logical_symbol = if self.negated { " AND " } else { " OR " };
        let need_null_check = if self.negated {
            !self.has_null
        } else {
            self.has_null
        };
        let null_check = if need_null_check {
            ctx.plan_templates
                .or_is_null_check(ctx.member_sql().to_string())?
        } else {
            "".to_string()
        };

        Ok(format!(
            "({}){}",
            like_parts.join(logical_symbol),
            null_check
        ))
    }
}

fn escape_like_pattern(value: &str, escape_char: char) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        if character == escape_char || matches!(character, '%' | '_') {
            escaped.push(escape_char);
        }
        escaped.push(character);
    }
    escaped
}
