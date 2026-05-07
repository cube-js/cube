use super::super::{MemberSqlContext, ToSql};
use crate::planner::sql_evaluator::symbols::measure_kinds::{CountMeasure, CountSql};
use cubenativeutils::CubeError;

impl ToSql for CountMeasure {
    fn to_sql(&self, ctx: &MemberSqlContext) -> Result<String, CubeError> {
        match self.sql() {
            CountSql::Explicit(sql) => ctx.eval_sql_call(sql),
            CountSql::Auto(pk_sqls) => {
                if pk_sqls.len() > 1 {
                    let pk_strings = pk_sqls
                        .iter()
                        .map(|pk| -> Result<_, CubeError> {
                            let res = ctx.eval_sql_call(pk)?;
                            ctx.templates.cast_to_string(&res)
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    ctx.templates.concat_strings(&pk_strings)
                } else if let Some(pk_sql) = pk_sqls.first() {
                    ctx.eval_sql_call(pk_sql)
                } else {
                    Ok("*".to_string())
                }
            }
        }
    }
}
