use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;

#[derive(Clone, Debug)]
pub struct NullabilityOp {
    negated: bool,
}

impl NullabilityOp {
    pub fn new(negated: bool) -> Self {
        Self { negated }
    }

    pub fn to_sql(
        &self,
        member_sql: &str,
        plan_templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        if self.negated {
            plan_templates.not_set_where(member_sql.to_string())
        } else {
            plan_templates.set_where(member_sql.to_string())
        }
    }
}
