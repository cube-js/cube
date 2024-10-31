use super::QueryPlan;
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;

pub struct Union {
    pub union: Vec<QueryPlan>,
}

impl Union {
    pub fn new(union: Vec<QueryPlan>) -> Self {
        Self { union }
    }

    pub fn to_sql(&self, templates: &PlanSqlTemplates) -> Result<String, CubeError> {
        let res = self
            .union
            .iter()
            .map(|q| q.to_sql(templates))
            .collect::<Result<Vec<_>, _>>()?
            .join(" UNION ALL ");
        Ok(res)
    }
}
