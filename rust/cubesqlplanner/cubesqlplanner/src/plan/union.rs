use super::{QueryPlan, Schema};
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;

pub struct Union {
    pub union: Vec<QueryPlan>,
}

impl Union {
    pub fn new(union: Vec<QueryPlan>) -> Self {
        Self { union }
    }
    pub fn make_schema(&self, self_alias: Option<String>) -> Schema {
        if self.union.is_empty() {
            Schema::empty()
        } else {
            self.union[0].make_schema(self_alias)
        }
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
