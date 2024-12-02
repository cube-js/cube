use super::{QueryPlan, Select};
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;

use std::rc::Rc;

#[derive(Clone)]
pub struct Subquery {
    query: Rc<QueryPlan>,
}

impl Subquery {
    pub fn new(query: Rc<QueryPlan>) -> Self {
        Self { query }
    }

    pub fn new_from_select(select: Rc<Select>, alias: String) -> Self {
        Self {
            query: Rc::new(QueryPlan::Select(select)),
        }
    }

    pub fn query(&self) -> &Rc<QueryPlan> {
        &self.query
    }

    pub fn alias(&self) -> &String {
        &self.alias
    }

    pub fn to_sql(&self, templates: &PlanSqlTemplates) -> Result<String, CubeError> {
        let sql = format!("({})", self.query.to_sql(templates)?);
        Ok(sql)
    }
}
