use super::{QueryPlan, Select};
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;

use std::rc::Rc;

#[derive(Clone)]
pub struct Subquery {
    query: Rc<QueryPlan>,
    alias: String,
}

impl Subquery {
    pub fn new(query: Rc<QueryPlan>, alias: String) -> Self {
        Self { query, alias }
    }

    pub fn new_from_select(select: Rc<Select>, alias: String) -> Self {
        Self {
            query: Rc::new(QueryPlan::Select(select)),
            alias,
        }
    }

    pub fn query(&self) -> &Rc<QueryPlan> {
        &self.query
    }

    pub fn alias(&self) -> &String {
        &self.alias
    }

    pub fn to_sql(&self, templates: &PlanSqlTemplates) -> Result<String, CubeError> {
        let sql = format!("({}) AS {}", self.query.to_sql(templates)?, self.alias);
        Ok(sql)
    }
}
