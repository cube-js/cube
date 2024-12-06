use super::{QueryPlan, Schema, Select};
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;

use std::rc::Rc;

#[derive(Clone)]
pub struct Cte {
    query: Rc<QueryPlan>,
    name: String,
}

impl Cte {
    pub fn new(query: Rc<QueryPlan>, name: String) -> Self {
        Self { query, name }
    }

    pub fn new_from_select(select: Rc<Select>, name: String) -> Self {
        Self {
            query: Rc::new(QueryPlan::Select(select)),
            name,
        }
    }

    pub fn make_schema(&self) -> Schema {
        self.query.make_schema(Some(self.name().clone()))
    }

    pub fn query(&self) -> &Rc<QueryPlan> {
        &self.query
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn to_sql(&self, templates: &PlanSqlTemplates) -> Result<String, CubeError> {
        let sql = format!("({})", self.query.to_sql(templates)?);
        Ok(sql)
    }
}
