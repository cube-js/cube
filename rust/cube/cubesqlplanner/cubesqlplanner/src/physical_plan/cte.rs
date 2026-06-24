use super::{QueryPlan, Select};
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;

use std::rc::Rc;

#[derive(Clone)]
pub struct Cte {
    query: Rc<QueryPlan>,
    name: String,
    /// Whether this CTE is a self-referencing recursive CTE. When any CTE in a
    /// `WITH` clause is recursive, dialects such as MySQL require the clause to
    /// be emitted as `WITH RECURSIVE`.
    is_recursive: bool,
}

impl Cte {
    pub fn new(query: Rc<QueryPlan>, name: String, is_recursive: bool) -> Self {
        Self {
            query,
            name,
            is_recursive,
        }
    }

    pub fn new_from_select(select: Rc<Select>, name: String) -> Self {
        Self {
            query: Rc::new(QueryPlan::Select(select)),
            name,
            is_recursive: false,
        }
    }

    pub fn query(&self) -> &Rc<QueryPlan> {
        &self.query
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn is_recursive(&self) -> bool {
        self.is_recursive
    }

    pub fn to_sql(&self, templates: &PlanSqlTemplates) -> Result<String, CubeError> {
        let sql = format!("({})", self.query.to_sql(templates)?);
        Ok(sql)
    }
}
