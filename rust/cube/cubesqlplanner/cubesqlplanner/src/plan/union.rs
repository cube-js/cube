use super::{QueryPlan, Schema};
use crate::{plan::Select, planner::sql_templates::PlanSqlTemplates};
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct Union {
    pub union: Vec<QueryPlan>,
    pub schema: Rc<Schema>,
}

impl Union {
    pub fn new(union: Vec<QueryPlan>) -> Self {
        let schema = if union.is_empty() {
            Rc::new(Schema::empty())
        } else {
            union[0].schema()
        };
        Self { union, schema }
    }

    pub fn new_from_subselects(selects: &Vec<Rc<Select>>) -> Self {
        let union = selects
            .iter()
            .map(|s| QueryPlan::Select(s.clone()))
            .collect();
        Self::new(union)
    }

    pub fn schema(&self) -> Rc<Schema> {
        self.schema.clone()
    }

    pub fn to_sql(&self, templates: &PlanSqlTemplates) -> Result<String, CubeError> {
        let res = self
            .union
            .iter()
            .map(|q| q.to_sql(templates))
            .collect::<Result<Vec<_>, _>>()?
            .join("\n UNION ALL \n");
        Ok(res)
    }
}
