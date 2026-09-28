use super::{Schema, Select, TimeSeries, Union};
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::rc::Rc;

#[derive(Clone)]
pub enum QueryPlan {
    Select(Rc<Select>),
    Union(Rc<Union>),
    TimeSeries(Rc<TimeSeries>),
}

impl QueryPlan {
    pub fn schema(&self) -> Rc<Schema> {
        match self {
            QueryPlan::Select(select) => select.schema(),
            QueryPlan::Union(union) => union.schema(),
            QueryPlan::TimeSeries(series) => series.schema(),
        }
    }
    pub fn to_sql(&self, templates: &PlanSqlTemplates) -> Result<String, CubeError> {
        match self {
            QueryPlan::Select(s) => s.to_sql(templates),
            QueryPlan::Union(u) => u.to_sql(templates),
            QueryPlan::TimeSeries(series) => series.to_sql(templates),
        }
    }
}
