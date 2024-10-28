use super::Select;
use super::Union;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub enum QueryPlan {
    Select(Rc<Select>),
    Union(Rc<Union>),
}

impl QueryPlan {
    pub fn to_sql(&self) -> Result<String, CubeError> {
        match self {
            QueryPlan::Select(s) => s.to_sql(),
            QueryPlan::Union(u) => u.to_sql(),
        }
    }
}
