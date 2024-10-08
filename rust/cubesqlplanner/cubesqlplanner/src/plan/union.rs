use super::QueryPlan;
use cubenativeutils::CubeError;

pub struct Union {
    pub union: Vec<QueryPlan>,
}

impl Union {
    pub fn new(union: Vec<QueryPlan>) -> Self {
        Self { union }
    }

    pub fn to_sql(&self) -> Result<String, CubeError> {
        let res = self
            .union
            .iter()
            .map(|q| q.to_sql())
            .collect::<Result<Vec<_>, _>>()?
            .join(" UNION ALL ");
        Ok(res)
    }
}
