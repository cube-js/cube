use super::super::super::MemberSymbol;
use super::super::common::AggregationType;
use crate::planner::{CubeRef, SqlCall};
use cubenativeutils::CubeError;
use std::rc::Rc;

#[derive(Clone)]
pub struct AggregatedMeasure {
    agg_type: AggregationType,
    member_sql: Option<Rc<SqlCall>>,
}

impl AggregatedMeasure {
    pub fn new(agg_type: AggregationType, member_sql: Rc<SqlCall>) -> Self {
        Self {
            agg_type,
            member_sql: Some(member_sql),
        }
    }

    pub fn new_without_sql(agg_type: AggregationType) -> Self {
        Self {
            agg_type,
            member_sql: None,
        }
    }

    pub fn agg_type(&self) -> AggregationType {
        self.agg_type
    }

    pub fn member_sql(&self) -> Option<&Rc<SqlCall>> {
        self.member_sql.as_ref()
    }

    pub fn get_dependencies(&self) -> Vec<Rc<MemberSymbol>> {
        let mut deps = vec![];
        if let Some(sql) = &self.member_sql {
            sql.extract_symbol_deps(&mut deps);
        }
        deps
    }

    pub fn apply_to_deps<F: Fn(&Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError>>(
        &self,
        f: &F,
    ) -> Result<Self, CubeError> {
        Ok(Self {
            agg_type: self.agg_type,
            member_sql: self
                .member_sql
                .as_ref()
                .map(|sql| sql.apply_recursive(f))
                .transpose()?,
        })
    }

    pub fn iter_sql_calls(&self) -> Box<dyn Iterator<Item = &Rc<SqlCall>> + '_> {
        Box::new(self.member_sql.iter())
    }

    pub fn get_cube_refs(&self) -> Vec<CubeRef> {
        let mut refs = vec![];
        if let Some(sql) = &self.member_sql {
            sql.extract_cube_refs(&mut refs);
        }
        refs
    }

    pub fn is_owned_by_cube(&self) -> bool {
        self.member_sql
            .as_ref()
            .is_some_and(|sql| sql.is_owned_by_cube())
    }
}
