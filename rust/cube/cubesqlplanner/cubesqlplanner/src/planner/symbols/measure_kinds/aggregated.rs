use super::super::common::AggregationType;
use super::super::deps::symbol_deps;
use crate::planner::SqlCall;
use std::rc::Rc;

/// `Aggregated` measure kind. `sql` may be absent when the measure
/// is built from a `case` body.
#[derive(Clone)]
pub struct AggregatedMeasure {
    agg_type: AggregationType,
    member_sql: Option<Rc<SqlCall>>,
}

symbol_deps! {
    AggregatedMeasure {
        agg_type: skip,
        member_sql: dep,
    }
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

    pub fn iter_sql_calls(&self) -> Box<dyn Iterator<Item = &Rc<SqlCall>> + '_> {
        Box::new(self.member_sql.iter())
    }

    pub fn is_owned_by_cube(&self) -> bool {
        self.member_sql
            .as_ref()
            .is_some_and(|sql| sql.is_owned_by_cube())
    }
}
