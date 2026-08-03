use super::super::deps::symbol_deps;
use crate::planner::SqlCall;
use std::rc::Rc;

/// Value type of a calculated (non-aggregating) measure as declared
/// in the data model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CalculatedMeasureType {
    Number,
    String,
    Time,
    Boolean,
}

impl CalculatedMeasureType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Number => "number",
            Self::String => "string",
            Self::Time => "time",
            Self::Boolean => "boolean",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "number" => Some(Self::Number),
            "string" => Some(Self::String),
            "time" => Some(Self::Time),
            "boolean" => Some(Self::Boolean),
            _ => None,
        }
    }
}

/// `Calculated` measure kind: a non-aggregating expression. `sql`
/// may be absent when the value is provided through a `case` body.
#[derive(Clone)]
pub struct CalculatedMeasure {
    calc_type: CalculatedMeasureType,
    member_sql: Option<Rc<SqlCall>>,
}

symbol_deps! {
    CalculatedMeasure {
        calc_type: skip,
        member_sql: dep,
    }
}

impl CalculatedMeasure {
    pub fn new(calc_type: CalculatedMeasureType, member_sql: Rc<SqlCall>) -> Self {
        Self {
            calc_type,
            member_sql: Some(member_sql),
        }
    }

    pub fn new_without_sql(calc_type: CalculatedMeasureType) -> Self {
        Self {
            calc_type,
            member_sql: None,
        }
    }

    pub fn calc_type(&self) -> CalculatedMeasureType {
        self.calc_type
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
