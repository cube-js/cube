use crate::planner::sql_evaluator::{MemberSymbol, SqlCall};
use std::fmt::Debug;
use std::rc::Rc;

#[derive(Clone)]
pub struct PreAggregationJoinItem {
    pub from: PreAggregationTable,
    pub to: PreAggregationTable,
    pub from_members: Vec<Rc<MemberSymbol>>,
    pub to_members: Vec<Rc<MemberSymbol>>,
    pub on_sql: Rc<SqlCall>,
}

#[derive(Clone)]
pub struct PreAggregationJoin {
    pub root: PreAggregationTable,
    pub items: Vec<PreAggregationJoinItem>,
}

#[derive(Clone)]
pub struct PreAggregationTable {
    pub cube_name: String,
    pub name: String,
    pub alias: Option<String>,
}

#[derive(Clone)]
pub enum PreAggregationSource {
    Table(PreAggregationTable),
    Join(PreAggregationJoin),
}

#[derive(Clone)]
pub struct CompiledPreAggregation {
    pub cube_name: String,
    pub name: String,
    pub source: Rc<PreAggregationSource>,
    pub granularity: Option<String>,
    pub external: Option<bool>,
    pub measures: Vec<Rc<MemberSymbol>>,
    pub dimensions: Vec<Rc<MemberSymbol>>,
    pub time_dimensions: Vec<(Rc<MemberSymbol>, Option<String>)>,
    pub allow_non_strict_date_range_match: bool,
}

impl Debug for CompiledPreAggregation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompiledPreAggregation")
            .field("cube_name", &self.cube_name)
            .field("name", &self.name)
            .field("granularity", &self.granularity)
            .field("external", &self.external)
            .field("measures", &self.measures)
            .field("dimensions", &self.dimensions)
            .field("time_dimensions", &self.time_dimensions)
            .field(
                "allow_non_strict_date_range_match",
                &self.allow_non_strict_date_range_match,
            )
            .finish()
    }
}
