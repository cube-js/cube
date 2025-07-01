use crate::cube_bridge::member_sql::MemberSql;
use crate::cube_bridge::pre_aggregation_description::PreAggregationDescription;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::MemberSymbol;
use cubenativeutils::CubeError;
use std::fmt::Debug;
use std::rc::Rc;
#[derive(Clone)]
pub enum PreAggregationSource {
    Table(String),
}

#[derive(Clone)]
pub struct CompiledPreAggregation {
    pub cube_name: String,
    pub name: String,
    pub source: PreAggregationSource,
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
