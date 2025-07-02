use super::pre_aggregation::PreAggregationSource;
use super::*;
use crate::planner::sql_evaluator::MemberSymbol;
use itertools::Itertools;
use std::rc::Rc;

pub struct PreAggregation {
    pub name: String,
    pub schema: Rc<LogicalSchema>,
    pub measures: Vec<Rc<MemberSymbol>>,
    pub dimensions: Vec<Rc<MemberSymbol>>,
    pub time_dimensions: Vec<(Rc<MemberSymbol>, Option<String>)>,
    pub external: bool,
    pub granularity: Option<String>,
    pub source: Rc<PreAggregationSource>,
    pub cube_name: String,
}

impl PrettyPrint for PreAggregation {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println("PreAggregation: ", state);
        let state = state.new_level();
        result.println(&format!("name: {}", self.name), &state);
        result.println(&format!("cube_name: {}", self.cube_name), &state);
        result.println(&format!("source:"), &state);
        match self.source.as_ref() {
            PreAggregationSource::Table(table) => {
                let state = state.new_level();
                result.println(
                    &format!("table: {}.{}", table.cube_name, table.name),
                    &state,
                );
            }
            PreAggregationSource::Join(_) => {
                let state = state.new_level();
                result.println(&format!("rollup join"), &state);
            }
        }
        result.println(&format!("external: {}", self.external), &state);
        result.println(
            &format!(
                "granularity: {}",
                self.granularity.clone().unwrap_or("None".to_string())
            ),
            &state,
        );
        result.println(
            &format!(
                "-time_dimensions: {}",
                &self
                    .time_dimensions
                    .iter()
                    .map(|(d, granularity)| format!(
                        "({} {})",
                        d.full_name(),
                        granularity.clone().unwrap_or("None".to_string())
                    ))
                    .join(", ")
            ),
            &state,
        );
        result.println(
            &format!("-dimensions: {}", print_symbols(&self.dimensions)),
            &state,
        );
        result.println(
            &format!("-measures: {}", print_symbols(&self.measures)),
            &state,
        );
    }
}
