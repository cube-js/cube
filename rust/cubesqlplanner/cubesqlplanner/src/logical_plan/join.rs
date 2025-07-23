use super::pretty_print::*;
use super::*;
use crate::planner::sql_evaluator::SqlCall;
use std::rc::Rc;

#[derive(Clone)]
pub struct LogicalJoinItem {
    pub cube: Rc<Cube>,
    pub on_sql: Rc<SqlCall>,
}

impl PrettyPrint for LogicalJoinItem {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println(&format!("CubeJoinItem: "), state);
        let details_state = state.new_level();
        self.cube.pretty_print(result, &details_state);
    }
}

#[derive(Clone)]
pub struct LogicalJoin {
    pub root: Rc<Cube>,
    pub joins: Vec<LogicalJoinItem>,
    pub dimension_subqueries: Vec<Rc<DimensionSubQuery>>,
}


impl PrettyPrint for LogicalJoin {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println(&format!("Join: "), state);
        let state = state.new_level();
        let details_state = state.new_level();
        result.println(&format!("root: "), &state);
        self.root.pretty_print(result, &details_state);
        result.println(&format!("joins: "), &state);
        let state = state.new_level();
        for join in self.joins.iter() {
            join.pretty_print(result, &state);
        }
        if !self.dimension_subqueries.is_empty() {
            result.println("dimension_subqueries:", &state);
            let details_state = state.new_level();
            for subquery in self.dimension_subqueries.iter() {
                subquery.pretty_print(result, &details_state);
            }
        }
    }
}
