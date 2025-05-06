use super::pretty_print::*;
use super::Cube;
use super::SimpleQuery;
use crate::planner::sql_evaluator::{MemberSymbol, SqlCall};
use std::rc::Rc;

#[derive(Clone)]
pub struct CubeJoinItem {
    pub cube: Rc<Cube>,
    pub on_sql: Rc<SqlCall>,
}

impl PrettyPrint for CubeJoinItem {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println(&format!("CubeJoinItem: "), state);
        let details_state = state.new_level();
        self.cube.pretty_print(result, &details_state);
    }
}

#[derive(Clone)]
pub struct SubqueryDimensionJoinItem {
    pub subquery: Rc<SimpleQuery>,
    pub dimension: Rc<MemberSymbol>,
    pub primary_keys_dimensions: Vec<Rc<MemberSymbol>>,
}

impl PrettyPrint for SubqueryDimensionJoinItem {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println(
            &format!(
                "SubqueryDimensionJoinItem for dimension `{}`: ",
                self.dimension.full_name()
            ),
            state,
        );
        result.println("subquery:", state);
        result.println("primary_keys_dimensions:", state);
        let state = state.new_level();
        for dim in self.primary_keys_dimensions.iter() {
            result.println(&format!("- {}", dim.full_name()), &state);
        }
    }
}

#[derive(Clone)]
pub enum LogicalJoinItem {
    CubeJoinItem(CubeJoinItem),
}

impl PrettyPrint for LogicalJoinItem {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        match self {
            LogicalJoinItem::CubeJoinItem(item) => item.pretty_print(result, state),
        }
    }
}

#[derive(Clone)]
pub struct LogicalJoin {
    pub root: Rc<Cube>,
    pub joins: Vec<LogicalJoinItem>,
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
    }
}
