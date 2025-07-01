use super::*;
use crate::planner::BaseCube;
use std::rc::Rc;

#[derive(Clone)]
pub struct OriginalSqlPreAggregation {
    pub name: String,
}

impl PrettyPrint for OriginalSqlPreAggregation {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println(&format!("OriginalSqlPreAggregation: {}", self.name), state);
    }
}

#[derive(Clone)]
pub struct Cube {
    pub name: String,
    pub cube: Rc<BaseCube>,
    pub original_sql_pre_aggregation: Option<OriginalSqlPreAggregation>,
}

impl PrettyPrint for Cube {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println(&format!("Cube: {}", self.name), state);
        if let Some(original_sql_pre_aggregation) = &self.original_sql_pre_aggregation {
            original_sql_pre_aggregation.pretty_print(result, state);
        }
    }
}

impl Cube {
    pub fn new(cube: Rc<BaseCube>) -> Rc<Self> {
        Rc::new(Self {
            name: cube.name().clone(),
            cube,
            original_sql_pre_aggregation: None,
        })
    }

    pub fn with_original_sql_pre_aggregation(
        self: Rc<Self>,
        original_sql_pre_aggregation: OriginalSqlPreAggregation,
    ) -> Rc<Self> {
        Rc::new(Self {
            name: self.name.clone(),
            cube: self.cube.clone(),
            original_sql_pre_aggregation: Some(original_sql_pre_aggregation),
        })
    }
}
