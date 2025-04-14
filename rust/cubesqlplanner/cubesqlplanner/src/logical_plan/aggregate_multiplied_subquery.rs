use super::pretty_print::*;
use super::LogicalFilter;
use super::LogicalJoin;
use super::*;
use crate::plan::{Expr, Filter, FilterItem, MemberExpression};
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::query_properties::OrderByItem;
use std::rc::Rc;

pub enum AggregateMultipliedSubquerySouce {
    Cube(Rc<Cube>)
}

impl PrettyPrint for AggregateMultipliedSubquerySouce {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        match self {
            AggregateMultipliedSubquerySouce::Cube(cube) => {
                result.println(&format!("Cube: {}", cube.cube.name()), state);
            }
        }
    }
}
    


pub struct AggregateMultipliedSubquery {
    pub schema: Rc<LogicalSchema>,
    pub dimension_subqueries: Vec<Rc<DimensionSubQuery>>,
    pub keys_subquery: Rc<KeysSubQuery>,
    pub source: Rc<AggregateMultipliedSubquerySouce>,
    
}

impl PrettyPrint for AggregateMultipliedSubquery {    
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println("AggregateMultipliedSubquery: ", state);
        let state = state.new_level();
        let details_state = state.new_level();
        result.println("schema:", &state);
        self.schema.pretty_print(result, &details_state);
        if !self.dimension_subqueries.is_empty() {
            result.println("dimension_subqueries:", &state);
            for subquery in self.dimension_subqueries.iter() {
                subquery.pretty_print(result, &details_state);
            }
        }
        result.println("keys_subquery:", &state);
        self.keys_subquery.pretty_print(result, &details_state);
        result.println("source:", &state);
        self.source.pretty_print(result, &details_state);

    }
}

