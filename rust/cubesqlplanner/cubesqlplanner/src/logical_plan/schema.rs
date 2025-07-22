use itertools::Itertools;

use super::pretty_print::*;
use crate::planner::sql_evaluator::MemberSymbol;
use std::collections::HashSet;
use std::rc::Rc;

#[derive(Default, Clone)]
pub struct LogicalSchema {
    pub time_dimensions: Vec<Rc<MemberSymbol>>,
    pub dimensions: Vec<Rc<MemberSymbol>>,
    pub measures: Vec<Rc<MemberSymbol>>,
    pub multiplied_measures: HashSet<String>,
}

impl LogicalSchema {
    pub fn set_time_dimensions(mut self, time_dimensions: Vec<Rc<MemberSymbol>>) -> Self {
        self.time_dimensions = time_dimensions;
        self
    }

    pub fn set_dimensions(mut self, dimensions: Vec<Rc<MemberSymbol>>) -> Self {
        self.dimensions = dimensions;
        self
    }

    pub fn set_measures(mut self, measures: Vec<Rc<MemberSymbol>>) -> Self {
        self.measures = measures;
        self
    }

    pub fn set_multiplied_measures(mut self, multiplied_measures: HashSet<String>) -> Self {
        self.multiplied_measures = multiplied_measures;
        self
    }

    pub fn into_rc(self) -> Rc<Self> {
        Rc::new(self)
    }
}

impl LogicalSchema {
    pub fn find_member_positions(&self, name: &str) -> Vec<usize> {
        let mut result = Vec::new();
        for (i, m) in self.dimensions.iter().enumerate() {
            if m.full_name() == name {
                result.push(i);
            }
        }
        for (i, m) in self.time_dimensions.iter().enumerate() {
            if m.full_name() == name {
                result.push(i + self.dimensions.len());
            } else if let Ok(time_dimension) = m.as_time_dimension() {
                if time_dimension.base_symbol().full_name() == name {
                    result.push(i + self.dimensions.len());
                }
            }
        }
        for (i, m) in self.measures.iter().enumerate() {
            if m.full_name() == name {
                result.push(i + self.time_dimensions.len() + self.dimensions.len());
            }
        }
        result
    }

    pub fn all_dimensions(&self) -> impl Iterator<Item = &Rc<MemberSymbol>> {
        self.dimensions.iter().chain(self.time_dimensions.iter())
    }

    pub fn all_members(&self) -> impl Iterator<Item = &Rc<MemberSymbol>> {
        self.all_dimensions().chain(self.measures.iter())
    }

    pub fn has_dimensions(&self) -> bool {
        !self.time_dimensions.is_empty() || !self.dimensions.is_empty()
    }
}

impl PrettyPrint for LogicalSchema {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println(
            &format!("-time_dimensions: {}", print_symbols(&self.time_dimensions)),
            state,
        );
        result.println(
            &format!("-dimensions: {}", print_symbols(&self.dimensions)),
            state,
        );
        result.println(
            &format!("-measures: {}", print_symbols(&self.measures)),
            state,
        );
        if !self.multiplied_measures.is_empty() {
            result.println(
                &format!(
                    "-multiplied_measures: {}",
                    self.multiplied_measures.iter().join(", ")
                ),
                state,
            );
        }
    }
}
