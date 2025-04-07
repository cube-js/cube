use crate::plan::{Expr, Filter, FilterItem, Select};
use crate::planner::sql_evaluator::MemberSymbol;
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::rc::Rc;

pub struct AllSymbolsCollector {
    symbols: Vec<Rc<MemberSymbol>>,
}

impl AllSymbolsCollector {
    pub fn new() -> Self {
        Self {
            symbols: Vec::new(),
        }
    }

    pub fn collect(&mut self, select: Rc<Select>) -> Result<(), CubeError> {
        for expr in &select.projection_columns {
            self.process_expression(&expr.expr)?;
        }
        for expr in &select.group_by {
            self.process_expression(&expr)?;
        }
        for expr in &select.order_by {
            self.process_expression(&expr.expr)?;
        }
        for expr in &select.group_by {
            self.process_expression(&expr)?;
        }
        self.process_filter(&select.filter)?;
        self.process_filter(&select.having)?;
        Ok(())
    }

    pub fn extract_result(self) -> Vec<Rc<MemberSymbol>> {
        self.symbols
            .into_iter()
            .unique_by(|s| s.full_name())
            .collect_vec() //TODO find owned by cubes
    }

    fn process_expression(&mut self, expr: &Expr) -> Result<(), CubeError> {
        match expr {
            Expr::Member(member) => {
                self.symbols.push(member.member.member_evaluator().clone());
            }
            Expr::Reference(reference) => {}
            Expr::Function(function) => {
                for arg in &function.arguments {
                    self.process_expression(arg)?;
                }
            }
        }
        Ok(())
    }

    fn process_filter(&mut self, filter: &Option<Filter>) -> Result<(), CubeError> {
        if let Some(filter) = filter {
            for filter_item in &filter.items {
                self.process_filter_item(filter_item)?;
            }
        }
        Ok(())
    }

    fn process_filter_item(&mut self, filter_item: &FilterItem) -> Result<(), CubeError> {
        match filter_item {
            FilterItem::Group(group) => {
                for filter_item in &group.items {
                    self.process_filter_item(filter_item)?;
                }
            }
            FilterItem::Item(item) => self.symbols.push(item.member_evaluator().clone()),
            FilterItem::Segment(segment) => self.symbols.push(segment.member_evaluator().clone()),
        }
        Ok(())
    }
}
