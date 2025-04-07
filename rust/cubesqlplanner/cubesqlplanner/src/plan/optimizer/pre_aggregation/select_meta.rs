use crate::plan::{Expr, Filter, FilterItem, Select};
use crate::planner::sql_evaluator::collectors::collect_cube_names_from_vec;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::sql_evaluator::TimeDimensionSymbol;
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::HashSet;
use std::rc::Rc;

pub struct SelectMeta {
    cube_names: Vec<String>,
    dimensions: Vec<Rc<MemberSymbol>>,
    measures: Vec<Rc<MemberSymbol>>,
    time_dimensions: Vec<TimeDimensionSymbol>,
}

impl SelectMeta {
    pub fn new(
        cube_names: Vec<String>,
        dimensions: Vec<Rc<MemberSymbol>>,
        measures: Vec<Rc<MemberSymbol>>,
        time_dimensions: Vec<TimeDimensionSymbol>,
    ) -> Self {
        Self {
            cube_names,
            dimensions,
            measures,
            time_dimensions,
        }
    }

    pub fn cube_names(&self) -> &Vec<String> {
        &self.cube_names
    }

    pub fn dimensions(&self) -> &Vec<Rc<MemberSymbol>> {
        &self.dimensions
    }

    pub fn measures(&self) -> &Vec<Rc<MemberSymbol>> {
        &self.measures
    }

    pub fn time_dimensions(&self) -> &Vec<TimeDimensionSymbol> {
        &self.time_dimensions
    }
}

pub struct SelectMetaCollector {
    all_symbols: Vec<Rc<MemberSymbol>>,
}

impl SelectMetaCollector {
    pub fn new() -> Self {
        Self {
            all_symbols: Vec::new(),
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

    pub fn extract_result(mut self) -> Result<SelectMeta, CubeError> {
        self.all_symbols = self
            .all_symbols
            .into_iter()
            .unique_by(|s| s.full_name())
            .collect_vec(); //TODO find owned by cubes
        let cube_names = collect_cube_names_from_vec(&self.all_symbols)?;

        let time_dimensions = self
            .all_symbols
            .iter()
            .filter_map(|s| match s.as_ref() {
                MemberSymbol::TimeDimension(time_dimension) => Some(time_dimension.clone()),
                _ => None,
            })
            .collect_vec();
        println!("!!! time dimensions: {}", time_dimensions.len());

        let dimensions = self
            .all_symbols
            .iter()
            .filter(|s| match s.as_ref() {
                MemberSymbol::Dimension(_) => time_dimensions.iter().find(|td| td.base_symbol().full_name() == s.full_name()).is_none(),
                _ => false,
            })
            .cloned()
            .collect_vec();

        let measures = self
            .all_symbols
            .iter()
            .filter(|s| matches!(s.as_ref(), MemberSymbol::Measure(_)))
            .cloned()
            .collect_vec();
        let meta = SelectMeta::new(cube_names, dimensions, measures, time_dimensions);

        Ok(meta)
    }

    fn process_expression(&mut self, expr: &Expr) -> Result<(), CubeError> {
        match expr {
            Expr::Member(member) => {
                self.all_symbols
                    .push(member.member.member_evaluator().clone());
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
            FilterItem::Item(item) => self.all_symbols.push(item.member_evaluator().clone()),
            FilterItem::Segment(segment) => {
                self.all_symbols.push(segment.member_evaluator().clone())
            }
        }
        Ok(())
    }
}
