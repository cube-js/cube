use itertools::Itertools;

use crate::logical_plan::*;
use crate::plan::{FilterItem, Filter};
use std::rc::Rc;
use crate::planner::sql_evaluator::MemberSymbol;

pub struct OptimizerHelper;


impl OptimizerHelper {
    pub fn new() -> Self {
        Self
    }
    pub fn all_dimensions(&self, schema: &Rc<LogicalSchema>, filters: &Rc<LogicalFilter>) -> Vec<Rc<MemberSymbol>> {
        let mut result = schema.dimensions.clone();
        self.fill_members_from_filters(&filters.dimensions_filters, &mut result);
        self.fill_members_from_filters(&filters.segments, &mut result); 
        result.into_iter().unique_by(|s| s.full_name()).collect()
    }

    pub fn all_measures(&self, schema: &Rc<LogicalSchema>, filters: &Rc<LogicalFilter>) -> Vec<Rc<MemberSymbol>> {
        let mut result = schema.measures.clone();
        self.fill_members_from_filters(&filters.measures_filter, &mut result);
        result.into_iter().unique_by(|s| s.full_name()).collect()
    }

    pub fn all_time_dimensions(&self, schema: &Rc<LogicalSchema>, filters: &Rc<LogicalFilter>) -> Vec<Rc<MemberSymbol>> {
        let mut result = schema.time_dimensions.clone();
        result.into_iter().unique_by(|s| s.full_name()).collect()
    }

    fn fill_members_from_filters(&self, filters: &Vec<FilterItem>, members: &mut Vec<Rc<MemberSymbol>>) {
        for item in filters.iter() {
            self.fill_members_from_filter_item(item, members);
        }
    }
        
    fn fill_members_from_filter_item(&self, item: &FilterItem, members: &mut Vec<Rc<MemberSymbol>>) {
        match item {
            FilterItem::Group(group) => {
                for item in group.items.iter() {
                    self.fill_members_from_filter_item(item, members)
                }
            }
            FilterItem::Item(item) => {
                members.push(item.member_evaluator().clone());
            }
            FilterItem::Segment(segment) => { 
                members.push(segment.member_evaluator().clone());
            }
        }
    }
}

