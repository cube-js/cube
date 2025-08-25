use super::SchemaColumn;
use crate::planner::sql_evaluator::MemberSymbol;
use itertools::Itertools;
use std::rc::Rc;

#[derive(Debug, Clone)]
pub struct Schema {
    columns: Vec<SchemaColumn>,
}

impl Schema {
    pub fn empty() -> Self {
        Self::new(vec![])
    }
    pub fn new(columns: Vec<SchemaColumn>) -> Self {
        Self { columns }
    }

    pub fn add_column(&mut self, column: SchemaColumn) {
        self.columns.push(column)
    }

    pub fn has_column(&self, column_name: &String) -> bool {
        self.columns.iter().any(|col| col.name() == column_name)
    }

    pub fn merge(&mut self, other: &Self) {
        let res = self
            .columns
            .iter()
            .chain(other.columns.iter())
            .unique_by(|col| col.name())
            .collect_vec();
        self.columns = res.into_iter().cloned().collect_vec();
    }

    pub fn resolve_member_alias(&self, member: &Rc<MemberSymbol>) -> String {
        if let Some(column) = self.find_column_for_member(&member.full_name()) {
            column.name().clone()
        } else {
            member.alias()
        }
    }

    pub fn resolve_member_reference(&self, member_name: &String) -> Option<String> {
        if let Some(column) = self.find_column_for_member(&member_name) {
            Some(column.name().clone())
        } else {
            None
        }
    }

    pub fn find_column_for_member(&self, member_name: &String) -> Option<&SchemaColumn> {
        self.columns.iter().find(|col| {
            if let Some(origin_member) = &col.origin_member() {
                origin_member == member_name
            } else {
                false
            }
        })
    }
}
