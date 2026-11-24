use super::SchemaColumn;
use crate::planner::MemberSymbol;
use itertools::Itertools;
use std::rc::Rc;

#[derive(Default, Debug, Clone)]
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
        if let Some(column) = self.find_column_for_member(member) {
            column.name().clone()
        } else {
            member.alias()
        }
    }

    pub fn resolve_member_reference(&self, member: &Rc<MemberSymbol>) -> Option<String> {
        if let Some(column) = self.find_column_for_member(&member) {
            Some(column.name().clone())
        } else {
            None
        }
    }

    /// The column carrying `member` itself, else the first one whose member
    /// resolves to the same base member.
    pub fn find_column_for_member(&self, member: &Rc<MemberSymbol>) -> Option<&SchemaColumn> {
        // Two view members can resolve to one cube member (a dimension included
        // plain and under an alias); each must still find its own column.
        let own_column = self.columns.iter().find(|col| {
            col.origin_member()
                .as_ref()
                .is_some_and(|origin| origin == member)
        });
        own_column.or_else(|| self.find_column_by_resolved_member(member))
    }

    fn find_column_by_resolved_member(&self, member: &Rc<MemberSymbol>) -> Option<&SchemaColumn> {
        self.columns.iter().find(|col| {
            if let Some(origin_member) = &col.origin_member() {
                origin_member.clone().resolve_reference_chain()
                    == member.clone().resolve_reference_chain()
            } else {
                false
            }
        })
    }
}
