use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::BaseMember;
use itertools::Itertools;
use std::rc::Rc;

#[derive(Debug, Clone)]
pub struct SchemaColumn {
    pub table_name: Option<String>,
    pub alias: String,
    pub origin_member: String,
}

impl SchemaColumn {
    pub fn new(table_name: Option<String>, alias: String, origin_member: String) -> Self {
        Self {
            table_name,
            alias,
            origin_member,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SchemaCube {
    pub name: String,
    pub alias: String,
}

impl SchemaCube {
    pub fn new(name: String, alias: String) -> Self {
        Self { name, alias }
    }
}

#[derive(Debug)]
pub struct Schema {
    columns: Vec<SchemaColumn>,
    cubes: Vec<SchemaCube>,
}

impl Schema {
    pub fn empty() -> Self {
        Self::new(vec![], vec![])
    }
    pub fn new(columns: Vec<SchemaColumn>, cubes: Vec<SchemaCube>) -> Self {
        Self { columns, cubes }
    }

    pub fn add_column(&mut self, column: SchemaColumn) {
        self.columns.push(column)
    }

    pub fn add_cube(&mut self, cube: SchemaCube) {
        self.cubes.push(cube)
    }
    pub fn merge(&mut self, other: Self) {
        let Schema {
            mut columns,
            mut cubes,
        } = other;
        self.columns.append(&mut columns);
        self.cubes.append(&mut cubes);
    }

    pub fn resolve_member_alias(
        &self,
        member: &Rc<dyn BaseMember>,
        source: &Option<String>,
    ) -> String {
        if let Some(column) = self.find_column_for_member(&member.full_name(), source) {
            column.alias.clone()
        } else {
            PlanSqlTemplates::memeber_alias_name(
                member.cube_name(),
                member.name(),
                member.alias_suffix(),
            )
        }
    }

    pub fn resolve_member_reference(
        &self,
        member_name: &String,
        source: &Option<String>,
    ) -> Option<String> {
        if let Some(column) = self.find_column_for_member(&member_name, source) {
            Some(column.alias.clone())
        } else {
            None
        }
    }

    pub fn resolve_cube_alias(&self, cube_name: &String) -> String {
        if let Some(cube) = self.find_cube_by_origin_cube_name(cube_name) {
            cube.alias.clone()
        } else {
            cube_name.clone()
        }
    }

    pub fn find_column_for_member(
        &self,
        member_name: &String,
        source: &Option<String>,
    ) -> Option<&SchemaColumn> {
        self.columns.iter().find(|col| {
            if source.is_some() && source != &col.table_name {
                return false;
            }
            &col.origin_member == member_name
        })
    }

    pub fn find_cube_by_origin_cube_name(&self, member_cube_name: &String) -> Option<&SchemaCube> {
        self.cubes
            .iter()
            .find(|cube| &cube.name == member_cube_name)
    }

    pub fn extract_source_schema(&self, source: &String) -> Rc<Self> {
        let columns = self
            .columns
            .iter()
            .filter(|col| col.table_name.is_some() && col.table_name.as_ref().unwrap() == source)
            .cloned()
            .collect_vec();
        let cubes = self
            .cubes
            .iter()
            .filter(|cb| &cb.alias == source)
            .cloned()
            .collect_vec();
        Rc::new(Self { columns, cubes })
    }

    pub fn move_to_source(&self, source: &String) -> Self {
        let mut columns = self.columns.clone();
        for col in columns.iter_mut() {
            col.table_name = Some(source.clone())
        }
        Self {
            columns,
            cubes: vec![], //we not fill cubes here
        }
    }
}
