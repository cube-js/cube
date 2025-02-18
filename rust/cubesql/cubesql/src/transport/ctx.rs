use datafusion::{arrow::datatypes::DataType, logical_plan::Column};
use itertools::Itertools;
use std::{collections::HashMap, ops::RangeFrom, sync::Arc};
use uuid::Uuid;

use crate::{sql::ColumnType, transport::SqlGenerator};

use super::{CubeMeta, CubeMetaDimension, CubeMetaMeasure, V1CubeMetaExt};

#[derive(Debug)]
pub struct MetaContext {
    pub cubes: Vec<CubeMeta>,
    pub tables: Vec<CubeMetaTable>,
    pub cube_to_data_source: HashMap<String, String>,
    pub data_source_to_sql_generator: HashMap<String, Arc<dyn SqlGenerator + Send + Sync>>,
    pub compiler_id: Uuid,
}

#[derive(Debug, Clone)]
pub struct CubeMetaTable {
    pub oid: u32,
    pub record_oid: u32,
    pub array_handler_oid: u32,
    pub name: String,
    pub description: Option<String>,
    pub columns: Vec<CubeMetaColumn>,
}

#[derive(Debug, Clone)]
pub struct CubeMetaColumn {
    pub oid: u32,
    pub name: String,
    pub description: Option<String>,
    pub column_type: ColumnType,
    pub can_be_null: bool,
}

impl MetaContext {
    pub fn new(
        cubes: Vec<CubeMeta>,
        cube_to_data_source: HashMap<String, String>,
        data_source_to_sql_generator: HashMap<String, Arc<dyn SqlGenerator + Send + Sync>>,
        compiler_id: Uuid,
    ) -> Self {
        // 18000 - max system table oid
        let mut oid_iter: RangeFrom<u32> = 18000..;
        let tables: Vec<CubeMetaTable> = cubes
            .iter()
            .map(|cube| CubeMetaTable {
                oid: oid_iter.next().unwrap_or(0),
                record_oid: oid_iter.next().unwrap_or(0),
                array_handler_oid: oid_iter.next().unwrap_or(0),
                name: cube.name.clone(),
                description: cube.description.clone(),
                columns: cube
                    .get_columns()
                    .iter()
                    .map(|column| CubeMetaColumn {
                        oid: oid_iter.next().unwrap_or(0),
                        name: column.get_name().clone(),
                        description: column.get_description().clone(),
                        column_type: column.get_column_type().clone(),
                        can_be_null: column.sql_can_be_null(),
                    })
                    .collect(),
            })
            .collect();

        Self {
            cubes,
            tables,
            cube_to_data_source,
            data_source_to_sql_generator,
            compiler_id,
        }
    }

    pub fn sql_generator_by_alias_to_cube(
        &self,
        alias_to_cube: &Vec<(String, String)>,
    ) -> Option<Arc<dyn SqlGenerator + Send + Sync>> {
        let data_source = alias_to_cube
            .iter()
            .map(|(_, c)| self.cube_to_data_source.get(c))
            .all_equal_value();

        // Don't care for non-equal data sources, nor for missing cube_to_data_source keys
        let data_source = data_source.ok()??;

        self.data_source_to_sql_generator.get(data_source).cloned()
    }

    pub fn find_cube_with_name(&self, name: &str) -> Option<&CubeMeta> {
        self.cubes.iter().find(|&cube| cube.name == name)
    }

    pub fn find_cube_by_column<'meta, 'alias>(
        &'meta self,
        alias_to_cube: &'alias Vec<(String, String)>,
        column: &Column,
    ) -> Option<(&'alias str, &'meta CubeMeta)> {
        (if let Some(rel) = column.relation.as_ref() {
            alias_to_cube.iter().find(|(a, _)| a == rel)
        } else {
            alias_to_cube.iter().find(|(_, c)| {
                if let Some(cube) = self.find_cube_with_name(c) {
                    // TODO replace cube.contains_member(&cube.member_name(...)) with searching by prepared column names
                    cube.contains_member(&cube.member_name(&column.name))
                } else {
                    false
                }
            })
        })
        .and_then(|(a, c)| self.find_cube_with_name(c).map(|cube| (a.as_str(), cube)))
    }

    pub fn find_cube_by_column_for_replacer<'alias>(
        &self,
        alias_to_cube: &'alias Vec<((String, String), String)>,
        column: &Column,
    ) -> Vec<((&'alias str, &'alias str), &CubeMeta)> {
        if let Some(rel) = column.relation.as_ref() {
            alias_to_cube
                .iter()
                .filter_map(|((old, new), c)| {
                    if old == rel {
                        self.find_cube_with_name(c)
                            .map(|cube| ((old.as_str(), new.as_str()), cube))
                    } else {
                        None
                    }
                })
                .collect()
        } else {
            alias_to_cube
                .iter()
                .filter_map(|((old, new), c)| {
                    if let Some(cube) = self.find_cube_with_name(c) {
                        // TODO replace cube.contains_member(&cube.member_name(...)) with searching by prepared column names
                        if cube.contains_member(&cube.member_name(&column.name)) {
                            return Some(((old.as_str(), new.as_str()), cube));
                        }
                    }

                    None
                })
                .collect()
        }
    }

    pub fn find_measure_with_name(&self, name: &str) -> Option<&CubeMetaMeasure> {
        let mut cube_and_member_name = name.split(".");
        let cube_name = cube_and_member_name.next()?;
        let member_name = cube_and_member_name.next()?;
        let cube = self.find_cube_with_name(cube_name)?;
        cube.lookup_measure(member_name)
    }

    pub fn find_dimension_with_name(&self, name: &str) -> Option<&CubeMetaDimension> {
        let mut cube_and_member_name = name.split(".");
        let cube_name = cube_and_member_name.next()?;
        let member_name = cube_and_member_name.next()?;
        let cube = self.find_cube_with_name(cube_name)?;
        cube.lookup_dimension(member_name)
    }

    pub fn is_synthetic_field(&self, name: &str) -> bool {
        let mut cube_and_member_name = name.split(".");
        let Some(cube_name) = cube_and_member_name.next() else {
            return false;
        };
        let Some(member_name) = cube_and_member_name.next() else {
            return MetaContext::is_synthetic_field_name(cube_name);
        };

        if self.find_cube_with_name(cube_name).is_some() {
            MetaContext::is_synthetic_field_name(member_name)
        } else {
            false
        }
    }

    pub fn is_synthetic_field_name(field_name: &str) -> bool {
        field_name == "__user" || field_name == "__cubeJoinField"
    }

    pub fn find_df_data_type(&self, member_name: &str) -> Option<DataType> {
        let (cube_name, _) = member_name.split_once(".")?;

        self.find_cube_with_name(cube_name)?
            .df_data_type(member_name)
    }

    pub fn find_cube_table_with_oid(&self, oid: u32) -> Option<&CubeMetaTable> {
        self.tables.iter().find(|table| table.oid == oid)
    }

    pub fn find_cube_table_with_name(&self, name: &str) -> Option<&CubeMetaTable> {
        self.tables.iter().find(|table| table.name == name)
    }

    pub fn cube_has_join(&self, cube_name: &str, join_name: &str) -> bool {
        if let Some(cube) = self.find_cube_with_name(cube_name) {
            if let Some(joins) = &cube.joins {
                return joins.iter().any(|j| j.name == join_name);
            }
        }

        return false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::CubeMetaType;

    #[test]
    fn test_find_tables() {
        let test_cubes = vec![
            CubeMeta {
                name: "test1".to_string(),
                description: None,
                title: None,
                r#type: CubeMetaType::Cube,
                dimensions: vec![],
                measures: vec![],
                segments: vec![],
                joins: None,
                folders: None,
                hierarchies: None,
                meta: None,
            },
            CubeMeta {
                name: "test2".to_string(),
                description: None,
                title: None,
                r#type: CubeMetaType::Cube,
                dimensions: vec![],
                measures: vec![],
                segments: vec![],
                joins: None,
                folders: None,
                hierarchies: None,
                meta: None,
            },
        ];

        // TODO
        let test_context =
            MetaContext::new(test_cubes, HashMap::new(), HashMap::new(), Uuid::new_v4());

        match test_context.find_cube_table_with_oid(18000) {
            Some(table) => assert_eq!(18000, table.oid),
            _ => panic!("wrong oid!"),
        }

        match test_context.find_cube_table_with_name("test2") {
            Some(table) => assert_eq!(18005, table.oid),
            _ => panic!("wrong name!"),
        }
    }
}
