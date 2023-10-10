use datafusion::{arrow::datatypes::DataType, logical_plan::Column};
use itertools::Itertools;
use std::{collections::HashMap, ops::RangeFrom, sync::Arc};

use cubeclient::models::{V1CubeMeta, V1CubeMetaDimension, V1CubeMetaMeasure};

use crate::{sql::ColumnType, transport::SqlGenerator};

use super::V1CubeMetaExt;

#[derive(Debug)]
pub struct MetaContext {
    pub cubes: Vec<V1CubeMeta>,
    pub tables: Vec<CubeMetaTable>,
    pub cube_to_data_source: HashMap<String, String>,
    pub data_source_to_sql_generator: HashMap<String, Arc<dyn SqlGenerator + Send + Sync>>,
}

#[derive(Debug, Clone)]
pub struct CubeMetaTable {
    pub oid: u32,
    pub record_oid: u32,
    pub array_handler_oid: u32,
    pub name: String,
    pub columns: Vec<CubeMetaColumn>,
}

#[derive(Debug, Clone)]
pub struct CubeMetaColumn {
    pub oid: u32,
    pub name: String,
    pub column_type: ColumnType,
    pub can_be_null: bool,
}

impl MetaContext {
    pub fn new(
        cubes: Vec<V1CubeMeta>,
        cube_to_data_source: HashMap<String, String>,
        data_source_to_sql_generator: HashMap<String, Arc<dyn SqlGenerator + Send + Sync>>,
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
                columns: cube
                    .get_columns()
                    .iter()
                    .map(|column| CubeMetaColumn {
                        oid: oid_iter.next().unwrap_or(0),
                        name: column.get_name().clone(),
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
        }
    }

    pub fn sql_generator_by_alias_to_cube(
        &self,
        alias_to_cube: &Vec<(String, String)>,
    ) -> Option<Arc<dyn SqlGenerator + Send + Sync>> {
        let data_sources = alias_to_cube
            .iter()
            .map(|(_, c)| self.cube_to_data_source.get(c))
            .unique()
            .collect::<Option<Vec<_>>>()?;
        if data_sources.len() != 1 {
            return None;
        }
        self.data_source_to_sql_generator
            .get(data_sources[0].as_str())
            .cloned()
    }

    pub fn find_cube_with_name(&self, name: &str) -> Option<V1CubeMeta> {
        for cube in self.cubes.iter() {
            if cube.name.eq(&name) {
                return Some(cube.clone());
            }
        }

        None
    }

    pub fn find_cube_by_column(
        &self,
        alias_to_cube: &Vec<(String, String)>,
        column: &Column,
    ) -> Option<(String, V1CubeMeta)> {
        (if let Some(rel) = column.relation.as_ref() {
            alias_to_cube.iter().find(|(a, _)| a == rel)
        } else {
            alias_to_cube.iter().find(|(_, c)| {
                if let Some(cube) = self.find_cube_with_name(c) {
                    cube.contains_member(&cube.member_name(&column.name))
                } else {
                    false
                }
            })
        })
        .and_then(|(a, c)| {
            self.find_cube_with_name(c)
                .map(|cube| (a.to_string(), cube))
        })
    }

    pub fn find_cube_by_column_for_replacer(
        &self,
        alias_to_cube: &Vec<((String, String), String)>,
        column: &Column,
    ) -> Vec<((String, String), V1CubeMeta)> {
        if let Some(rel) = column.relation.as_ref() {
            alias_to_cube
                .iter()
                .filter_map(|((old, new), c)| {
                    if old == rel {
                        self.find_cube_with_name(c)
                            .map(|cube| ((old.to_string(), new.to_string()), cube))
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
                        if cube.contains_member(&cube.member_name(&column.name)) {
                            return Some(((old.to_string(), new.to_string()), cube));
                        }
                    }

                    None
                })
                .collect()
        }
    }

    pub fn find_measure_with_name(&self, name: String) -> Option<V1CubeMetaMeasure> {
        let cube_and_member_name = name.split(".").collect::<Vec<_>>();
        if let Some(cube) = self.find_cube_with_name(cube_and_member_name[0]) {
            cube.lookup_measure(cube_and_member_name[1]).cloned()
        } else {
            None
        }
    }

    pub fn find_dimension_with_name(&self, name: String) -> Option<V1CubeMetaDimension> {
        let cube_and_member_name = name.split(".").collect::<Vec<_>>();
        if let Some(cube) = self.find_cube_with_name(cube_and_member_name[0]) {
            cube.lookup_dimension(cube_and_member_name[1]).cloned()
        } else {
            None
        }
    }

    pub fn find_df_data_type(&self, member_name: String) -> Option<DataType> {
        self.find_cube_with_name(member_name.split(".").next()?)?
            .df_data_type(member_name.as_str())
    }

    pub fn find_cube_table_with_oid(&self, oid: u32) -> Option<CubeMetaTable> {
        self.tables.iter().find(|table| table.oid == oid).cloned()
    }

    pub fn find_cube_table_with_name(&self, name: String) -> Option<CubeMetaTable> {
        self.tables.iter().find(|table| table.name == name).cloned()
    }

    pub fn cube_has_join(&self, cube_name: &str, join_name: String) -> bool {
        if let Some(cube) = self.find_cube_with_name(cube_name) {
            if let Some(joins) = cube.joins {
                return joins.iter().any(|j| j.name == join_name);
            }
        }

        return false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_tables() {
        let test_cubes = vec![
            V1CubeMeta {
                name: "test1".to_string(),
                title: None,
                dimensions: vec![],
                measures: vec![],
                segments: vec![],
                joins: None,
            },
            V1CubeMeta {
                name: "test2".to_string(),
                title: None,
                dimensions: vec![],
                measures: vec![],
                segments: vec![],
                joins: None,
            },
        ];

        // TODO
        let test_context = MetaContext::new(test_cubes, HashMap::new(), HashMap::new());

        match test_context.find_cube_table_with_oid(18000) {
            Some(table) => assert_eq!(18000, table.oid),
            _ => panic!("wrong oid!"),
        }

        match test_context.find_cube_table_with_name("test2".to_string()) {
            Some(table) => assert_eq!(18005, table.oid),
            _ => panic!("wrong name!"),
        }
    }
}
