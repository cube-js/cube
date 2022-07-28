use datafusion::arrow::datatypes::DataType;
use std::ops::RangeFrom;

use cubeclient::models::{V1CubeMeta, V1CubeMetaMeasure};

use crate::sql::ColumnType;

use super::V1CubeMetaExt;

#[derive(Debug)]
pub struct MetaContext {
    pub cubes: Vec<V1CubeMeta>,
    pub tables: Vec<CubeMetaTable>,
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
    pub fn new(cubes: Vec<V1CubeMeta>) -> Self {
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

        Self { cubes, tables }
    }

    pub fn find_cube_with_name(&self, name: &str) -> Option<V1CubeMeta> {
        for cube in self.cubes.iter() {
            if cube.name.eq(&name) {
                return Some(cube.clone());
            }
        }

        None
    }

    pub fn find_measure_with_name(&self, name: String) -> Option<V1CubeMetaMeasure> {
        let cube_and_member_name = name.split(".").collect::<Vec<_>>();
        if let Some(cube) = self.find_cube_with_name(cube_and_member_name[0]) {
            cube.lookup_measure(cube_and_member_name[1]).cloned()
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
            },
            V1CubeMeta {
                name: "test2".to_string(),
                title: None,
                dimensions: vec![],
                measures: vec![],
                segments: vec![],
            },
        ];

        let test_context = MetaContext::new(test_cubes);

        match test_context.find_cube_table_with_oid(18000) {
            Some(table) => assert_eq!(18000, table.oid),
            _ => panic!("wrong oid!"),
        }

        match test_context.find_cube_table_with_name("test2".to_string()) {
            Some(table) => assert_eq!(18004, table.oid),
            _ => panic!("wrong name!"),
        }
    }
}
