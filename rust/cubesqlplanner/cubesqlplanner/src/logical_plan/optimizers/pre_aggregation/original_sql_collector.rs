use crate::logical_plan::*;
use crate::planner::query_tools::QueryTools;
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

pub struct OriginalSqlCollector {
    query_tools: Rc<QueryTools>,
}

impl OriginalSqlCollector {
    pub fn new(query_tools: Rc<QueryTools>) -> Self {
        Self { query_tools }
    }

    pub fn collect(&mut self, plan: &Query) -> Result<HashMap<String, String>, CubeError> {
        let mut cube_names_collector = CubeNamesCollector::new();
        cube_names_collector.collect(&plan)?;
        let cube_names = cube_names_collector.result();
        let mut result = HashMap::new();
        for cube_name in cube_names.iter() {
            let pre_aggregations = self
                .query_tools
                .cube_evaluator()
                .pre_aggregations_for_cube_as_array(cube_name.clone())?;
            if let Some(found_pre_aggregation) = pre_aggregations
                .iter()
                .find(|p| p.static_data().pre_aggregation_type == "originalSql")
            {
                let name = if let Some(alias) = &found_pre_aggregation.static_data().sql_alias {
                    alias.clone()
                } else {
                    found_pre_aggregation.static_data().name.clone()
                };
                let table_name = self
                    .query_tools
                    .base_tools()
                    .pre_aggregation_table_name(cube_name.clone(), name)?;
                result.insert(cube_name.clone(), table_name.clone());
            }
        }
        Ok(result)
    }
}
