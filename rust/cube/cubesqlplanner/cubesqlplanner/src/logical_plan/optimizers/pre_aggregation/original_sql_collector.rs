use crate::logical_plan::*;
use crate::planner::query_tools::QueryTools;
use crate::planner::CubeId;
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

    pub fn collect(&mut self, plan: &Rc<RootQuery>) -> Result<HashMap<CubeId, String>, CubeError> {
        let cube_names = collect_cube_names_from_node(&plan)?;
        let mut found = Vec::new();
        for cube_name in cube_names.iter() {
            let pre_aggregation = self
                .query_tools
                .cube_evaluator()
                .pre_aggregations_for_cube_as_array(cube_name.target().to_string())?
                .into_iter()
                .find(|p| p.static_data().pre_aggregation_type == "originalSql");
            found.push((cube_name, pre_aggregation));
        }
        // A Cube Store table can only be read when every cube of the query is in Cube Store.
        let runs_in_cube_store = found.iter().all(|(_, p)| {
            p.as_ref()
                .is_some_and(|p| p.static_data().external == Some(true))
        });

        let mut result = HashMap::new();
        for (cube_name, pre_aggregation) in found {
            let Some(pre_aggregation) = pre_aggregation else {
                continue;
            };
            let static_data = pre_aggregation.static_data();
            if static_data.external == Some(true) && !runs_in_cube_store {
                continue;
            }
            let name = static_data
                .sql_alias
                .clone()
                .unwrap_or_else(|| static_data.name.clone());
            let table_name = self
                .query_tools
                .base_tools()
                .pre_aggregation_table_name(cube_name.target().to_string(), name)?;
            result.insert(cube_name.clone(), table_name);
        }
        Ok(result)
    }
}
