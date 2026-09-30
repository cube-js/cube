use super::sql_nodes::SqlNode;
use super::SqlEvaluatorVisitor;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_call::CubeRef;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::CubeId;
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

pub struct CubeRefEvaluator {
    cube_name_references: HashMap<CubeId, String>,
    original_sql_pre_aggregations: HashMap<CubeId, String>,
}

impl CubeRefEvaluator {
    pub fn new(
        cube_name_references: HashMap<CubeId, String>,
        original_sql_pre_aggregations: HashMap<CubeId, String>,
    ) -> Self {
        Self {
            cube_name_references,
            original_sql_pre_aggregations,
        }
    }

    pub fn evaluate(
        &self,
        cube_ref: &CubeRef,
        visitor: &SqlEvaluatorVisitor,
        node_processor: Rc<dyn SqlNode>,
        query_tools: Rc<QueryTools>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        match cube_ref {
            CubeRef::Name(symbol) => {
                let alias = self.resolve_cube_alias(symbol.cube_name());
                templates.quote_identifier(&alias)
            }
            CubeRef::Table(symbol) => {
                if let Some(pre_agg) = self.original_sql_pre_aggregations.get(symbol.cube_name()) {
                    return Ok(pre_agg.clone());
                }
                symbol.evaluate_sql(visitor, node_processor, query_tools, templates)
            }
        }
    }

    fn resolve_cube_alias(&self, name: &CubeId) -> String {
        if let Some(alias) = self.cube_name_references.get(name) {
            alias.clone()
        } else {
            name.to_string()
        }
    }
}
