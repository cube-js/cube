use super::sql_nodes::SqlNode;
use super::SqlEvaluatorVisitor;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::sql_call::CubeRef;
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

pub struct CubeRefEvaluator {
    cube_name_references: HashMap<String, String>,
    original_sql_pre_aggregations: HashMap<String, String>,
}

impl CubeRefEvaluator {
    pub fn new(
        cube_name_references: HashMap<String, String>,
        original_sql_pre_aggregations: HashMap<String, String>,
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
                let name = symbol.evaluate_sql()?;
                let alias = self.resolve_cube_alias(&name);
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

    fn resolve_cube_alias(&self, name: &String) -> String {
        if let Some(alias) = self.cube_name_references.get(name) {
            alias.clone()
        } else {
            name.clone()
        }
    }
}
