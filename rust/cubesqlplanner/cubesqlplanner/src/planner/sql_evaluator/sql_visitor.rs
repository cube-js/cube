use super::sql_nodes::SqlNode;
use super::MemberSymbol;
use crate::plan::Filter;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::rc::Rc;

#[derive(Clone)]
pub struct SqlEvaluatorVisitor {
    query_tools: Rc<QueryTools>,
    all_filters: Option<Filter>, //To pass to FILTER_PARAMS and FILTER_GROUP
}

impl SqlEvaluatorVisitor {
    pub fn new(query_tools: Rc<QueryTools>, all_filters: Option<Filter>) -> Self {
        Self {
            query_tools,
            all_filters,
        }
    }

    pub fn all_filters(&self) -> Option<Filter> {
        self.all_filters.clone()
    }

    pub fn apply(
        &self,
        node: &Rc<MemberSymbol>,
        node_processor: Rc<dyn SqlNode>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        let result = node_processor.to_sql(
            self,
            node,
            self.query_tools.clone(),
            node_processor.clone(),
            templates,
        )?;
        Ok(result)
    }
}
