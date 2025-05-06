use super::query_tools::QueryTools;
use super::sql_evaluator::sql_nodes::{SqlNode, SqlNodesFactory};
use super::sql_evaluator::{MemberSymbol, SqlCall};
use crate::plan::Filter;
use crate::planner::sql_evaluator::SqlEvaluatorVisitor;
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct FiltersContext {
    pub use_local_tz: bool,
}

impl Default for FiltersContext {
    fn default() -> Self {
        Self {
            use_local_tz: false,
        }
    }
}

pub struct VisitorContext {
    node_processor: Rc<dyn SqlNode>,
    all_filters: Option<Filter>, //To pass to FILTER_PARAMS and FILTER_GROUP
    filters_context: FiltersContext,
}

impl VisitorContext {
    pub fn new(nodes_factory: &SqlNodesFactory, all_filters: Option<Filter>) -> Self {
        let filters_context = FiltersContext {
            use_local_tz: nodes_factory.use_local_tz_in_date_range(),
        };
        Self {
            node_processor: nodes_factory.default_node_processor(),
            all_filters,
            filters_context,
        }
    }

    pub fn make_visitor(&self, query_tools: Rc<QueryTools>) -> SqlEvaluatorVisitor {
        SqlEvaluatorVisitor::new(query_tools, self.all_filters.clone())
    }

    pub fn node_processor(&self) -> Rc<dyn SqlNode> {
        self.node_processor.clone()
    }

    pub fn filters_context(&self) -> &FiltersContext {
        &self.filters_context
    }
}

pub fn evaluate_with_context(
    node: &Rc<MemberSymbol>,
    query_tools: Rc<QueryTools>,
    context: Rc<VisitorContext>,
    templates: &PlanSqlTemplates,
) -> Result<String, CubeError> {
    let visitor = context.make_visitor(query_tools);
    let node_processor = context.node_processor();

    visitor.apply(node, node_processor, templates)
}

pub fn evaluate_sql_call_with_context(
    sql_call: &Rc<SqlCall>,
    query_tools: Rc<QueryTools>,
    context: Rc<VisitorContext>,
    templates: &PlanSqlTemplates,
) -> Result<String, CubeError> {
    let visitor = context.make_visitor(query_tools.clone());
    let node_processor = context.node_processor();
    sql_call.eval(&visitor, node_processor, query_tools, templates)
}
