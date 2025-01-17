use super::query_tools::QueryTools;
use super::sql_evaluator::sql_nodes::{SqlNode, SqlNodesFactory};
use super::sql_evaluator::{MemberSymbol, SqlCall};
use crate::planner::sql_evaluator::SqlEvaluatorVisitor;
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct VisitorContext {
    node_processor: Rc<dyn SqlNode>,
}

impl VisitorContext {
    pub fn new(nodes_factory: &SqlNodesFactory) -> Self {
        Self {
            node_processor: nodes_factory.default_node_processor(),
        }
    }

    pub fn make_visitor(&self, query_tools: Rc<QueryTools>) -> SqlEvaluatorVisitor {
        SqlEvaluatorVisitor::new(query_tools)
    }

    pub fn node_processor(&self) -> Rc<dyn SqlNode> {
        self.node_processor.clone()
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
