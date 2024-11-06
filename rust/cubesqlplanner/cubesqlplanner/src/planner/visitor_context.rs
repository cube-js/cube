use super::query_tools::QueryTools;
use super::sql_evaluator::sql_nodes::{SqlNode, SqlNodesFactory};
use super::sql_evaluator::EvaluationNode;
use crate::plan::Schema;
use crate::planner::sql_evaluator::visitor::EvaluatorVisitor;
use crate::planner::sql_evaluator::SqlEvaluatorVisitor;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct VisitorContext {
    source_schema: Rc<Schema>,
    cube_alias_prefix: Option<String>,
    node_processor: Rc<dyn SqlNode>,
}

impl VisitorContext {
    pub fn new(cube_alias_prefix: Option<String>, node_processor: Rc<dyn SqlNode>) -> Self {
        Self {
            cube_alias_prefix,
            node_processor,
            source_schema: Rc::new(Schema::empty()),
        }
    }

    pub fn new_with_cube_alias_prefix(
        nodes_factory: Rc<SqlNodesFactory>,
        cube_alias_prefix: String,
    ) -> Self {
        Self::new(
            Some(cube_alias_prefix),
            nodes_factory.default_node_processor(),
        )
    }

    pub fn default(nodes_factory: Rc<SqlNodesFactory>) -> Self {
        Self::new(Default::default(), nodes_factory.default_node_processor())
    }

    pub fn make_visitor(
        &self,
        query_tools: Rc<QueryTools>,
        source_schema: Rc<Schema>,
    ) -> SqlEvaluatorVisitor {
        SqlEvaluatorVisitor::new(
            query_tools,
            self.cube_alias_prefix.clone(),
            self.node_processor.clone(),
            source_schema,
        )
    }

    pub fn cube_alias_prefix(&self) -> &Option<String> {
        &self.cube_alias_prefix
    }

    pub fn source_schema(&self) -> &Rc<Schema> {
        &self.source_schema
    }

    pub fn set_source_schema(&mut self, schema: Rc<Schema>) {
        self.source_schema = schema
    }
}

pub fn evaluate_with_context(
    node: &Rc<EvaluationNode>,
    query_tools: Rc<QueryTools>,
    context: Rc<VisitorContext>,
    source_schema: Rc<Schema>,
) -> Result<String, CubeError> {
    let mut visitor = context.make_visitor(query_tools, source_schema);
    visitor.apply(node)
}
