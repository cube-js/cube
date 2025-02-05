use super::SqlNode;
use crate::plan::QualifiedColumnName;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::sql_evaluator::SqlEvaluatorVisitor;
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::any::Any;
use std::collections::HashMap;
use std::rc::Rc;

pub struct RenderReferencesSqlNode {
    input: Rc<dyn SqlNode>,
    references: HashMap<String, QualifiedColumnName>,
}

impl RenderReferencesSqlNode {
    pub fn new(
        input: Rc<dyn SqlNode>,
        references: HashMap<String, QualifiedColumnName>,
    ) -> Rc<Self> {
        Rc::new(Self { input, references })
    }

    pub fn input(&self) -> &Rc<dyn SqlNode> {
        &self.input
    }
}

impl SqlNode for RenderReferencesSqlNode {
    fn to_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node: &Rc<MemberSymbol>,
        query_tools: Rc<QueryTools>,
        node_processor: Rc<dyn SqlNode>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        let full_name = node.full_name();
        if let Some(reference) = self.references.get(&full_name) {
            let table_ref = reference.source().as_ref().map_or_else(
                || format!(""),
                |table_name| format!("{}.", query_tools.escape_column_name(table_name)),
            );
            Ok(format!(
                "{}{}",
                table_ref,
                query_tools.escape_column_name(&reference.name())
            ))
        } else {
            self.input.to_sql(
                visitor,
                node,
                query_tools.clone(),
                node_processor.clone(),
                templates,
            )
        }
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self.clone()
    }

    fn childs(&self) -> Vec<Rc<dyn SqlNode>> {
        vec![self.input.clone()]
    }
}
