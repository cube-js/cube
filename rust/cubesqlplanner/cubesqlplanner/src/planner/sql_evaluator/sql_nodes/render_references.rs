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

pub enum RenderReferencesType {
    QualifiedColumnName(QualifiedColumnName),
    LiteralValue(String),
}

impl From<QualifiedColumnName> for RenderReferencesType {
    fn from(value: QualifiedColumnName) -> Self {
        Self::QualifiedColumnName(value)
    }
}

impl From<String> for RenderReferencesType {
    fn from(value: String) -> Self {
        Self::LiteralValue(value)
    }
}

#[derive(Default)]
pub struct RenderReferences {
    references: HashMap<String, RenderReferencesType>,
}

impl RenderReferences {
    pub fn insert<T: Into<RenderReferencesType>>(&mut self, name: String, value: T) {
        self.references.insert(name, value.into());
    }

    pub fn get(&self, name: &str) -> Option<&RenderReferencesType> {
        self.references.get(name)
    }
}

pub struct RenderReferencesSqlNode {
    input: Rc<dyn SqlNode>,
    references: RenderReferences,
}

impl RenderReferencesSqlNode {
    pub fn new(input: Rc<dyn SqlNode>, references: RenderReferences) -> Rc<Self> {
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
            let table_ref = if let Some(table_name) = reference.source() {
                format!("{}.", templates.quote_identifier(table_name)?)
            } else {
                format!("")
            };
            Ok(format!(
                "{}{}",
                table_ref,
                templates.quote_identifier(&reference.name())?
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
