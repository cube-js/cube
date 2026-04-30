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

#[derive(Clone)]
pub struct RawReferenceValue(pub String);

#[derive(Clone)]
pub enum RenderReferencesType {
    QualifiedColumnName(QualifiedColumnName),
    LiteralValue(String),
    RawReferenceValue(String),
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

impl From<RawReferenceValue> for RenderReferencesType {
    fn from(value: RawReferenceValue) -> Self {
        Self::RawReferenceValue(value.0)
    }
}

#[derive(Default, Clone)]
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

    pub fn is_empty(&self) -> bool {
        self.references.is_empty()
    }

    pub fn contains_key(&self, name: &str) -> bool {
        self.references.contains_key(name)
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
            match reference {
                RenderReferencesType::QualifiedColumnName(column_name) => {
                    let table_ref = if let Some(table_name) = column_name.source() {
                        format!("{}.", templates.quote_identifier(table_name)?)
                    } else {
                        format!("")
                    };
                    Ok(format!(
                        "{}{}",
                        table_ref,
                        templates.quote_identifier(&column_name.name())?
                    ))
                }
                RenderReferencesType::LiteralValue(value) => templates.quote_string(value),
                RenderReferencesType::RawReferenceValue(value) => Ok(value.clone()),
            }
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
