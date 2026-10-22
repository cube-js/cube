use super::SqlNode;
use crate::physical_plan::QualifiedColumnName;
use crate::physical_plan::SqlEvaluatorVisitor;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;
use std::any::Any;
use std::collections::HashMap;
use std::rc::Rc;

/// The columns of a pre-aggregation table a select reads from, keyed by
/// the `full_name` of the member each column stores.
#[derive(Default, Clone)]
pub struct PreAggregationColumns {
    columns: HashMap<String, QualifiedColumnName>,
}

impl PreAggregationColumns {
    pub fn insert(&mut self, name: String, column: QualifiedColumnName) {
        self.columns.insert(name, column);
    }

    pub fn get(&self, name: &str) -> Option<&QualifiedColumnName> {
        self.columns.get(name)
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    pub fn contains_key(&self, name: &str) -> bool {
        self.columns.contains_key(name)
    }
}

/// Renders a member stored in the pre-aggregation as its column,
/// falling through to `input` for any other member.
pub struct PreAggregationColumnSqlNode {
    input: Rc<dyn SqlNode>,
    columns: PreAggregationColumns,
}

impl PreAggregationColumnSqlNode {
    pub fn new(input: Rc<dyn SqlNode>, columns: PreAggregationColumns) -> Rc<Self> {
        Rc::new(Self { input, columns })
    }

    pub fn input(&self) -> &Rc<dyn SqlNode> {
        &self.input
    }
}

impl SqlNode for PreAggregationColumnSqlNode {
    fn to_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node: &Rc<MemberSymbol>,
        query_tools: Rc<QueryTools>,
        node_processor: Rc<dyn SqlNode>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        if let Some(column) = self.columns.get(&node.full_name()) {
            templates.column_reference(column.source(), column.name())
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
