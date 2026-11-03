use super::SqlNode;
use crate::physical_plan::SqlEvaluatorVisitor;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

/// Wraps a boolean dimension with the dialect's boolean form, for dialects whose
/// predicates evaluate to an integer (e.g. ClickHouse `UInt8`).
pub struct BooleanDimensionSqlNode {
    input: Rc<dyn SqlNode>,
}

impl BooleanDimensionSqlNode {
    pub fn new(input: Rc<dyn SqlNode>) -> Rc<Self> {
        Rc::new(Self { input })
    }

    pub fn input(&self) -> &Rc<dyn SqlNode> {
        &self.input
    }
}

impl SqlNode for BooleanDimensionSqlNode {
    fn to_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node: &Rc<MemberSymbol>,
        query_tools: Rc<QueryTools>,
        node_processor: Rc<dyn SqlNode>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        let input = self
            .input
            .to_sql(visitor, node, query_tools, node_processor, templates)?;
        match node.as_ref() {
            MemberSymbol::Dimension(d) if d.dimension_type() == "boolean" => {
                templates.wrap_boolean_dimension(input)
            }
            _ => Ok(input),
        }
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self.clone()
    }

    fn childs(&self) -> Vec<Rc<dyn SqlNode>> {
        vec![self.input.clone()]
    }
}
