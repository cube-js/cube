use super::window_partition::render_partition_by;
use super::SqlNode;
use crate::physical_plan::SqlEvaluatorVisitor;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::{MeasureRenderModifier, MemberSymbol};
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

/// Wraps a measure carrying the `MultiStageWindow` render modifier
/// as a SQL window function partitioned by the modifier's members.
/// Everything else goes through `else_processor`.
pub struct MultiStageWindowNode {
    input: Rc<dyn SqlNode>,
    else_processor: Rc<dyn SqlNode>,
}

impl MultiStageWindowNode {
    pub fn new(input: Rc<dyn SqlNode>, else_processor: Rc<dyn SqlNode>) -> Rc<Self> {
        Rc::new(Self {
            input,
            else_processor,
        })
    }

    pub fn input(&self) -> &Rc<dyn SqlNode> {
        &self.input
    }

    pub fn else_processor(&self) -> &Rc<dyn SqlNode> {
        &self.else_processor
    }
}

impl SqlNode for MultiStageWindowNode {
    fn to_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node: &Rc<MemberSymbol>,
        query_tools: Rc<QueryTools>,
        node_processor: Rc<dyn SqlNode>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        let res = match node.as_ref() {
            MemberSymbol::Measure(m) => {
                if let Some(modifier @ MeasureRenderModifier::MultiStageWindow { partition }) =
                    m.render_modifier()
                {
                    modifier.ensure_applies_to(m)?;
                    let inner_visitor = visitor.with_arg_needs_paren_safe(false);
                    let input_sql = self.input.to_sql(
                        &inner_visitor,
                        node,
                        query_tools.clone(),
                        node_processor.clone(),
                        templates,
                    )?;

                    let partition_by = render_partition_by(
                        partition,
                        &inner_visitor,
                        node_processor.clone(),
                        templates,
                    )?;
                    let measure_type = m.measure_type();
                    format!("{measure_type}({measure_type}({input_sql})) OVER ({partition_by})")
                } else {
                    self.else_processor.to_sql(
                        visitor,
                        node,
                        query_tools.clone(),
                        node_processor.clone(),
                        templates,
                    )?
                }
            }
            _ => {
                return Err(CubeError::internal(format!(
                    "Unexpected evaluation node type for MultStageWindowNode"
                )));
            }
        };
        Ok(res)
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self.clone()
    }

    fn childs(&self) -> Vec<Rc<dyn SqlNode>> {
        vec![self.input.clone(), self.else_processor.clone()]
    }
}
