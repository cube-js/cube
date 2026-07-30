use super::SqlNode;
use crate::physical_plan::SqlEvaluatorVisitor;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::symbols::MeasureKind;
use crate::planner::{MeasureRenderModifier, MemberSymbol};
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

/// Renders a `Rank` measure carrying the `MultiStageRank` render
/// modifier as a SQL window function partitioned by the modifier's
/// members. Everything else goes through `else_processor`.
pub struct MultiStageRankNode {
    else_processor: Rc<dyn SqlNode>,
}

impl MultiStageRankNode {
    pub fn new(else_processor: Rc<dyn SqlNode>) -> Rc<Self> {
        Rc::new(Self { else_processor })
    }

    pub fn else_processor(&self) -> &Rc<dyn SqlNode> {
        &self.else_processor
    }
}

impl SqlNode for MultiStageRankNode {
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
                if let (Some(MeasureRenderModifier::MultiStageRank { partition }), true) = (
                    m.render_modifier(),
                    m.is_multi_stage() && matches!(m.kind(), MeasureKind::Rank),
                ) {
                    let inner_visitor = visitor.with_arg_needs_paren_safe(false);
                    let order_by = if !m.measure_order_by().is_empty() {
                        let sql = m
                            .measure_order_by()
                            .iter()
                            .map(|item| -> Result<String, CubeError> {
                                let sql = item.sql_call().eval(
                                    &inner_visitor,
                                    node_processor.clone(),
                                    query_tools.clone(),
                                    templates,
                                )?;
                                Ok(format!("{} {}", sql, item.direction()))
                            })
                            .collect::<Result<Vec<_>, _>>()?
                            .join(", ");
                        format!("ORDER BY {sql}")
                    } else {
                        "".to_string()
                    };
                    let partition_by = if partition.is_empty() {
                        "".to_string()
                    } else {
                        let columns = partition
                            .iter()
                            .map(|dim| inner_visitor.apply(dim, node_processor.clone(), templates))
                            .collect::<Result<Vec<_>, _>>()?
                            .join(", ");
                        format!("PARTITION BY {} ", columns)
                    };
                    format!("rank() OVER ({partition_by}{order_by})")
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
                    "Unexpected evaluation node type for MultStageRankNode"
                )));
            }
        };
        Ok(res)
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self.clone()
    }

    fn childs(&self) -> Vec<Rc<dyn SqlNode>> {
        vec![self.else_processor.clone()]
    }
}
