use super::SqlNode;
use crate::physical_plan::SqlEvaluatorVisitor;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::{MeasureRenderModifier, MemberSymbol};
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

/// Routes a measure to the chain matching its render modifier: the
/// final aggregation by default, the window-partial merge for
/// `RollingMerge`, or one of the row-level forms. Rank and window
/// measures are intercepted by their dedicated nodes higher in the
/// chain and must not reach this dispatcher.
pub struct MeasureRenderModifierSqlNode {
    aggregated: Rc<dyn SqlNode>,
    rolling_merge: Rc<dyn SqlNode>,
    raw_value: Rc<dyn SqlNode>,
    ungrouped_final: Rc<dyn SqlNode>,
}

impl MeasureRenderModifierSqlNode {
    pub fn new(
        aggregated: Rc<dyn SqlNode>,
        rolling_merge: Rc<dyn SqlNode>,
        raw_value: Rc<dyn SqlNode>,
        ungrouped_final: Rc<dyn SqlNode>,
    ) -> Rc<Self> {
        Rc::new(Self {
            aggregated,
            rolling_merge,
            raw_value,
            ungrouped_final,
        })
    }
}

impl SqlNode for MeasureRenderModifierSqlNode {
    fn to_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node: &Rc<MemberSymbol>,
        query_tools: Rc<QueryTools>,
        node_processor: Rc<dyn SqlNode>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        let chain = match node.as_ref() {
            MemberSymbol::Measure(m) => match m.render_modifier() {
                None => &self.aggregated,
                Some(modifier @ MeasureRenderModifier::RollingMerge) => {
                    modifier.ensure_applies_to(m)?;
                    &self.rolling_merge
                }
                Some(MeasureRenderModifier::RawValue) => &self.raw_value,
                Some(MeasureRenderModifier::UngroupedFinal) => &self.ungrouped_final,
                Some(MeasureRenderModifier::MultiStageRank { .. })
                | Some(MeasureRenderModifier::MultiStageWindow { .. }) => {
                    return Err(CubeError::internal(format!(
                        "Multi-stage window measure {} reached the render-modifier dispatcher instead of its dedicated node",
                        m.full_name()
                    )));
                }
            },
            _ => {
                return Err(CubeError::internal(format!(
                    "Measure render modifier node processor called for wrong node",
                )));
            }
        };
        chain.to_sql(visitor, node, query_tools, node_processor, templates)
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self.clone()
    }

    fn childs(&self) -> Vec<Rc<dyn SqlNode>> {
        vec![
            self.aggregated.clone(),
            self.rolling_merge.clone(),
            self.raw_value.clone(),
            self.ungrouped_final.clone(),
        ]
    }
}
