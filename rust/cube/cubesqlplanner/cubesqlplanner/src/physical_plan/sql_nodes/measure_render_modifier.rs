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
/// modifiers route to the aggregation chain — their measures are
/// intercepted by the dedicated nodes higher in the chain.
pub struct MeasureRenderModifierSqlNode {
    aggregated: Rc<dyn SqlNode>,
    rolling_merge: Rc<dyn SqlNode>,
    ungrouped: Rc<dyn SqlNode>,
    ungrouped_query: Rc<dyn SqlNode>,
}

impl MeasureRenderModifierSqlNode {
    pub fn new(
        aggregated: Rc<dyn SqlNode>,
        rolling_merge: Rc<dyn SqlNode>,
        ungrouped: Rc<dyn SqlNode>,
        ungrouped_query: Rc<dyn SqlNode>,
    ) -> Rc<Self> {
        Rc::new(Self {
            aggregated,
            rolling_merge,
            ungrouped,
            ungrouped_query,
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
                None
                | Some(MeasureRenderModifier::MultiStageRank { .. })
                | Some(MeasureRenderModifier::MultiStageWindow { .. }) => &self.aggregated,
                Some(MeasureRenderModifier::RollingMerge) => &self.rolling_merge,
                Some(MeasureRenderModifier::Ungrouped) => &self.ungrouped,
                Some(MeasureRenderModifier::UngroupedQueryValue) => &self.ungrouped_query,
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
            self.ungrouped.clone(),
            self.ungrouped_query.clone(),
        ]
    }
}
