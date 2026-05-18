use crate::physical_plan::sql_nodes::NodeProcessor;
use crate::physical_plan::SqlEvaluatorVisitor;
use crate::planner::filter::operators::measure_filter::MeasureFilterOp;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;
use std::rc::Rc;

impl MeasureFilterOp {
    pub fn to_sql(
        &self,
        member_evaluator: &Rc<MemberSymbol>,
        visitor: &SqlEvaluatorVisitor,
        node_processor: Rc<NodeProcessor>,
        query_tools: Rc<QueryTools>,
        plan_templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        match member_evaluator.as_ref() {
            MemberSymbol::Measure(measure_symbol) => {
                if measure_symbol.measure_filters().is_empty()
                    && measure_symbol.measure_drill_filters().is_empty()
                {
                    plan_templates.always_true()
                } else {
                    let parts = measure_symbol
                        .measure_filters()
                        .iter()
                        .chain(measure_symbol.measure_drill_filters().iter())
                        .map(|filter| -> Result<String, CubeError> {
                            Ok(format!(
                                "({})",
                                filter.eval(
                                    visitor,
                                    node_processor.clone(),
                                    query_tools.clone(),
                                    plan_templates,
                                )?
                            ))
                        })
                        .collect::<Result<Vec<_>, _>>()?;

                    Ok(parts.join(" AND "))
                }
            }
            _ => plan_templates.always_true(),
        }
    }
}
