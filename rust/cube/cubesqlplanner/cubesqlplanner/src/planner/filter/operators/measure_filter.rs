use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::VisitorContext;
use cubenativeutils::CubeError;
use std::rc::Rc;

#[derive(Clone, Debug)]
pub struct MeasureFilterOp;

impl MeasureFilterOp {
    pub fn new() -> Self {
        Self
    }

    pub fn to_sql(
        &self,
        member_evaluator: &Rc<MemberSymbol>,
        query_tools: &Rc<QueryTools>,
        context: &Rc<VisitorContext>,
        plan_templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        match member_evaluator.as_ref() {
            MemberSymbol::Measure(measure_symbol) => {
                if measure_symbol.measure_filters().is_empty()
                    && measure_symbol.measure_drill_filters().is_empty()
                {
                    plan_templates.always_true()
                } else {
                    let visitor = context.make_visitor(query_tools.clone());
                    let node_processor = context.node_processor();

                    let parts = measure_symbol
                        .measure_filters()
                        .iter()
                        .chain(measure_symbol.measure_drill_filters().iter())
                        .map(|filter| -> Result<String, CubeError> {
                            Ok(format!(
                                "({})",
                                filter.eval(
                                    &visitor,
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
