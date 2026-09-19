use super::SqlNode;
use crate::physical_plan::SqlEvaluatorVisitor;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

/// Renders a time dimension: applies the granularity (predefined
/// or calendar SQL) and timezone conversion, unless the symbol is
/// marked as already timezone-converted.
pub struct TimeDimensionNode {
    input: Rc<dyn SqlNode>,
}

impl TimeDimensionNode {
    pub fn new(input: Rc<dyn SqlNode>) -> Rc<Self> {
        Rc::new(Self { input })
    }
}

impl SqlNode for TimeDimensionNode {
    fn to_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node: &Rc<MemberSymbol>,
        query_tools: Rc<QueryTools>,
        node_processor: Rc<dyn SqlNode>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        match node.as_ref() {
            MemberSymbol::TimeDimension(ev) => {
                if let Some(granularity_obj) = ev.granularity_obj() {
                    // Short-circuits to calendar SQL — `self.input` is not used.
                    // Propagate the outer visitor: the calendar SQL is the
                    // expression itself, not wrapped further here.
                    if let Some(calendar_sql) = granularity_obj.calendar_sql() {
                        return calendar_sql.eval(
                            visitor,
                            node_processor.clone(),
                            query_tools.clone(),
                            templates,
                        );
                    }
                    // Wraps in `convert_tz(…)` and a granularity function —
                    // safe, reset for child render.
                    let inner_visitor = visitor.with_arg_needs_paren_safe(false);
                    let input_sql = self.input.to_sql(
                        &inner_visitor,
                        node,
                        query_tools.clone(),
                        node_processor.clone(),
                        templates,
                    )?;
                    let converted_tz = if ev.tz_converted_at_source() {
                        input_sql
                    } else {
                        templates.convert_tz(input_sql)?
                    };

                    Ok(granularity_obj.apply_to_input_sql(templates, converted_tz)?)
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
            MemberSymbol::Dimension(ev) => {
                // Only the cube owning the column reads it from the database, so
                // only there is a conversion needed. A dimension that merely
                // references another one renders through the owning symbol,
                // which converts on its own; converting here as well would
                // shift the value twice.
                let wraps_convert_tz = !visitor.ignore_tz_convert()
                    && query_tools.convert_tz_for_raw_time_dimension()
                    && ev.dimension_type() == "time"
                    && ev.owned_by_cube();
                if wraps_convert_tz {
                    let inner_visitor = visitor.with_arg_needs_paren_safe(false);
                    let input_sql = self.input.to_sql(
                        &inner_visitor,
                        node,
                        query_tools.clone(),
                        node_processor.clone(),
                        templates,
                    )?;
                    Ok(templates.convert_tz(input_sql)?)
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
            _ => self.input.to_sql(
                visitor,
                node,
                query_tools.clone(),
                node_processor.clone(),
                templates,
            ),
        }
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self.clone()
    }

    fn childs(&self) -> Vec<Rc<dyn SqlNode>> {
        vec![]
    }
}
