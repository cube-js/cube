use super::SqlNode;
use crate::physical_plan::sql_nodes::render_references::RenderReferences;
use crate::physical_plan::SqlEvaluatorVisitor;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::symbols::CalendarDimensionTimeShift;
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;
use std::any::Any;
use std::collections::HashMap;
use std::rc::Rc;

/// Applies a calendar-cube time shift: when the dimension's
/// resolved time-shift primary key matches an entry in `shifts`,
/// renders the shifted reference (interval / named slot / custom
/// SQL) declared on the calendar cube.
///
/// `substituted` names the members rendered as a stored column. The shift is
/// declared against the calendar cube's own table, so a substituted member can
/// only be rendered when the columns it reads are stored too.
pub struct CalendarTimeShiftSqlNode {
    shifts: HashMap<String, CalendarDimensionTimeShift>, // Key is the full pk name of the calendar cube
    substituted: RenderReferences,
    input: Rc<dyn SqlNode>,
}

impl CalendarTimeShiftSqlNode {
    pub fn new(
        shifts: HashMap<String, CalendarDimensionTimeShift>,
        substituted: RenderReferences,
        input: Rc<dyn SqlNode>,
    ) -> Rc<Self> {
        Rc::new(Self {
            shifts,
            substituted,
            input,
        })
    }

    pub fn input(&self) -> &Rc<dyn SqlNode> {
        &self.input
    }

    fn renders_from_stored_columns(&self, shift: &CalendarDimensionTimeShift) -> bool {
        shift.renders_from_stored(|name| self.substituted.contains_key(name))
    }
}

impl SqlNode for CalendarTimeShiftSqlNode {
    fn to_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node: &Rc<MemberSymbol>,
        query_tools: Rc<QueryTools>,
        node_processor: Rc<dyn SqlNode>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        let res = match node.as_ref() {
            MemberSymbol::Dimension(ev) => {
                if !ev.is_reference() {
                    if let Some(shift) = self.shifts.get(&ev.full_name()) {
                        if !self.substituted.is_empty() && !self.renders_from_stored_columns(shift)
                        {
                            return Err(CubeError::internal(format!(
                                "Calendar time shift for {} cannot be rendered: the shift reads columns of the calendar cube, which this query neither stores nor selects from",
                                ev.full_name()
                            )));
                        }
                        if let Some(sql) = &shift.sql {
                            sql.eval(
                                visitor,
                                node_processor.clone(),
                                query_tools.clone(),
                                templates,
                            )?
                        } else if let Some(interval) = &shift.interval {
                            let inner_visitor = visitor.with_arg_needs_paren_safe(false);
                            let input = self.input.to_sql(
                                &inner_visitor,
                                node,
                                query_tools.clone(),
                                node_processor.clone(),
                                templates,
                            )?;
                            let res = templates
                                .add_timestamp_interval(input, interval.inverse().to_sql())?;
                            format!("({})", res)
                        } else {
                            self.input.to_sql(
                                visitor,
                                node,
                                query_tools.clone(),
                                node_processor.clone(),
                                templates,
                            )?
                        }
                    } else {
                        self.input.to_sql(
                            visitor,
                            node,
                            query_tools.clone(),
                            node_processor.clone(),
                            templates,
                        )?
                    }
                } else {
                    self.input.to_sql(
                        visitor,
                        node,
                        query_tools.clone(),
                        node_processor.clone(),
                        templates,
                    )?
                }
            }
            _ => self.input.to_sql(
                visitor,
                node,
                query_tools.clone(),
                node_processor.clone(),
                templates,
            )?,
        };
        Ok(res)
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self.clone()
    }

    fn childs(&self) -> Vec<Rc<dyn SqlNode>> {
        vec![self.input.clone()]
    }
}
