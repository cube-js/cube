use super::SqlNode;
use crate::physical_plan::sql_nodes::render_references::RenderReferences;
use crate::physical_plan::SqlEvaluatorVisitor;
use crate::planner::planners::multi_stage::TimeShiftState;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

/// Applies a per-dimension time shift to time dimensions whose
/// full name is in `shifts`, by rendering the dimension expression
/// shifted by the configured interval.
///
/// `substituted` names the dimensions rendered as a stored column instead
/// of being evaluated. Their SQL is never expanded, so the shift cannot be
/// picked up further down and has to be applied to the column itself.
pub struct TimeShiftSqlNode {
    shifts: TimeShiftState,
    substituted: RenderReferences,
    input: Rc<dyn SqlNode>,
}

impl TimeShiftSqlNode {
    pub fn new(
        shifts: TimeShiftState,
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
}

impl SqlNode for TimeShiftSqlNode {
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
                if !ev.is_reference() && ev.is_time() {
                    // The first probe is by exact name on purpose: a dimension
                    // that gets evaluated has its shift applied when the
                    // recursion reaches the owned member it wraps, and matching
                    // it here as well would add the interval twice. Only a
                    // substituted dimension, which is never expanded, resolves
                    // through the chain.
                    let shift = self
                        .shifts
                        .dimensions_shifts
                        .get(&ev.full_name())
                        .or_else(|| {
                            if self.substituted.contains_key(&ev.full_name()) {
                                self.shifts.get_for_symbol(node)
                            } else {
                                None
                            }
                        });
                    if let Some(shift) = shift {
                        let shift = shift
                            .interval
                            .as_ref()
                            .ok_or_else(|| {
                                CubeError::internal(format!(
                                    "Time shift for dimension {} has no interval",
                                    ev.full_name()
                                ))
                            })?
                            .to_sql();
                        let inner_visitor = visitor.with_arg_needs_paren_safe(false);
                        let input = self.input.to_sql(
                            &inner_visitor,
                            node,
                            query_tools.clone(),
                            node_processor.clone(),
                            templates,
                        )?;
                        let res = templates.add_timestamp_interval(input, shift)?;
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
