use super::SqlNode;
use crate::planner::planners::multi_stage::TimeShiftState;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::sql_evaluator::SqlEvaluatorVisitor;
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

pub struct TimeShiftSqlNode {
    shifts: TimeShiftState,
    input: Rc<dyn SqlNode>,
}

impl TimeShiftSqlNode {
    pub fn new(shifts: TimeShiftState, input: Rc<dyn SqlNode>) -> Rc<Self> {
        Rc::new(Self { shifts, input })
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
                    if let Some(shift) = self.shifts.dimensions_shifts.get(&ev.full_name()) {
                        let shift = shift.interval.clone().unwrap().to_sql();
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
