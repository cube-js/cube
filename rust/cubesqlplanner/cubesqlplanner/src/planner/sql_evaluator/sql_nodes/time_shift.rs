use super::SqlNode;
use crate::planner::planners::multi_stage::TimeShiftState;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::sql_evaluator::SqlEvaluatorVisitor;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::SqlInterval;
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
        let input = self.input.to_sql(
            visitor,
            node,
            query_tools.clone(),
            node_processor.clone(),
            templates,
        )?;
        let res = match node.as_ref() {
            MemberSymbol::Dimension(ev) => {
                if !ev.is_reference() && ev.dimension_type() == "time" {
                    let mut interval = self.shifts.common_time_shift.clone().unwrap_or_default();
                    if let Some(shift) = self.shifts.dimensions_shifts.get(&ev.full_name()) {
                        interval += &shift.interval;
                    }
                    if interval == SqlInterval::default() {
                        input
                    } else {
                        let shift = interval.to_sql();
                        let res = templates.add_timestamp_interval(input, shift)?;
                        format!("({})", res)
                    }
                } else {
                    input
                }
            }
            _ => input,
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
