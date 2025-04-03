use super::SqlNode;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::SqlEvaluatorVisitor;
use crate::planner::sql_evaluator::{MeasureTimeShift, MemberSymbol};
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::any::Any;
use std::collections::HashMap;
use std::rc::Rc;

pub struct TimeShiftSqlNode {
    shifts: HashMap<String, MeasureTimeShift>,
    input: Rc<dyn SqlNode>,
}

impl TimeShiftSqlNode {
    pub fn new(shifts: HashMap<String, MeasureTimeShift>, input: Rc<dyn SqlNode>) -> Rc<Self> {
        Rc::new(Self { shifts, input })
    }

    pub fn shifts(&self) -> &HashMap<String, MeasureTimeShift> {
        &self.shifts
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
                if let Some(shift) = self.shifts.get(&ev.full_name()) {
                    let shift = shift.interval.to_sql();
                    format!("({input} + interval '{shift}')")
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
