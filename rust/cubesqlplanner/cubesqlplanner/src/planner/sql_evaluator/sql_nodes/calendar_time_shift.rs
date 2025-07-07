use super::SqlNode;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::symbols::CalendarDimensionTimeShift;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::sql_evaluator::SqlEvaluatorVisitor;
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::any::Any;
use std::collections::HashMap;
use std::rc::Rc;

pub struct CalendarTimeShiftSqlNode {
    shifts: HashMap<String, CalendarDimensionTimeShift>, // Key is the full pk name of the calendar cube
    input: Rc<dyn SqlNode>,
}

impl CalendarTimeShiftSqlNode {
    pub fn new(
        shifts: HashMap<String, CalendarDimensionTimeShift>,
        input: Rc<dyn SqlNode>,
    ) -> Rc<Self> {
        Rc::new(Self { shifts, input })
    }

    pub fn input(&self) -> &Rc<dyn SqlNode> {
        &self.input
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
        let input = self.input.to_sql(
            visitor,
            node,
            query_tools.clone(),
            node_processor.clone(),
            templates,
        )?;
        let res = match node.as_ref() {
            MemberSymbol::Dimension(ev) => {
                if !ev.is_reference() {
                    if let Some(shift) = self.shifts.get(&ev.full_name()) {
                        if let Some(sql) = &shift.sql {
                            sql.eval(
                                visitor,
                                node_processor.clone(),
                                query_tools.clone(),
                                templates,
                            )?
                        } else {
                            input
                        }
                    } else {
                        input
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
