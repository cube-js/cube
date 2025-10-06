use super::SqlNode;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::sql_evaluator::SqlEvaluatorVisitor;
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::any::Any;
use std::collections::HashSet;
use std::rc::Rc;

pub struct TimeDimensionNode {
    dimensions_with_ignored_timezone: HashSet<String>,
    input: Rc<dyn SqlNode>,
}

impl TimeDimensionNode {
    pub fn new(
        dimensions_with_ignored_timezone: HashSet<String>,
        input: Rc<dyn SqlNode>,
    ) -> Rc<Self> {
        Rc::new(Self {
            dimensions_with_ignored_timezone,
            input,
        })
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
        let input_sql = self.input.to_sql(
            visitor,
            node,
            query_tools.clone(),
            node_processor.clone(),
            templates,
        )?;
        match node.as_ref() {
            MemberSymbol::TimeDimension(ev) => {
                let res = if let Some(granularity_obj) = ev.granularity_obj() {
                    if let Some(calendar_sql) = granularity_obj.calendar_sql() {
                        return calendar_sql.eval(
                            visitor,
                            node_processor.clone(),
                            query_tools.clone(),
                            templates,
                        );
                    }

                    let converted_tz = if self
                        .dimensions_with_ignored_timezone
                        .contains(&ev.full_name())
                    {
                        input_sql
                    } else {
                        templates.convert_tz(input_sql)?
                    };

                    granularity_obj.apply_to_input_sql(templates, converted_tz)?
                } else {
                    input_sql
                };
                Ok(res)
            }
            _ => Ok(input_sql),
        }
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self.clone()
    }

    fn childs(&self) -> Vec<Rc<dyn SqlNode>> {
        vec![]
    }
}
