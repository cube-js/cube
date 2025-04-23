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
                    let converted_tz = if self
                        .dimensions_with_ignored_timezone
                        .contains(&ev.full_name())
                    {
                        input_sql
                    } else {
                        query_tools.base_tools().convert_tz(input_sql)?
                    };

                    let res = if granularity_obj.is_natural_aligned() {
                        if let Some(granularity_offset) = granularity_obj.granularity_offset() {
                            let dt =
                                templates.sub_interval(converted_tz, granularity_offset.clone())?;
                            let dt = query_tools.base_tools().time_grouped_column(
                                granularity_obj.granularity_from_interval()?,
                                dt,
                            )?;
                            templates.add_interval(dt, granularity_offset.clone())?
                        } else {
                            query_tools.base_tools().time_grouped_column(
                                granularity_obj.granularity().clone(),
                                converted_tz,
                            )?
                        }
                    } else {
                        query_tools.base_tools().date_bin(
                            granularity_obj.granularity_interval().clone(),
                            converted_tz,
                            granularity_obj.origin_local_formatted(),
                        )?
                    };

                    res
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
