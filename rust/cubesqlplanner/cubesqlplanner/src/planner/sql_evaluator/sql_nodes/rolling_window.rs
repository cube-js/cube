use super::SqlNode;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::{MemberSymbol, SqlEvaluatorVisitor};
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

pub struct RollingWindowNode {
    input: Rc<dyn SqlNode>,
    default_processor: Rc<dyn SqlNode>,
}

impl RollingWindowNode {
    pub fn new(input: Rc<dyn SqlNode>, default_processor: Rc<dyn SqlNode>) -> Rc<Self> {
        Rc::new(Self {
            input,
            default_processor,
        })
    }

    pub fn input(&self) -> &Rc<dyn SqlNode> {
        &self.input
    }
}

impl SqlNode for RollingWindowNode {
    fn to_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node: &Rc<MemberSymbol>,
        query_tools: Rc<QueryTools>,
        node_processor: Rc<dyn SqlNode>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        let res = match node.as_ref() {
            MemberSymbol::Measure(m) => {
                if m.is_cumulative() {
                    let input = self.input.to_sql(
                        visitor,
                        node,
                        query_tools.clone(),
                        node_processor.clone(),
                        templates,
                    )?;
                    if m.measure_type() == "countDistinctApprox" {
                        query_tools.base_tools().hll_cardinality_merge(input)?
                    } else {
                        if m.measure_type() == "sum"
                            || m.measure_type() == "count"
                            || m.measure_type() == "runningTotal"
                        {
                            format!("sum({})", input)
                        } else if m.measure_type() == "min" || m.measure_type() == "max" {
                            format!("{}({})", m.measure_type(), input)
                        } else {
                            self.default_processor.to_sql(
                                visitor,
                                node,
                                query_tools.clone(),
                                node_processor,
                                templates,
                            )?
                        }
                    }
                } else {
                    self.default_processor.to_sql(
                        visitor,
                        node,
                        query_tools.clone(),
                        node_processor,
                        templates,
                    )?
                }
            }
            _ => {
                return Err(CubeError::internal(format!(
                    "Unexpected evaluation node type for RollingWindowNode"
                )));
            }
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
