use super::SqlNode;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::MeasureSymbol;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::sql_evaluator::SqlEvaluatorVisitor;
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::any::Any;
use std::collections::HashSet;
use std::rc::Rc;

pub struct FinalMeasureSqlNode {
    input: Rc<dyn SqlNode>,
    rendered_as_multiplied_measures: HashSet<String>,
    count_approx_as_state: bool,
}

impl FinalMeasureSqlNode {
    pub fn new(
        input: Rc<dyn SqlNode>,
        rendered_as_multiplied_measures: HashSet<String>,
        count_approx_as_state: bool,
    ) -> Rc<Self> {
        Rc::new(Self {
            input,
            rendered_as_multiplied_measures,
            count_approx_as_state,
        })
    }

    pub fn input(&self) -> &Rc<dyn SqlNode> {
        &self.input
    }

    fn is_count_distinct(&self, symbol: &MeasureSymbol) -> bool {
        symbol.measure_type() == "countDistinct"
            || (symbol.measure_type() == "count"
                && self
                    .rendered_as_multiplied_measures
                    .contains(&symbol.full_name()))
    }
}

impl SqlNode for FinalMeasureSqlNode {
    fn to_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node: &Rc<MemberSymbol>,
        query_tools: Rc<QueryTools>,
        node_processor: Rc<dyn SqlNode>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        let res = match node.as_ref() {
            MemberSymbol::Measure(ev) => {
                let input = self.input.to_sql(
                    visitor,
                    node,
                    query_tools.clone(),
                    node_processor.clone(),
                    templates,
                )?;

                if ev.is_calculated() || ev.measure_type() == "numberAgg" {
                    input
                } else if ev.measure_type() == "countDistinctApprox" {
                    if self.count_approx_as_state {
                        query_tools.base_tools().hll_init(input)?
                    } else {
                        query_tools.base_tools().count_distinct_approx(input)?
                    }
                } else if self.is_count_distinct(ev) {
                    templates.count_distinct(&input)?
                } else {
                    let measure_type = if ev.measure_type() == "runningTotal" {
                        "sum"
                    } else {
                        &ev.measure_type()
                    };

                    format!("{}({})", measure_type, input)
                }
            }
            _ => {
                return Err(CubeError::internal(format!(
                    "Measure filter node processor called for wrong node",
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
