use super::SqlNode;
use crate::plan::QualifiedColumnName;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::sql_evaluator::SqlEvaluatorVisitor;
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::any::Any;
use std::collections::HashMap;
use std::rc::Rc;

pub struct FinalPreAggregationMeasureSqlNode {
    input: Rc<dyn SqlNode>,
    references: HashMap<String, QualifiedColumnName>,
}

impl FinalPreAggregationMeasureSqlNode {
    pub fn new(
        input: Rc<dyn SqlNode>,
        references: HashMap<String, QualifiedColumnName>,
    ) -> Rc<Self> {
        Rc::new(Self { input, references })
    }

    pub fn input(&self) -> &Rc<dyn SqlNode> {
        &self.input
    }
}

impl SqlNode for FinalPreAggregationMeasureSqlNode {
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
                if let Some(reference) = self.references.get(&node.full_name()) {
                    let table_ref = reference.source().as_ref().map_or_else(
                        || format!(""),
                        |table_name| format!("{}.", query_tools.escape_column_name(table_name)),
                    );
                    let pre_aggregation_measure = format!(
                        "{}{}",
                        table_ref,
                        query_tools.escape_column_name(&reference.name())
                    );
                    if ev.measure_type() == "count" || ev.measure_type() == "sum" {
                        format!("sum({})", pre_aggregation_measure)
                    } else if ev.measure_type() == "countDistinctApprox" {
                        query_tools
                            .base_tools()
                            .count_distinct_approx(pre_aggregation_measure)?
                    } else if ev.measure_type() == "min" || ev.measure_type() == "max" {
                        format!("{}({})", ev.measure_type(), pre_aggregation_measure)
                    } else {
                        format!("sum({})", pre_aggregation_measure)
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
            _ => {
                return Err(CubeError::internal(format!(
                    "final preaggregation measure node processor called for wrong node",
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
