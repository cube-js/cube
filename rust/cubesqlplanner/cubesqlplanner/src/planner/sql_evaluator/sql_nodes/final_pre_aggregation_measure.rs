use super::SqlNode;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::sql_nodes::RenderReferences;
use crate::planner::sql_evaluator::sql_nodes::RenderReferencesType;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::sql_evaluator::SqlEvaluatorVisitor;
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

pub struct FinalPreAggregationMeasureSqlNode {
    input: Rc<dyn SqlNode>,
    references: RenderReferences,
}

impl FinalPreAggregationMeasureSqlNode {
    pub fn new(input: Rc<dyn SqlNode>, references: RenderReferences) -> Rc<Self> {
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
                    match reference {
                        RenderReferencesType::QualifiedColumnName(column_name) => {
                            let table_ref = if let Some(table_name) = column_name.source() {
                                format!("{}.", templates.quote_identifier(table_name)?)
                            } else {
                                format!("")
                            };
                            let pre_aggregation_measure = format!(
                                "{}{}",
                                table_ref,
                                templates.quote_identifier(&column_name.name())?
                            );
                            if ev.measure_type() == "count" || ev.measure_type() == "sum" {
                                format!("sum({})", pre_aggregation_measure)
                            } else if ev.measure_type() == "countDistinctApprox" {
                                templates.count_distinct_approx(pre_aggregation_measure)?
                            } else if ev.measure_type() == "min" || ev.measure_type() == "max" {
                                format!("{}({})", ev.measure_type(), pre_aggregation_measure)
                            } else {
                                format!("sum({})", pre_aggregation_measure)
                            }
                        }
                        RenderReferencesType::LiteralValue(value) => {
                            templates.quote_string(value)?
                        }
                        RenderReferencesType::RawReferenceValue(value) => value.clone(),
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
