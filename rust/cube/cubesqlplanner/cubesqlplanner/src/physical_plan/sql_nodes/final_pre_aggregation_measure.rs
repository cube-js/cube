use super::SqlNode;
use crate::physical_plan::sql_nodes::RenderReferences;
use crate::physical_plan::sql_nodes::RenderReferencesType;
use crate::physical_plan::SqlEvaluatorVisitor;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::symbols::AggregateWrap;
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

/// Substitutes a measure with the matching pre-aggregation column
/// reference (rolled up via the measure's `pre_aggregate_wrap`),
/// or falls through to `input` when the measure has no
/// pre-aggregation entry.
pub struct FinalPreAggregationMeasureSqlNode {
    input: Rc<dyn SqlNode>,
    references: RenderReferences,
    count_approx_as_state: bool,
}

impl FinalPreAggregationMeasureSqlNode {
    pub fn new(
        input: Rc<dyn SqlNode>,
        references: RenderReferences,
        count_approx_as_state: bool,
    ) -> Rc<Self> {
        Rc::new(Self {
            input,
            references,
            count_approx_as_state,
        })
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
                            match ev.kind().pre_aggregate_wrap() {
                                AggregateWrap::CountDistinctApprox => {
                                    // The rollup column holds an HLL state, so it
                                    // must be merged, not recomputed. Keep the
                                    // merged state when this query itself feeds a
                                    // further aggregation; otherwise take its
                                    // cardinality.
                                    if self.count_approx_as_state {
                                        templates.hll_merge(pre_aggregation_measure)?
                                    } else {
                                        templates.hll_cardinality_merge(pre_aggregation_measure)?
                                    }
                                }
                                AggregateWrap::Function(name) => {
                                    format!("{}({})", name, pre_aggregation_measure)
                                }
                                _ => format!("sum({})", pre_aggregation_measure),
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
