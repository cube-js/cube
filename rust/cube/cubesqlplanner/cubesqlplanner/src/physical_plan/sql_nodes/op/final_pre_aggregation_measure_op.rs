use crate::physical_plan::sql_nodes::{RenderReferences, RenderReferencesType};
use crate::planner::symbols::AggregateWrap;
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;
use std::rc::Rc;

use super::{OpCtx, OpExec};

/// Replaces a measure's aggregation with the equivalent rollup over a
/// pre-aggregation column when one is available — `sum(state)` for
/// pre-aggregated counts, `merge(state)` for HLL, etc. Falls through when
/// the measure is not covered by the active pre-aggregation.
#[derive(Clone, Debug)]
pub struct FinalPreAggregationMeasureOp {
    references: Rc<RenderReferences>,
}

impl FinalPreAggregationMeasureOp {
    pub fn new(references: RenderReferences) -> Self {
        Self {
            references: Rc::new(references),
        }
    }
}

impl OpExec for FinalPreAggregationMeasureOp {
    fn exec(&self, ctx: &mut OpCtx<'_>) -> Result<String, CubeError> {
        let MemberSymbol::Measure(ev) = ctx.sym.as_ref() else {
            return Err(CubeError::internal(
                "FinalPreAggregationMeasure op called for non-measure symbol".to_string(),
            ));
        };
        let Some(reference) = self.references.get(&ctx.sym.full_name()) else {
            return ctx.render_tail();
        };
        match reference {
            RenderReferencesType::QualifiedColumnName(column_name) => {
                let table_ref = if let Some(table_name) = column_name.source() {
                    format!("{}.", ctx.templates.quote_identifier(table_name)?)
                } else {
                    String::new()
                };
                let pre_aggregation_measure = format!(
                    "{}{}",
                    table_ref,
                    ctx.templates.quote_identifier(&column_name.name())?
                );
                match ev.kind().pre_aggregate_wrap() {
                    AggregateWrap::CountDistinctApprox => {
                        ctx.templates.count_distinct_approx(pre_aggregation_measure)
                    }
                    AggregateWrap::Function(name) => {
                        Ok(format!("{}({})", name, pre_aggregation_measure))
                    }
                    _ => Ok(format!("sum({})", pre_aggregation_measure)),
                }
            }
            RenderReferencesType::LiteralValue(value) => ctx.templates.quote_string(value),
            RenderReferencesType::RawReferenceValue(value) => Ok(value.clone()),
        }
    }
}
