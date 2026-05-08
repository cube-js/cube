use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::symbols::AggregateWrap;
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;
use std::collections::HashSet;
use std::rc::Rc;

use super::{OpCtx, OpExec};

/// Wraps the measure expression with its aggregate (`SUM`/`AVG`/`COUNT
/// DISTINCT`/HLL state init/etc.), choosing the right form when the measure
/// is being rendered against a multiplied join branch.
#[derive(Clone, Debug)]
pub struct FinalMeasureOp {
    rendered_as_multiplied_measures: Rc<HashSet<String>>,
    count_approx_as_state: bool,
}

impl FinalMeasureOp {
    pub fn new(
        rendered_as_multiplied_measures: HashSet<String>,
        count_approx_as_state: bool,
    ) -> Self {
        Self {
            rendered_as_multiplied_measures: Rc::new(rendered_as_multiplied_measures),
            count_approx_as_state,
        }
    }

    fn apply_wrap(
        &self,
        wrap: AggregateWrap,
        input: String,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        match wrap {
            AggregateWrap::PassThrough => Ok(input),
            AggregateWrap::Function(name) => Ok(format!("{}({})", name, input)),
            AggregateWrap::CountDistinct => templates.count_distinct(&input),
            AggregateWrap::CountDistinctApprox => {
                if self.count_approx_as_state {
                    templates.hll_init(input)
                } else {
                    templates.count_distinct_approx(input)
                }
            }
        }
    }
}

impl OpExec for FinalMeasureOp {
    fn exec(&self, ctx: &mut OpCtx<'_>) -> Result<String, CubeError> {
        let MemberSymbol::Measure(ev) = ctx.sym.as_ref() else {
            return Err(CubeError::internal(
                "FinalMeasure op called for non-measure symbol".to_string(),
            ));
        };
        let is_multiplied = self
            .rendered_as_multiplied_measures
            .contains(&ev.full_name());
        let wrap = ev.kind().aggregate_wrap(is_multiplied);
        let child_visitor = match wrap {
            AggregateWrap::PassThrough => ctx.visitor.clone(),
            _ => ctx.visitor.with_arg_needs_paren_safe(false),
        };
        let input = ctx.with_visitor(child_visitor).render_tail()?;
        self.apply_wrap(wrap, input, ctx.templates)
    }
}
