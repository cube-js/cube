use crate::physical_plan::filter::ToSql;
use crate::planner::FiltersContext;
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;

use super::{OpCtx, OpExec};

/// Replaces a member's value with its `mask` SQL whenever the user lacks
/// access to the underlying data, optionally guarding the substitution with
/// the member's mask filter so authorized rows still see the real value.
#[derive(Clone, Debug)]
pub struct MaskedOp {
    ungrouped: bool,
}

impl MaskedOp {
    pub fn new(ungrouped: bool) -> Self {
        Self { ungrouped }
    }

    fn resolve_mask(&self, ctx: &mut OpCtx<'_>) -> Result<Option<String>, CubeError> {
        let full_name = ctx.sym.full_name();
        if !ctx.query_tools.is_member_masked(&full_name) {
            return Ok(None);
        }

        let mask_filter = ctx.query_tools.member_mask_filter(&full_name);

        let masked_sql = if let Some(mask_call) = ctx.sym.mask_sql() {
            if self.ungrouped {
                if let MemberSymbol::Measure(_) = ctx.sym.as_ref() {
                    if mask_call.dependencies_count() > 0 {
                        return Ok(None);
                    }
                }
            }
            mask_call.eval(
                &ctx.visitor,
                ctx.node_processor.clone(),
                ctx.query_tools.clone(),
                ctx.templates,
            )?
        } else {
            "(NULL)".to_string()
        };

        let Some(filter_item) = mask_filter else {
            return Ok(Some(masked_sql));
        };

        let original_sql = ctx.render_tail()?;
        // Use the unmasked tail as `node_processor` for filter rendering so
        // member references inside the filter resolve through it — prevents
        // recursing back through this MaskedOp when the filter member is
        // itself masked.
        let tail_as_node_processor = ctx.tail_as_node_processor();
        // TODO: support FILTER_PARAMS in mask filter SQL by passing a proper
        // FiltersContext with filter_params_columns.
        let filter_sql = filter_item.to_sql(
            &ctx.visitor,
            tail_as_node_processor,
            ctx.query_tools.clone(),
            ctx.templates,
            &FiltersContext::default(),
        )?;
        if filter_sql.is_empty() {
            Ok(Some(masked_sql))
        } else {
            Ok(Some(ctx.templates.case(
                None,
                vec![(filter_sql, original_sql)],
                Some(masked_sql),
            )?))
        }
    }
}

impl OpExec for MaskedOp {
    fn exec(&self, ctx: &mut OpCtx<'_>) -> Result<String, CubeError> {
        if let Some(masked) = self.resolve_mask(ctx)? {
            return Ok(masked);
        }
        ctx.render_tail()
    }
}
