use crate::planner::symbols::DimensionKind;
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;

use super::{OpCtx, OpExec};

/// Emits a Geo dimension as a single `lat,lng` string column so the pair
/// can travel through downstream queries as one value.
pub struct GeoDimensionOp;

impl OpExec for GeoDimensionOp {
    fn exec(&self, ctx: &mut OpCtx<'_>) -> Result<String, CubeError> {
        match ctx.sym.as_ref() {
            MemberSymbol::Dimension(ev) => {
                if let DimensionKind::Geo(geo) = ev.kind() {
                    let inner_visitor = ctx.visitor.with_arg_needs_paren_safe(false);
                    let latitude_str = geo.latitude().eval(
                        &inner_visitor,
                        ctx.legacy_node_processor.clone(),
                        ctx.query_tools.clone(),
                        ctx.templates,
                    )?;
                    let longitude_str = geo.longitude().eval(
                        &inner_visitor,
                        ctx.legacy_node_processor.clone(),
                        ctx.query_tools.clone(),
                        ctx.templates,
                    )?;
                    ctx.templates
                        .concat_strings(&vec![latitude_str, format!("','"), longitude_str])
                } else {
                    ctx.render_tail()
                }
            }
            _ => Err(CubeError::internal(
                "GeoDimension op called for non-dimension symbol".to_string(),
            )),
        }
    }
}
