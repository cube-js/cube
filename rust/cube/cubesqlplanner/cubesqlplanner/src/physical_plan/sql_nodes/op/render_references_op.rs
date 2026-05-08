use crate::physical_plan::sql_nodes::{RenderReferences, RenderReferencesType};
use cubenativeutils::CubeError;
use std::rc::Rc;

use super::{OpCtx, OpExec};

/// Reuses already-materialized columns (CTE outputs, pre-aggregation
/// fields) for known members instead of recomputing them from raw cube data.
#[derive(Clone)]
pub struct RenderReferencesOp {
    references: Rc<RenderReferences>,
}

impl RenderReferencesOp {
    pub fn new(references: RenderReferences) -> Self {
        Self {
            references: Rc::new(references),
        }
    }
}

impl OpExec for RenderReferencesOp {
    fn exec(&self, ctx: &mut OpCtx<'_>) -> Result<String, CubeError> {
        let full_name = ctx.sym.full_name();
        match self.references.get(&full_name) {
            Some(RenderReferencesType::QualifiedColumnName(column_name)) => {
                let table_ref = if let Some(table_name) = column_name.source() {
                    format!("{}.", ctx.templates.quote_identifier(table_name)?)
                } else {
                    String::new()
                };
                Ok(format!(
                    "{}{}",
                    table_ref,
                    ctx.templates.quote_identifier(&column_name.name())?
                ))
            }
            Some(RenderReferencesType::LiteralValue(value)) => ctx.templates.quote_string(value),
            Some(RenderReferencesType::RawReferenceValue(value)) => Ok(value.clone()),
            None => ctx.render_tail(),
        }
    }
}
