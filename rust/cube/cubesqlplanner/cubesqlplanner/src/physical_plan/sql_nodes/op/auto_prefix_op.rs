use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;
use lazy_static::lazy_static;
use regex::Regex;
use std::collections::HashMap;
use std::rc::Rc;

use super::{OpCtx, OpExec};

/// Qualifies a bare identifier with its cube alias so the column resolves
/// unambiguously when multiple cubes appear in the same query.
pub struct AutoPrefixOp {
    cube_references: Rc<HashMap<String, String>>,
}

impl AutoPrefixOp {
    pub fn new(cube_references: HashMap<String, String>) -> Self {
        Self {
            cube_references: Rc::new(cube_references),
        }
    }

    fn resolve_cube_alias(&self, name: &str) -> String {
        self.cube_references
            .get(name)
            .cloned()
            .unwrap_or_else(|| name.to_string())
    }

    fn auto_prefix_with_cube_name(
        &self,
        cube_name: &str,
        sql: &str,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        lazy_static! {
            static ref SINGLE_MEMBER_RE: Regex = Regex::new(r"^[_a-zA-Z][_a-zA-Z0-9]*$").unwrap();
        }
        if SINGLE_MEMBER_RE.is_match(sql) {
            Ok(format!(
                "{}.{}",
                templates.quote_identifier(&PlanSqlTemplates::alias_name(cube_name))?,
                sql
            ))
        } else {
            Ok(sql.to_string())
        }
    }
}

impl OpExec for AutoPrefixOp {
    fn exec(&self, ctx: &mut OpCtx<'_>) -> Result<String, CubeError> {
        let input = ctx.render_tail()?;
        let res = match ctx.sym.as_ref() {
            MemberSymbol::Dimension(ev) => {
                let cube_alias = self.resolve_cube_alias(&ev.cube_name());
                self.auto_prefix_with_cube_name(&cube_alias, &input, ctx.templates)?
            }
            MemberSymbol::Measure(ev) => {
                let cube_alias = self.resolve_cube_alias(&ev.cube_name());
                self.auto_prefix_with_cube_name(&cube_alias, &input, ctx.templates)?
            }
            _ => input,
        };
        Ok(res)
    }
}
