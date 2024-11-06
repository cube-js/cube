use crate::cube_bridge::sql_templates_render::SqlTemplatesRender;
use convert_case::{Case, Casing};
use cubenativeutils::CubeError;
use minijinja::context;
use std::rc::Rc;

pub struct PlanSqlTemplates {
    render: Rc<dyn SqlTemplatesRender>,
}

impl PlanSqlTemplates {
    pub fn new(render: Rc<dyn SqlTemplatesRender>) -> Self {
        Self { render }
    }

    pub fn alias_name(name: &str) -> String {
        name.to_case(Case::Snake).replace(".", "__")
    }

    pub fn memeber_alias_name(cube_name: &str, name: &str, suffix: Option<String>) -> String {
        let suffix = if let Some(suffix) = suffix {
            format!("_{}", suffix)
        } else {
            format!("")
        };
        format!(
            "{}__{}{}",
            Self::alias_name(cube_name),
            Self::alias_name(name),
            suffix
        )
    }

    pub fn quote_identifier(&self, column_name: &str) -> Result<String, CubeError> {
        let quote = self.render.get_template("quotes/identifiers")?;
        let escape = self.render.get_template("quotes/escape")?;
        Ok(format!(
            "{}{}{}",
            quote,
            column_name.replace(quote, escape),
            quote
        ))
    }

    pub fn alias_expr(&self, expr: &str, alias: &str) -> Result<String, CubeError> {
        let quoted_alias = self.quote_identifier(alias)?;
        self.render.render_template(
            "expressions/column_aliased",
            context! { expr => expr, quoted_alias => quoted_alias },
        )
    }

    pub fn column_reference(
        &self,
        table_name: &Option<String>,
        name: &str,
    ) -> Result<String, CubeError> {
        let table_name = if let Some(table_name) = table_name {
            Some(self.quote_identifier(table_name)?)
        } else {
            None
        };
        let name = self.quote_identifier(name)?;
        self.render.render_template(
            "expressions/column_reference",
            context! { table_name => table_name, name => name },
        )
    }

    pub fn is_null_expr(&self, expr: &str, negate: bool) -> Result<String, CubeError> {
        self.render.render_template(
            "expressions/is_null",
            context! { expr => expr, negate => negate },
        )
    }
}
