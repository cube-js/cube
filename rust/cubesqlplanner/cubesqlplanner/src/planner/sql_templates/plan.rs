use super::{TemplateGroupByColumn, TemplateOrderByColumn, TemplateProjectionColumn};
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

    pub fn column_aliased(&self, expr: &str, alias: &str) -> Result<String, CubeError> {
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
    pub fn always_true(&self) -> Result<String, CubeError> {
        Ok(self.render.get_template("filters/always_true")?.clone())
    }

    pub fn query_aliased(&self, query: &str, alias: &str) -> Result<String, CubeError> {
        let quoted_alias = self.quote_identifier(alias)?;
        self.render.render_template(
            "expressions/query_aliased",
            context! { query => query, quoted_alias => quoted_alias },
        )
    }

    pub fn order_by(
        &self,
        expr: &str,
        index: Option<usize>,
        asc: bool,
    ) -> Result<String, CubeError> {
        self.render.render_template(
            "expressions/order_by",
            context! {
                expr => expr,
                index => index,
                asc => asc
            },
        )
    }

    pub fn group_by(&self, items: Vec<TemplateGroupByColumn>) -> Result<String, CubeError> {
        self.render.render_template(
            "statements/group_by_exprs",
            context! {
                group_by => items
            },
        )
    }

    pub fn cte(&self, query: &str, alias: &str) -> Result<String, CubeError> {
        self.render.render_template(
            "statements/cte",
            context! {
                query => query,
                alias => alias
            },
        )
    }

    pub fn time_series_select(
        &self,
        from_date: Option<String>,
        to_date: Option<String>,
        seria: Vec<Vec<String>>,
    ) -> Result<String, CubeError> {
        self.render.render_template(
            "statements/time_series_select",
            context! {
                from_date => from_date,
                to_date => to_date,
                seria => seria
            },
        )
    }

    pub fn select(
        &self,
        ctes: Vec<String>,
        from: &str,
        projection: Vec<TemplateProjectionColumn>,
        where_condition: Option<String>,
        group_by: Vec<TemplateGroupByColumn>,
        having: Option<String>,
        order_by: Vec<TemplateOrderByColumn>,
        limit: Option<usize>,
        offset: Option<usize>,
        distinct: bool,
    ) -> Result<String, CubeError> {
        self.render.render_template(
            "statements/select",
            context! {
                from_prepared => from,
                select_concat => projection,
                group_by => self.group_by(group_by)?,
                projection => projection,
                order_by => order_by,
                filter => where_condition,
                having => having,
                limit => limit,
                offset => offset,
                distinct => distinct,
                ctes => ctes,
            },
        )
    }

    pub fn join(&self, source: &str, condition: &str, is_inner: bool) -> Result<String, CubeError> {
        let join_type = if is_inner {
            self.render.get_template("join_types/inner")?
        } else {
            self.render.get_template("join_types/left")?
        };
        self.render.render_template(
            "statements/join",
            context! { source => source, condition => condition, join_type => join_type },
        )
    }

    pub fn join_by_dimension_conditions(
        &self,
        left_column: &String,
        right_column: &String,
        null_check: bool,
    ) -> Result<String, CubeError> {
        let null_check = if null_check {
            format!(
                " OR ({} AND {})",
                self.is_null_expr(&left_column, false)?,
                self.is_null_expr(&right_column, false)?
            )
        } else {
            format!("")
        };

        Ok(format!(
            "({} = {}{})",
            left_column, right_column, null_check
        ))
    }
}
