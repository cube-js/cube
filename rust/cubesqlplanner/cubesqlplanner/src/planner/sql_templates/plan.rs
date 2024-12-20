use super::{TemplateGroupByColumn, TemplateOrderByColumn, TemplateProjectionColumn};
use crate::cube_bridge::sql_templates_render::SqlTemplatesRender;
use crate::plan::join::JoinType;
use convert_case::{Case, Casing};
use cubenativeutils::CubeError;
use minijinja::context;
use std::rc::Rc;

#[derive(Clone)]
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

    pub fn memeber_alias_name(cube_name: &str, name: &str, suffix: &Option<String>) -> String {
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

    pub fn join(
        &self,
        source: &str,
        condition: &str,
        join_type: &JoinType,
    ) -> Result<String, CubeError> {
        let join_type = match join_type {
            JoinType::Full => self.render.get_template("join_types/full")?,
            JoinType::Inner => self.render.get_template("join_types/inner")?,
            JoinType::Left => self.render.get_template("join_types/left")?,
        };
        self.render.render_template(
            "statements/join",
            context! { source => source, condition => condition, join_type => join_type },
        )
    }

    pub fn binary_expr(&self, left: &str, op: &str, right: &str) -> Result<String, CubeError> {
        self.render.render_template(
            "expressions/binary",
            context! { left => left, op => op, right => right },
        )
    }

    pub fn join_by_dimension_conditions(
        &self,
        left_column: &String,
        right_column: &String,
        null_check: bool,
    ) -> Result<String, CubeError> {
        let null_check = if null_check {
            if self.supports_is_not_distinct_from() {
                let is_not_distinct_from_op = self
                    .render
                    .render_template("operators/is_not_distinct_from", context! {})?;

                return self.binary_expr(left_column, &is_not_distinct_from_op, right_column);
            }
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

    pub fn supports_full_join(&self) -> bool {
        self.render.contains_template("join_types/full")
    }

    pub fn supports_is_not_distinct_from(&self) -> bool {
        self.render
            .contains_template("operators/is_not_distinct_from")
    }

    pub fn param(&self, param_index: usize) -> Result<String, CubeError> {
        self.render
            .render_template("params/param", context! { param_index => param_index })
    }

    pub fn scalar_function(
        &self,
        scalar_function: String,
        args: Vec<String>,
        date_part: Option<String>,
        interval: Option<String>,
    ) -> Result<String, CubeError> {
        let function = scalar_function.to_string().to_uppercase();
        let args_concat = args.join(", ");
        self.render.render_template(
            &format!("functions/{}", function),
            context! {
                args_concat => args_concat,
                args => args,
                date_part => date_part,
                interval => interval,
            },
        )
    }
}
