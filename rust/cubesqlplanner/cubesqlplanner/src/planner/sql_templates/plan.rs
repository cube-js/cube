use super::{TemplateGroupByColumn, TemplateOrderByColumn, TemplateProjectionColumn};
use crate::cube_bridge::sql_templates_render::SqlTemplatesRender;
use crate::plan::join::JoinType;
use convert_case::{Boundary, Case, Casing};
use cubenativeutils::CubeError;
use minijinja::context;
use std::rc::Rc;

#[derive(Clone)]
pub struct PlanSqlTemplates {
    render: Rc<dyn SqlTemplatesRender>,
}
pub const UNDERSCORE_UPPER_BOUND: Boundary = Boundary {
    name: "LowerUpper",
    condition: |s, _| {
        s.get(0) == Some(&"_")
            && s.get(1)
                .map(|c| c.to_uppercase() != c.to_lowercase() && *c == c.to_uppercase())
                == Some(true)
    },
    arg: None,
    start: 0,
    len: 0,
};

impl PlanSqlTemplates {
    pub fn new(render: Rc<dyn SqlTemplatesRender>) -> Self {
        Self { render }
    }

    pub fn alias_name(name: &str) -> String {
        let res = name
            .with_boundaries(&[
                UNDERSCORE_UPPER_BOUND,
                Boundary::LOWER_UPPER,
                Boundary::DIGIT_UPPER,
                Boundary::ACRONYM,
            ])
            .to_case(Case::Snake)
            .replace(".", "__");
        res
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

    //FIXME duplicated with filter templates
    pub fn add_interval(&self, date: String, interval: String) -> Result<String, CubeError> {
        self.render.render_template(
            &"expressions/add_interval",
            context! {
                date => date,
                interval => interval
            },
        )
    }

    pub fn sub_interval(&self, date: String, interval: String) -> Result<String, CubeError> {
        self.render.render_template(
            &"expressions/sub_interval",
            context! {
                date => date,
                interval => interval
            },
        )
    }

    pub fn quote_string(&self, string: &str) -> Result<String, CubeError> {
        Ok(format!("'{}'", string))
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

    pub fn cast(&self, expr: &str, data_type: &str) -> Result<String, CubeError> {
        self.render.render_template(
            "expressions/cast",
            context! {
                expr => expr,
                data_type => data_type,
            },
        )
    }

    pub fn cast_to_string(&self, expr: &str) -> Result<String, CubeError> {
        self.render.render_template(
            "expressions/cast_to_string",
            context! {
                expr => expr,
            },
        )
    }

    pub fn count_distinct(&self, expr: &str) -> Result<String, CubeError> {
        self.render.render_template(
            "functions/COUNT_DISTINCT",
            context! {
                args_concat => expr,
            },
        )
    }

    pub fn max(&self, expr: &str) -> Result<String, CubeError> {
        self.render
            .render_template("functions/MAX", context! { args_concat => expr })
    }

    pub fn min(&self, expr: &str) -> Result<String, CubeError> {
        self.render
            .render_template("functions/MIN", context! { args_concat => expr })
    }

    pub fn concat_strings(&self, strings: &Vec<String>) -> Result<String, CubeError> {
        self.render.render_template(
            "expressions/concat_strings",
            context! {
                strings => strings,
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
        from_date: String,
        to_date: String,
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

    pub fn time_series_get_range(
        &self,
        max_expr: &str,
        min_expr: &str,
        max_name: &str,
        min_name: &str,
        from: &str,
    ) -> Result<String, CubeError> {
        let quoted_min_name = self.quote_identifier(min_name)?;
        let quoted_max_name = self.quote_identifier(max_name)?;
        self.render.render_template(
            "expressions/time_series_get_range",
            context! {
                max_expr => max_expr,
                min_expr => min_expr,
                from_prepared => from,
                quoted_min_name => quoted_min_name,
                quoted_max_name => quoted_max_name
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

    pub fn supports_generated_time_series(&self) -> bool {
        self.render
            .contains_template("statements/generated_time_series_select")
    }

    pub fn generated_time_series_select(
        &self,
        start: &str,
        end: &str,
        granularity: &str,
    ) -> Result<String, CubeError> {
        self.render.render_template(
            "statements/generated_time_series_select",
            context! { start => start, end => end, granularity => granularity },
        )
    }

    pub fn param(&self, param_index: usize) -> Result<String, CubeError> {
        self.render
            .render_template("params/param", context! { param_index => param_index })
    }

    pub fn case(
        &self,
        expr: Option<String>,
        when_then: Vec<(String, String)>,
        else_expr: Option<String>,
    ) -> Result<String, CubeError> {
        self.render.render_template(
            "expressions/case",
            context! { expr => expr, when_then => when_then, else_expr => else_expr },
        )
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
