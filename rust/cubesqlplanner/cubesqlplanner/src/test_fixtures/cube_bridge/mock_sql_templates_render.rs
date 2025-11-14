use crate::cube_bridge::sql_templates_render::SqlTemplatesRender;
use cubenativeutils::CubeError;
use minijinja::{value::Value, Environment};
use std::collections::HashMap;

/// Mock implementation of SqlTemplatesRender for testing
///
/// This mock provides a simple in-memory template rendering system using minijinja.
/// It allows tests to define SQL templates and render them with context values.
///
/// # Example
///
/// ```
/// use cubesqlplanner::test_fixtures::cube_bridge::MockSqlTemplatesRender;
/// use minijinja::context;
///
/// let mut templates = std::collections::HashMap::new();
/// templates.insert("filters/equals".to_string(), "{{column}} = {{value}}".to_string());
///
/// let render = MockSqlTemplatesRender::try_new(templates).unwrap();
/// let result = render.render_template(
///     "filters/equals",
///     minijinja::context! { column => "id", value => "123" }
/// ).unwrap();
///
/// assert_eq!(result, "id = 123");
/// ```
#[derive(Clone)]
pub struct MockSqlTemplatesRender {
    templates: HashMap<String, String>,
    jinja: Environment<'static>,
}

impl MockSqlTemplatesRender {
    /// Creates a new MockSqlTemplatesRender with the given templates
    ///
    /// # Arguments
    ///
    /// * `templates` - HashMap of template name to template content
    ///
    /// # Returns
    ///
    /// Result containing the MockSqlTemplatesRender or a CubeError if template parsing fails
    pub fn try_new(templates: HashMap<String, String>) -> Result<Self, CubeError> {
        let mut jinja = Environment::new();
        for (name, template) in templates.iter() {
            jinja
                .add_template_owned(name.to_string(), template.to_string())
                .map_err(|e| {
                    CubeError::internal(format!(
                        "Error parsing template {} '{}': {}",
                        name, template, e
                    ))
                })?;
        }

        Ok(Self { templates, jinja })
    }

    /// Creates a default MockSqlTemplatesRender with common SQL templates
    ///
    /// This includes templates for common filter operations, functions, and types
    /// that are frequently used in tests. Based on BaseQuery.sqlTemplates() from
    /// packages/cubejs-schema-compiler/src/adapter/BaseQuery.js
    pub fn default_templates() -> Self {
        let mut templates = HashMap::new();

        // Functions - based on BaseQuery.js:4241-4315
        templates.insert(
            "functions/SUM".to_string(),
            "SUM({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/MIN".to_string(),
            "MIN({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/MAX".to_string(),
            "MAX({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/COUNT".to_string(),
            "COUNT({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/COUNT_DISTINCT".to_string(),
            "COUNT(DISTINCT {{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/AVG".to_string(),
            "AVG({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/STDDEV_POP".to_string(),
            "STDDEV_POP({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/STDDEV_SAMP".to_string(),
            "STDDEV_SAMP({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/VAR_POP".to_string(),
            "VAR_POP({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/VAR_SAMP".to_string(),
            "VAR_SAMP({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/COVAR_POP".to_string(),
            "COVAR_POP({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/COVAR_SAMP".to_string(),
            "COVAR_SAMP({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/GROUP_ANY".to_string(),
            "max({{ expr }})".to_string(),
        );
        templates.insert(
            "functions/COALESCE".to_string(),
            "COALESCE({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/CONCAT".to_string(),
            "CONCAT({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/FLOOR".to_string(),
            "FLOOR({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/CEIL".to_string(),
            "CEIL({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/TRUNC".to_string(),
            "TRUNC({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/LOWER".to_string(),
            "LOWER({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/UPPER".to_string(),
            "UPPER({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/LEFT".to_string(),
            "LEFT({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/RIGHT".to_string(),
            "RIGHT({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/SQRT".to_string(),
            "SQRT({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/ABS".to_string(),
            "ABS({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/ACOS".to_string(),
            "ACOS({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/ASIN".to_string(),
            "ASIN({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/ATAN".to_string(),
            "ATAN({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/COS".to_string(),
            "COS({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/EXP".to_string(),
            "EXP({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/LN".to_string(),
            "LN({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/LOG".to_string(),
            "LOG({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/DLOG10".to_string(),
            "LOG10({{ args_concat }})".to_string(),
        );
        templates.insert("functions/PI".to_string(), "PI()".to_string());
        templates.insert(
            "functions/POWER".to_string(),
            "POWER({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/SIN".to_string(),
            "SIN({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/TAN".to_string(),
            "TAN({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/REPEAT".to_string(),
            "REPEAT({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/NULLIF".to_string(),
            "NULLIF({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/ROUND".to_string(),
            "ROUND({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/STDDEV".to_string(),
            "STDDEV_SAMP({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/SUBSTR".to_string(),
            "SUBSTRING({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/CHARACTERLENGTH".to_string(),
            "CHAR_LENGTH({{ args[0] }})".to_string(),
        );
        templates.insert(
            "functions/BTRIM".to_string(),
            "BTRIM({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/LTRIM".to_string(),
            "LTRIM({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/RTRIM".to_string(),
            "RTRIM({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/ATAN2".to_string(),
            "ATAN2({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/COT".to_string(),
            "COT({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/DEGREES".to_string(),
            "DEGREES({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/RADIANS".to_string(),
            "RADIANS({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/SIGN".to_string(),
            "SIGN({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/ASCII".to_string(),
            "ASCII({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/STRPOS".to_string(),
            "POSITION({{ args[1] }} IN {{ args[0] }})".to_string(),
        );
        templates.insert(
            "functions/REPLACE".to_string(),
            "REPLACE({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/DATEDIFF".to_string(),
            "DATEDIFF({{ date_part }}, {{ args[1] }}, {{ args[2] }})".to_string(),
        );
        templates.insert(
            "functions/TO_CHAR".to_string(),
            "TO_CHAR({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/DATE".to_string(),
            "DATE({{ args_concat }})".to_string(),
        );
        templates.insert(
            "functions/PERCENTILECONT".to_string(),
            "PERCENTILE_CONT({{ args_concat }})".to_string(),
        );

        // Expressions - based on BaseQuery.js:4360-4391
        templates.insert(
            "expressions/column_reference".to_string(),
            "{% if table_name %}{{ table_name }}.{% endif %}{{ name }}".to_string(),
        );
        templates.insert(
            "expressions/column_aliased".to_string(),
            "{{expr}} {{quoted_alias}}".to_string(),
        );
        templates.insert(
            "expressions/query_aliased".to_string(),
            "{{ query }} AS {{ quoted_alias }}".to_string(),
        );
        templates.insert("expressions/case".to_string(), "CASE{% if expr %} {{ expr }}{% endif %}{% for when, then in when_then %} WHEN {{ when }} THEN {{ then }}{% endfor %}{% if else_expr %} ELSE {{ else_expr }}{% endif %} END".to_string());
        templates.insert(
            "expressions/is_null".to_string(),
            "({{ expr }} IS {% if negate %}NOT {% endif %}NULL)".to_string(),
        );
        templates.insert(
            "expressions/binary".to_string(),
            "({{ left }} {{ op }} {{ right }})".to_string(),
        );
        templates.insert("expressions/sort".to_string(), "{{ expr }} {% if asc %}ASC{% else %}DESC{% endif %} NULLS {% if nulls_first %}FIRST{% else %}LAST{% endif %}".to_string());
        templates.insert("expressions/order_by".to_string(), "{% if index %} {{ index }} {% else %} {{ expr }} {% endif %} {% if asc %}ASC{% else %}DESC{% endif %}{% if nulls_first %} NULLS FIRST{% endif %}".to_string());
        templates.insert(
            "expressions/cast".to_string(),
            "CAST({{ expr }} AS {{ data_type }})".to_string(),
        );
        templates.insert("expressions/window_function".to_string(), "{{ fun_call }} OVER ({% if partition_by_concat %}PARTITION BY {{ partition_by_concat }}{% if order_by_concat or window_frame %} {% endif %}{% endif %}{% if order_by_concat %}ORDER BY {{ order_by_concat }}{% if window_frame %} {% endif %}{% endif %}{% if window_frame %}{{ window_frame }}{% endif %})".to_string());
        templates.insert(
            "expressions/window_frame_bounds".to_string(),
            "{{ frame_type }} BETWEEN {{ frame_start }} AND {{ frame_end }}".to_string(),
        );
        templates.insert(
            "expressions/in_list".to_string(),
            "{{ expr }} {% if negated %}NOT {% endif %}IN ({{ in_exprs_concat }})".to_string(),
        );
        templates.insert(
            "expressions/subquery".to_string(),
            "({{ expr }})".to_string(),
        );
        templates.insert(
            "expressions/in_subquery".to_string(),
            "{{ expr }} {% if negated %}NOT {% endif %}IN {{ subquery_expr }}".to_string(),
        );
        templates.insert(
            "expressions/rollup".to_string(),
            "ROLLUP({{ exprs_concat }})".to_string(),
        );
        templates.insert(
            "expressions/cube".to_string(),
            "CUBE({{ exprs_concat }})".to_string(),
        );
        templates.insert(
            "expressions/negative".to_string(),
            "-({{ expr }})".to_string(),
        );
        templates.insert(
            "expressions/not".to_string(),
            "NOT ({{ expr }})".to_string(),
        );
        templates.insert(
            "expressions/add_interval".to_string(),
            "{{ date }} + interval '{{ interval }}'".to_string(),
        );
        templates.insert(
            "expressions/sub_interval".to_string(),
            "{{ date }} - interval '{{ interval }}'".to_string(),
        );
        templates.insert("expressions/true".to_string(), "TRUE".to_string());
        templates.insert("expressions/false".to_string(), "FALSE".to_string());
        templates.insert(
            "expressions/like".to_string(),
            "{{ expr }} {% if negated %}NOT {% endif %}LIKE {{ pattern }}".to_string(),
        );
        templates.insert(
            "expressions/ilike".to_string(),
            "{{ expr }} {% if negated %}NOT {% endif %}ILIKE {{ pattern }}".to_string(),
        );
        templates.insert(
            "expressions/like_escape".to_string(),
            "{{ like_expr }} ESCAPE {{ escape_char }}".to_string(),
        );
        templates.insert(
            "expressions/within_group".to_string(),
            "{{ fun_sql }} WITHIN GROUP (ORDER BY {{ within_group_concat }})".to_string(),
        );
        templates.insert(
            "expressions/concat_strings".to_string(),
            "{{ strings | join(' || ' ) }}".to_string(),
        );
        templates.insert(
            "expressions/rolling_window_expr_timestamp_cast".to_string(),
            "{{ value }}".to_string(),
        );
        templates.insert(
            "expressions/timestamp_literal".to_string(),
            "{{ value }}".to_string(),
        );
        templates.insert(
            "expressions/between".to_string(),
            "{{ expr }} {% if negated %}NOT {% endif %}BETWEEN {{ low }} AND {{ high }}"
                .to_string(),
        );

        // Tesseract - based on BaseQuery.js:4392-4397
        templates.insert(
            "tesseract/ilike".to_string(),
            "{{ expr }} {% if negated %}NOT {% endif %}ILIKE {{ pattern }}".to_string(),
        );
        templates.insert(
            "tesseract/series_bounds_cast".to_string(),
            "{{ expr }}".to_string(),
        );
        templates.insert(
            "tesseract/bool_param_cast".to_string(),
            "{{ expr }}".to_string(),
        );
        templates.insert(
            "tesseract/number_param_cast".to_string(),
            "{{ expr }}".to_string(),
        );

        // Filters - based on BaseQuery.js:4398-4414
        templates.insert(
            "filters/equals".to_string(),
            "{{ column }} = {{ value }}{{ is_null_check }}".to_string(),
        );
        templates.insert(
            "filters/not_equals".to_string(),
            "{{ column }} <> {{ value }}{{ is_null_check }}".to_string(),
        );
        templates.insert(
            "filters/or_is_null_check".to_string(),
            " OR {{ column }} IS NULL".to_string(),
        );
        templates.insert(
            "filters/set_where".to_string(),
            "{{ column }} IS NOT NULL".to_string(),
        );
        templates.insert(
            "filters/not_set_where".to_string(),
            "{{ column }} IS NULL".to_string(),
        );
        templates.insert(
            "filters/in".to_string(),
            "{{ column }} IN ({{ values_concat }}){{ is_null_check }}".to_string(),
        );
        templates.insert(
            "filters/not_in".to_string(),
            "{{ column }} NOT IN ({{ values_concat }}){{ is_null_check }}".to_string(),
        );
        templates.insert(
            "filters/time_range_filter".to_string(),
            "{{ column }} >= {{ from_timestamp }} AND {{ column }} <= {{ to_timestamp }}"
                .to_string(),
        );
        templates.insert(
            "filters/time_not_in_range_filter".to_string(),
            "{{ column }} < {{ from_timestamp }} OR {{ column }} > {{ to_timestamp }}".to_string(),
        );
        templates.insert(
            "filters/gt".to_string(),
            "{{ column }} > {{ param }}".to_string(),
        );
        templates.insert(
            "filters/gte".to_string(),
            "{{ column }} >= {{ param }}".to_string(),
        );
        templates.insert(
            "filters/lt".to_string(),
            "{{ column }} < {{ param }}".to_string(),
        );
        templates.insert(
            "filters/lte".to_string(),
            "{{ column }} <= {{ param }}".to_string(),
        );
        templates.insert(
            "filters/like_pattern".to_string(),
            "{% if start_wild %}'%' || {% endif %}{{ value }}{% if end_wild %}|| '%'{% endif %}"
                .to_string(),
        );
        templates.insert("filters/always_true".to_string(), "1 = 1".to_string());

        // Quotes - based on BaseQuery.js:4417-4420
        templates.insert("quotes/identifiers".to_string(), "\"".to_string());
        templates.insert("quotes/escape".to_string(), "\"\"".to_string());

        // Params - based on BaseQuery.js:4421-4423
        templates.insert("params/param".to_string(), "?".to_string());

        // Join types - based on BaseQuery.js:4424-4427
        templates.insert("join_types/inner".to_string(), "INNER".to_string());
        templates.insert("join_types/left".to_string(), "LEFT".to_string());

        // Window frame types - based on BaseQuery.js:4428-4431
        templates.insert("window_frame_types/rows".to_string(), "ROWS".to_string());
        templates.insert("window_frame_types/range".to_string(), "RANGE".to_string());

        // Window frame bounds - based on BaseQuery.js:4432-4436
        templates.insert(
            "window_frame_bounds/preceding".to_string(),
            "{% if n is not none %}{{ n }}{% else %}UNBOUNDED{% endif %} PRECEDING".to_string(),
        );
        templates.insert(
            "window_frame_bounds/current_row".to_string(),
            "CURRENT ROW".to_string(),
        );
        templates.insert(
            "window_frame_bounds/following".to_string(),
            "{% if n is not none %}{{ n }}{% else %}UNBOUNDED{% endif %} FOLLOWING".to_string(),
        );

        // Types - based on BaseQuery.js:4437-4452
        templates.insert("types/string".to_string(), "STRING".to_string());
        templates.insert("types/boolean".to_string(), "BOOLEAN".to_string());
        templates.insert("types/tinyint".to_string(), "TINYINT".to_string());
        templates.insert("types/smallint".to_string(), "SMALLINT".to_string());
        templates.insert("types/integer".to_string(), "INTEGER".to_string());
        templates.insert("types/bigint".to_string(), "BIGINT".to_string());
        templates.insert("types/float".to_string(), "FLOAT".to_string());
        templates.insert("types/double".to_string(), "DOUBLE".to_string());
        templates.insert(
            "types/decimal".to_string(),
            "DECIMAL({{ precision }},{{ scale }})".to_string(),
        );
        templates.insert("types/timestamp".to_string(), "TIMESTAMP".to_string());
        templates.insert("types/date".to_string(), "DATE".to_string());
        templates.insert("types/time".to_string(), "TIME".to_string());
        templates.insert("types/interval".to_string(), "INTERVAL".to_string());
        templates.insert("types/binary".to_string(), "BINARY".to_string());

        Self::try_new(templates).expect("Default templates should always parse successfully")
    }
}

impl SqlTemplatesRender for MockSqlTemplatesRender {
    fn contains_template(&self, template_name: &str) -> bool {
        self.templates.contains_key(template_name)
    }

    fn get_template(&self, template_name: &str) -> Result<&String, CubeError> {
        self.templates
            .get(template_name)
            .ok_or_else(|| CubeError::user(format!("{} template not found", template_name)))
    }

    fn render_template(&self, name: &str, ctx: Value) -> Result<String, CubeError> {
        Ok(self
            .jinja
            .get_template(name)
            .map_err(|e| CubeError::internal(format!("Error getting {} template: {}", name, e)))?
            .render(ctx)
            .map_err(|e| {
                CubeError::internal(format!("Error rendering {} template: {}", name, e))
            })?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use minijinja::context;

    #[test]
    fn test_basic_template_rendering() {
        let mut templates = HashMap::new();
        templates.insert("test".to_string(), "Hello {{name}}!".to_string());

        let render = MockSqlTemplatesRender::try_new(templates).unwrap();

        let result = render
            .render_template("test", context! { name => "World" })
            .unwrap();
        assert_eq!(result, "Hello World!");
    }

    #[test]
    fn test_contains_template() {
        let mut templates = HashMap::new();
        templates.insert("exists".to_string(), "template".to_string());

        let render = MockSqlTemplatesRender::try_new(templates).unwrap();

        assert!(render.contains_template("exists"));
        assert!(!render.contains_template("not_exists"));
    }

    #[test]
    fn test_get_template() {
        let mut templates = HashMap::new();
        templates.insert("test".to_string(), "content".to_string());

        let render = MockSqlTemplatesRender::try_new(templates).unwrap();

        assert_eq!(render.get_template("test").unwrap(), "content");
        assert!(render.get_template("not_exists").is_err());
    }

    #[test]
    fn test_invalid_template_syntax() {
        let mut templates = HashMap::new();
        templates.insert("bad".to_string(), "{{unclosed".to_string());

        let result = MockSqlTemplatesRender::try_new(templates);
        assert!(result.is_err());
    }

    #[test]
    fn test_template_with_multiple_variables() {
        let mut templates = HashMap::new();
        templates.insert("complex".to_string(), "{{column}} = {{value}}".to_string());

        let render = MockSqlTemplatesRender::try_new(templates).unwrap();

        let result = render
            .render_template("complex", context! { column => "id", value => "123" })
            .unwrap();

        assert_eq!(result, "id = 123");
    }

    #[test]
    fn test_template_with_numeric_values() {
        let mut templates = HashMap::new();
        templates.insert(
            "numeric".to_string(),
            "LIMIT {{limit}} OFFSET {{offset}}".to_string(),
        );

        let render = MockSqlTemplatesRender::try_new(templates).unwrap();

        let result = render
            .render_template("numeric", context! { limit => 10, offset => 20 })
            .unwrap();

        assert_eq!(result, "LIMIT 10 OFFSET 20");
    }

    #[test]
    fn test_default_templates_functions() {
        let render = MockSqlTemplatesRender::default_templates();

        // Test SUM
        let result = render
            .render_template("functions/SUM", context! { args_concat => "revenue" })
            .unwrap();
        assert_eq!(result, "SUM(revenue)");

        // Test COUNT DISTINCT
        let result = render
            .render_template(
                "functions/COUNT_DISTINCT",
                context! { args_concat => "user_id" },
            )
            .unwrap();
        assert_eq!(result, "COUNT(DISTINCT user_id)");

        // Test GROUP_ANY (uses expr instead of args_concat)
        let result = render
            .render_template("functions/GROUP_ANY", context! { expr => "status" })
            .unwrap();
        assert_eq!(result, "max(status)");

        // Test COALESCE
        let result = render
            .render_template("functions/COALESCE", context! { args_concat => "a, b, c" })
            .unwrap();
        assert_eq!(result, "COALESCE(a, b, c)");
    }

    #[test]
    fn test_default_templates_filters() {
        let render = MockSqlTemplatesRender::default_templates();

        // Test equals
        let result = render
            .render_template(
                "filters/equals",
                context! { column => "id", value => "123", is_null_check => "" },
            )
            .unwrap();
        assert_eq!(result, "id = 123");

        // Test not_equals with null check
        let result = render
            .render_template(
                "filters/not_equals",
                context! { column => "status", value => "'active'", is_null_check => " OR status IS NULL" },
            )
            .unwrap();
        assert_eq!(result, "status <> 'active' OR status IS NULL");

        // Test in filter
        let result = render
            .render_template(
                "filters/in",
                context! { column => "status", values_concat => "'active', 'pending'", is_null_check => "" },
            )
            .unwrap();
        assert_eq!(result, "status IN ('active', 'pending')");

        // Test time_range_filter
        let result = render
            .render_template(
                "filters/time_range_filter",
                context! {
                    column => "created_at",
                    from_timestamp => "TIMESTAMP '2024-01-01'",
                    to_timestamp => "TIMESTAMP '2024-12-31'"
                },
            )
            .unwrap();
        assert_eq!(
            result,
            "created_at >= TIMESTAMP '2024-01-01' AND created_at <= TIMESTAMP '2024-12-31'"
        );

        // Test like_pattern with wildcards
        let result = render
            .render_template(
                "filters/like_pattern",
                context! { value => "'john'", start_wild => true, end_wild => true },
            )
            .unwrap();
        assert_eq!(result, "'%' || 'john'|| '%'");
    }

    #[test]
    fn test_default_templates_expressions() {
        let render = MockSqlTemplatesRender::default_templates();

        // Test column_reference with table
        let result = render
            .render_template(
                "expressions/column_reference",
                context! { table_name => "users", name => "id" },
            )
            .unwrap();
        assert_eq!(result, "users.id");

        // Test column_reference without table
        let result = render
            .render_template("expressions/column_reference", context! { name => "id" })
            .unwrap();
        assert_eq!(result, "id");

        // Test cast
        let result = render
            .render_template(
                "expressions/cast",
                context! { expr => "value", data_type => "INTEGER" },
            )
            .unwrap();
        assert_eq!(result, "CAST(value AS INTEGER)");

        // Test binary expression
        let result = render
            .render_template(
                "expressions/binary",
                context! { left => "a", op => "+", right => "b" },
            )
            .unwrap();
        assert_eq!(result, "(a + b)");

        // Test is_null
        let result = render
            .render_template(
                "expressions/is_null",
                context! { expr => "value", negate => false },
            )
            .unwrap();
        assert_eq!(result, "(value IS NULL)");

        // Test is_null with negation
        let result = render
            .render_template(
                "expressions/is_null",
                context! { expr => "value", negate => true },
            )
            .unwrap();
        assert_eq!(result, "(value IS NOT NULL)");
    }

    #[test]
    fn test_default_templates_types() {
        let render = MockSqlTemplatesRender::default_templates();

        // Test simple types
        assert_eq!(
            render.render_template("types/string", context! {}).unwrap(),
            "STRING"
        );
        assert_eq!(
            render
                .render_template("types/integer", context! {})
                .unwrap(),
            "INTEGER"
        );
        assert_eq!(
            render
                .render_template("types/timestamp", context! {})
                .unwrap(),
            "TIMESTAMP"
        );

        // Test decimal with parameters
        let result = render
            .render_template("types/decimal", context! { precision => 10, scale => 2 })
            .unwrap();
        assert_eq!(result, "DECIMAL(10,2)");
    }

    #[test]
    fn test_default_templates_window_functions() {
        let render = MockSqlTemplatesRender::default_templates();

        // Test window_frame_bounds with specific n
        let result = render
            .render_template("window_frame_bounds/preceding", context! { n => 5 })
            .unwrap();
        assert_eq!(result, "5 PRECEDING");

        // Test window_frame_bounds with UNBOUNDED (n is None)
        let result = render
            .render_template(
                "window_frame_bounds/preceding",
                context! { n => Value::from(()) },
            )
            .unwrap();
        assert_eq!(result, "UNBOUNDED PRECEDING");

        // Test window_frame_bounds following with UNBOUNDED
        let result = render
            .render_template(
                "window_frame_bounds/following",
                context! { n => Value::from(()) },
            )
            .unwrap();
        assert_eq!(result, "UNBOUNDED FOLLOWING");

        // Test current_row
        let result = render
            .render_template("window_frame_bounds/current_row", context! {})
            .unwrap();
        assert_eq!(result, "CURRENT ROW");
    }

    #[test]
    fn test_default_templates_join_types() {
        let render = MockSqlTemplatesRender::default_templates();

        assert_eq!(
            render
                .render_template("join_types/inner", context! {})
                .unwrap(),
            "INNER"
        );
        assert_eq!(
            render
                .render_template("join_types/left", context! {})
                .unwrap(),
            "LEFT"
        );
    }

    #[test]
    fn test_default_templates_params() {
        let render = MockSqlTemplatesRender::default_templates();

        assert_eq!(
            render.render_template("params/param", context! {}).unwrap(),
            "?"
        );
    }

    #[test]
    fn test_default_templates_quotes() {
        let render = MockSqlTemplatesRender::default_templates();

        assert_eq!(
            render
                .render_template("quotes/identifiers", context! {})
                .unwrap(),
            "\""
        );
        assert_eq!(
            render
                .render_template("quotes/escape", context! {})
                .unwrap(),
            "\"\""
        );
    }
}
