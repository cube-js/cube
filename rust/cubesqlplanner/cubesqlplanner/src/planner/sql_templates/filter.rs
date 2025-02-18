use crate::cube_bridge::sql_templates_render::SqlTemplatesRender;
use cubenativeutils::CubeError;
use minijinja::context;
use std::rc::Rc;

#[derive(Clone)]
pub struct FilterTemplates {
    render: Rc<dyn SqlTemplatesRender>,
}

impl FilterTemplates {
    pub fn new(render: Rc<dyn SqlTemplatesRender>) -> Self {
        Self { render }
    }

    pub fn equals(
        &self,
        column: String,
        value: String,
        is_null_check: bool,
    ) -> Result<String, CubeError> {
        self.render.render_template(
            &"filters/equals",
            context! {
                value => value,
                is_null_check => self.additional_null_check(is_null_check, &column)?,
                column => column,
            },
        )
    }

    pub fn not_equals(
        &self,
        column: String,
        value: String,
        is_null_check: bool,
    ) -> Result<String, CubeError> {
        self.render.render_template(
            &"filters/not_equals",
            context! {
                value => value,
                is_null_check => self.additional_null_check(is_null_check, &column)?,
                column => column,
            },
        )
    }

    pub fn time_range_filter(
        &self,
        column: String,
        from_timestamp: String,
        to_timestamp: String,
    ) -> Result<String, CubeError> {
        self.render.render_template(
            &"filters/time_range_filter",
            context! {
                column => column,
                from_timestamp => from_timestamp,
                to_timestamp => to_timestamp,
            },
        )
    }

    pub fn in_where(
        &self,
        column: String,
        values: Vec<String>,
        is_null_check: bool,
    ) -> Result<String, CubeError> {
        let values_concat = values.join(", ");
        self.render.render_template(
            &"filters/in",
            context! {
                is_null_check => self.additional_null_check(is_null_check, &column)?,
                values_concat => values_concat,
                column => column,
            },
        )
    }

    pub fn not_in_where(
        &self,
        column: String,
        values: Vec<String>,
        is_null_check: bool,
    ) -> Result<String, CubeError> {
        let values_concat = values.join(", ");
        self.render.render_template(
            &"filters/not_in",
            context! {
                is_null_check => self.additional_null_check(is_null_check, &column)?,
                values_concat => values_concat,
                column => column,
            },
        )
    }

    pub fn or_is_null_check(&self, column: String) -> Result<String, CubeError> {
        self.render.render_template(
            &"filters/or_is_null_check",
            context! {
                column => column,
            },
        )
    }

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

    pub fn set_where(&self, column: String) -> Result<String, CubeError> {
        self.render.render_template(
            &"filters/set_where",
            context! {
                column => column,
            },
        )
    }

    pub fn not_set_where(&self, column: String) -> Result<String, CubeError> {
        self.render.render_template(
            &"filters/not_set_where",
            context! {
                column => column,
            },
        )
    }

    pub fn gt(&self, column: String, param: String) -> Result<String, CubeError> {
        self.render.render_template(
            &"filters/gt",
            context! {
                column => column,
                param => param
            },
        )
    }

    pub fn always_true(&self) -> Result<String, CubeError> {
        Ok(self.render.get_template("filters/always_true")?.clone())
    }

    pub fn gte(&self, column: String, param: String) -> Result<String, CubeError> {
        self.render.render_template(
            &"filters/gte",
            context! {
                column => column,
                param => param
            },
        )
    }

    pub fn lt(&self, column: String, param: String) -> Result<String, CubeError> {
        self.render.render_template(
            &"filters/lt",
            context! {
                column => column,
                param => param
            },
        )
    }

    pub fn lte(&self, column: String, param: String) -> Result<String, CubeError> {
        self.render.render_template(
            &"filters/lte",
            context! {
                column => column,
                param => param
            },
        )
    }

    pub fn additional_null_check(&self, need: bool, column: &String) -> Result<String, CubeError> {
        if need {
            self.or_is_null_check(column.clone())
        } else {
            Ok(String::default())
        }
    }

    pub fn ilike(
        &self,
        column: &str,
        value: &str,
        start_wild: bool,
        end_wild: bool,
        not: bool,
    ) -> Result<String, CubeError> {
        let pattern = self.render.render_template(
            &"filters/like_pattern",
            context! {
                start_wild => start_wild,
                value => value,
                end_wild => end_wild
            },
        )?;
        self.render.render_template(
            &"expressions/ilike",
            context! {
                expr => column,
                negated => not,
                pattern => pattern
            },
        )
    }
}
