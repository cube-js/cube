use crate::cube_bridge::sql_templates_render::SqlTemplatesRender;
use cubenativeutils::CubeError;
use minijinja::{context, value::Value, Environment};
use std::rc::Rc;

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

    fn additional_null_check(&self, need: bool, column: &String) -> Result<String, CubeError> {
        if need {
            self.or_is_null_check(column.clone())
        } else {
            Ok(String::default())
        }
    }
}
