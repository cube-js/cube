use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use cubenativeutils::utils::MapCubeErrExt;
use cubesql::transport::{SqlGenerator, SqlTemplates};
use cubesql::CubeError;
#[cfg(debug_assertions)]
use neon::prelude::*;

#[derive(Debug)]
pub struct NodeSqlGenerator {
    channel: Arc<Channel>,
    sql_generator_obj: Option<Arc<Root<JsObject>>>,
    sql_templates: Arc<SqlTemplates>,
}

impl NodeSqlGenerator {
    pub fn new(
        cx: &mut FunctionContext,
        channel: Arc<Channel>,
        sql_generator_obj: Arc<Root<JsObject>>,
    ) -> Result<Self, CubeError> {
        let sql_templates = Arc::new(get_sql_templates(cx, sql_generator_obj.clone())?);
        Ok(NodeSqlGenerator {
            channel,
            sql_generator_obj: Some(sql_generator_obj),
            sql_templates,
        })
    }
}

fn get_sql_templates(
    cx: &mut FunctionContext,
    sql_generator: Arc<Root<JsObject>>,
) -> Result<SqlTemplates, CubeError> {
    let sql_generator = sql_generator.to_inner(cx);
    let reuse_params = sql_generator
        .get::<JsBoolean, _, _>(cx, "shouldReuseParams")
        .map_cube_err("Can't get shouldReuseParams")?
        .value(cx);
    let sql_templates = sql_generator
        .get::<JsFunction, _, _>(cx, "sqlTemplates")
        .map_cube_err("Can't get sqlTemplates")?;
    let templates = sql_templates
        .call(cx, sql_generator, Vec::new())
        .map_cube_err("Can't call sqlTemplates function")?
        .downcast_or_throw::<JsObject, _>(cx)
        .map_cube_err("Can't cast sqlTemplates to object")?;

    let template_types = templates
        .get_own_property_names(cx)
        .map_cube_err("Can't get template types")?;

    let mut templates_map = HashMap::new();

    for i in 0..template_types.len(cx) {
        let template_type = template_types
            .get::<JsString, _, _>(cx, i)
            .map_cube_err("Can't get template type")?;
        let template = templates
            .get::<JsObject, _, _>(cx, template_type)
            .map_cube_err("Can't get template")?;

        let template_names = template
            .get_own_property_names(cx)
            .map_cube_err("Can't get template names")?;

        for i in 0..template_names.len(cx) {
            let template_name = template_names
                .get::<JsString, _, _>(cx, i)
                .map_cube_err("Can't get function names")?;
            templates_map.insert(
                format!("{}/{}", template_type.value(cx), template_name.value(cx)),
                template
                    .get::<JsString, _, _>(cx, template_name)
                    .map_cube_err("Can't get function value")?
                    .value(cx),
            );
        }
    }

    SqlTemplates::new(templates_map, reuse_params)
}

// TODO impl drop for SqlGenerator
#[async_trait]
impl SqlGenerator for NodeSqlGenerator {
    fn get_sql_templates(&self) -> Arc<SqlTemplates> {
        self.sql_templates.clone()
    }

    #[allow(clippy::diverging_sub_expression)]
    async fn call_template(
        &self,
        _name: String,
        _params: HashMap<String, String>,
    ) -> Result<String, CubeError> {
        todo!()
    }
}

impl Drop for NodeSqlGenerator {
    fn drop(&mut self) {
        let channel = self.channel.clone();
        let sql_generator_obj = self.sql_generator_obj.take().unwrap();
        channel.send(move |mut cx| {
            let _ = match Arc::try_unwrap(sql_generator_obj) {
                Ok(v) => v.into_inner(&mut cx),
                Err(_) => {
                    log::error!("Unable to drop sql generator: reference is copied somewhere else. Potential memory leak");
                    return Ok(());
                },
            };
            Ok(())
        });
    }
}
