use crate::cube_bridge::sql_templates_render::SqlTemplatesRender;
use cubenativeutils::CubeError;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

/// Planner state shared by every query against one compiled data model. The
/// JS side keeps it for as long as the model is not recompiled.
#[derive(Default)]
pub struct ModelContext {
    // Fetching the templates from JS and compiling them into a jinja
    // environment is the most expensive single step of planning a query.
    sql_templates: RefCell<HashMap<(String, bool), Rc<dyn SqlTemplatesRender>>>,
}

impl ModelContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn sql_templates(
        &self,
        data_source: &str,
        external: bool,
        load: impl FnOnce() -> Result<Rc<dyn SqlTemplatesRender>, CubeError>,
    ) -> Result<Rc<dyn SqlTemplatesRender>, CubeError> {
        let key = (data_source.to_string(), external);
        if let Some(templates) = self.sql_templates.borrow().get(&key) {
            return Ok(templates.clone());
        }
        let templates = load()?;
        self.sql_templates
            .borrow_mut()
            .insert(key, templates.clone());
        Ok(templates)
    }
}

/// A query's view of its `ModelContext`. A query is built in the dialect of
/// its data source, so its templates, and its external ones, are those of the
/// data source.
#[derive(Clone)]
pub struct QueryModelContext {
    model_context: Rc<ModelContext>,
    data_source: Option<String>,
    disable_external_pre_aggregations: bool,
}

impl QueryModelContext {
    pub fn new(
        model_context: Rc<ModelContext>,
        data_source: Option<String>,
        disable_external_pre_aggregations: bool,
    ) -> Self {
        Self {
            model_context,
            data_source,
            disable_external_pre_aggregations,
        }
    }

    pub fn sql_templates(
        &self,
        external: bool,
        load: impl FnOnce() -> Result<Rc<dyn SqlTemplatesRender>, CubeError>,
    ) -> Result<Rc<dyn SqlTemplatesRender>, CubeError> {
        // With external pre-aggregations disabled, `driverTools(true)` is the query
        // itself rather than the Cube Store one: caching what it returns as the
        // external templates would hand the source dialect to every external query.
        if external && self.disable_external_pre_aggregations {
            return load();
        }
        match &self.data_source {
            Some(data_source) => self
                .model_context
                .sql_templates(data_source, external, load),
            None => load(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::cube_bridge::MockSqlTemplatesRender;

    fn templates() -> Rc<dyn SqlTemplatesRender> {
        Rc::new(MockSqlTemplatesRender::default_templates())
    }

    #[test]
    fn external_templates_are_not_cached_with_external_pre_aggregations_disabled() {
        let model_context = Rc::new(ModelContext::new());
        let disabled =
            QueryModelContext::new(model_context.clone(), Some("default".to_string()), true);
        let enabled = QueryModelContext::new(model_context, Some("default".to_string()), false);

        let source = templates();
        disabled.sql_templates(true, || Ok(source.clone())).unwrap();

        let cubestore = templates();
        let loaded = enabled
            .sql_templates(true, || Ok(cubestore.clone()))
            .unwrap();
        assert!(Rc::ptr_eq(&loaded, &cubestore));
    }

    #[test]
    fn source_templates_are_cached_with_external_pre_aggregations_disabled() {
        let model_context = Rc::new(ModelContext::new());
        let disabled =
            QueryModelContext::new(model_context.clone(), Some("default".to_string()), true);
        let enabled = QueryModelContext::new(model_context, Some("default".to_string()), false);

        let source = templates();
        disabled
            .sql_templates(false, || Ok(source.clone()))
            .unwrap();

        let loaded = enabled
            .sql_templates(false, || panic!("source templates were not cached"))
            .unwrap();
        assert!(Rc::ptr_eq(&loaded, &source));
    }
}
