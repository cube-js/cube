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
    // Keyed by data source and whether they are its external (Cube Store) ones.
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

/// A query's view of its `ModelContext`. A query whose data source the JS side
/// hasn't resolved (e.g. one joining across data sources) fetches its templates
/// from JS each time.
#[derive(Clone)]
pub struct QueryModelContext {
    model_context: Rc<ModelContext>,
    data_source: Option<String>,
}

impl QueryModelContext {
    pub fn new(model_context: Rc<ModelContext>, data_source: Option<String>) -> Self {
        Self {
            model_context,
            data_source,
        }
    }

    pub fn sql_templates(
        &self,
        external: bool,
        load: impl FnOnce() -> Result<Rc<dyn SqlTemplatesRender>, CubeError>,
    ) -> Result<Rc<dyn SqlTemplatesRender>, CubeError> {
        match &self.data_source {
            Some(data_source) => self
                .model_context
                .sql_templates(data_source, external, load),
            None => load(),
        }
    }
}
