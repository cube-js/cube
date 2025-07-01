use cubenativeutils::wrappers::inner_types::InnerTypes;
use cubenativeutils::wrappers::serializer::NativeDeserialize;
use cubenativeutils::wrappers::NativeObjectHandle;
use cubenativeutils::CubeError;
use minijinja::{value::Value, Environment};
use std::collections::HashMap;
use std::marker::PhantomData;

pub trait SqlTemplatesRender {
    fn contains_template(&self, template_name: &str) -> bool;
    fn render_template(&self, name: &str, ctx: Value) -> Result<String, CubeError>;
    fn get_template(&self, template_name: &str) -> Result<&String, CubeError>;
}

pub struct NativeSqlTemplatesRender<IT: InnerTypes> {
    templates: HashMap<String, String>,
    jinja: Environment<'static>,
    phantom: PhantomData<IT>,
}

impl<IT: InnerTypes> NativeSqlTemplatesRender<IT> {
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

        Ok(Self {
            templates,
            jinja,
            phantom: PhantomData::default(),
        })
    }
}

impl<IT: InnerTypes> SqlTemplatesRender for NativeSqlTemplatesRender<IT> {
    fn contains_template(&self, template_name: &str) -> bool {
        self.templates.contains_key(template_name)
    }

    fn get_template(&self, template_name: &str) -> Result<&String, CubeError> {
        self.templates
            .get(template_name)
            .ok_or_else(|| CubeError::user("{template_name} template not found".to_string()))
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

impl<IT: InnerTypes> NativeDeserialize<IT> for NativeSqlTemplatesRender<IT> {
    fn from_native(native_object: NativeObjectHandle<IT>) -> Result<Self, CubeError> {
        let raw_data = HashMap::<String, HashMap<String, String>>::from_native(native_object)?;
        let mut templates_map = HashMap::new();
        for (template_type, templates) in raw_data {
            for (template_name, template) in templates {
                templates_map.insert(format!("{}/{}", template_type, template_name), template);
            }
        }
        NativeSqlTemplatesRender::try_new(templates_map)
    }
}
