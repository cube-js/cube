use convert_case::{Case, Casing};
use neon::prelude::*;
use pyo3::{PyAny, PyResult};

use crate::python::cross::{CLRepr, CLReprObject};

pub struct CubeConfigPy {
    properties: CLReprObject,
}

impl CubeConfigPy {
    pub fn new() -> Self {
        Self {
            properties: CLReprObject::new(),
        }
    }

    pub fn get_static_attrs(&self) -> Vec<&'static str> {
        vec![
            "schema_path",
            "base_path",
            "web_sockets_base_path",
            "compiler_cache_size",
            "telemetry",
            "pg_sql_port",
            "cache_and_queue_driver",
            "allow_js_duplicate_props_in_schema",
            "process_subscriptions_interval",
            "http",
            "jwt",
        ]
    }

    pub fn apply_dynamic_functions(&mut self, config_module: &PyAny) -> PyResult<()> {
        self.attr(config_module, "logger")?;
        self.attr(config_module, "context_to_app_id")?;
        self.attr(config_module, "context_to_orchestrator_id")?;
        self.attr(config_module, "driver_factory")?;
        self.attr(config_module, "db_type")?;
        self.attr(config_module, "check_auth")?;
        self.attr(config_module, "check_sql_auth")?;
        self.attr(config_module, "can_switch_sql_user")?;
        self.attr(config_module, "query_rewrite")?;
        self.attr(config_module, "extend_context")?;
        self.attr(config_module, "scheduled_refresh_contexts")?;
        self.attr(config_module, "context_to_api_scopes")?;
        self.attr(config_module, "repository_factory")?;
        self.attr(config_module, "semantic_layer_sync")?;
        self.attr(config_module, "schema_version")?;
        self.attr(config_module, "pre_aggregations_schema")?;
        self.attr(config_module, "orchestrator_options")?;

        Ok(())
    }

    pub fn attr(&mut self, config_module: &PyAny, key: &str) -> PyResult<()> {
        let v = config_module.getattr(&*key)?;
        if !v.is_none() {
            let value = CLRepr::from_python_ref(v)?;
            self.properties.insert(key.to_case(Case::Camel), value);
        };

        Ok(())
    }
}

impl Finalize for CubeConfigPy {}

impl CubeConfigPy {
    #[allow(clippy::wrong_self_convention)]
    pub fn to_object<'a, C: Context<'a>>(self, cx: &mut C) -> JsResult<'a, JsValue> {
        let obj = CLRepr::Object(self.properties);
        obj.into_js(cx)
    }
}
