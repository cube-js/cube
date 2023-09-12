use convert_case::{Case, Casing};
use neon::prelude::*;
use pyo3::exceptions::PyTypeError;
use pyo3::types::PyFunction;
use pyo3::{Py, PyAny, PyErr, PyResult};

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
        ]
    }

    pub fn apply_dynamic_functions(&mut self, config_module: &PyAny) -> PyResult<()> {
        self.function_attr(config_module, "logger")?;
        self.function_attr(config_module, "context_to_app_id")?;
        self.function_attr(config_module, "context_to_orchestrator_id")?;
        self.function_attr(config_module, "driver_factory")?;
        self.function_attr(config_module, "db_type")?;
        self.function_attr(config_module, "check_auth")?;
        self.function_attr(config_module, "check_sql_auth")?;
        self.function_attr(config_module, "can_switch_sql_user")?;
        self.function_attr(config_module, "query_rewrite")?;
        self.function_attr(config_module, "extend_context")?;
        self.function_attr(config_module, "scheduled_refresh_contexts")?;
        self.function_attr(config_module, "context_to_api_scopes")?;
        self.function_attr(config_module, "repository_factory")?;
        self.function_attr(config_module, "semanticLayerSync")?;
        self.function_attr(config_module, "schemaVersion")?;

        Ok(())
    }

    pub fn function_attr<'a>(
        &mut self,
        config_module: &'a PyAny,
        key: &str,
    ) -> PyResult<Option<Py<PyFunction>>> {
        let v = config_module.getattr(&*key)?;
        if !v.is_none() {
            if v.get_type().is_subclass_of::<PyFunction>()? {
                let cb = v.downcast::<PyFunction>()?;
                let py: Py<PyFunction> = cb.into();

                let value = CLRepr::PyFunction(py);
                self.properties.insert(key.to_case(Case::Camel), value);
            } else {
                return Err(PyErr::new::<PyTypeError, _>(format!(
                    "Unsupported configuration type: {} for key: {}, must be a lambda",
                    v.get_type(),
                    key
                )));
            }
        }

        Ok(None)
    }

    pub fn static_attr(&mut self, config_module: &PyAny, key: &str) -> PyResult<()> {
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
