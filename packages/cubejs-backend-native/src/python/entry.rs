use crate::cross::*;
use crate::python::cube_config::CubeConfigPy;
use crate::python::neon_py::*;
use crate::python::python_model::CubePythonModel;
use crate::python::runtime::py_runtime_init;
use neon::prelude::*;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyDict, PyFunction, PyList, PyString};
use std::path::Path;

fn extend_sys_path(py: Python, file_name: &String) -> PyResult<()> {
    let binding = py.import_bound("sys")?.getattr("path")?;
    let sys_path = binding.downcast::<PyList>()?;

    let config_dir = Path::new(&file_name)
        .parent()
        .unwrap_or_else(|| Path::new("."));
    let config_dir_str = config_dir.to_str().unwrap_or(".");

    sys_path.insert(0, PyString::new_bound(py, config_dir_str))?;
    Ok(())
}

fn python_load_config(mut cx: FunctionContext) -> JsResult<JsPromise> {
    let file_content_arg = cx.argument::<JsString>(0)?.value(&mut cx);
    let options_arg = cx.argument::<JsObject>(1)?;
    let options_file_name = options_arg
        .get::<JsString, _, _>(&mut cx, "fileName")?
        .value(&mut cx);

    let (deferred, promise) = cx.promise();
    let channel = cx.channel();

    py_runtime_init(&mut cx, channel.clone())?;

    let conf_res = Python::with_gil(|py| -> PyResult<CubeConfigPy> {
        extend_sys_path(py, &options_file_name)?;

        let cube_code = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/python/cube/src/__init__.py"
        ));
        PyModule::from_code_bound(py, cube_code, "__init__.py", "cube")?;

        let config_module =
            PyModule::from_code_bound(py, &file_content_arg, &options_file_name, "")?;
        if !config_module.hasattr("config")? {
            return Err(PyErr::new::<PyException, String>(
                "`cube.py` configuration file must define the 'config' attribute. Did you forget to add the `from cube import config` import?".to_string(),
            ));
        }

        let config_py = config_module.getattr("config")?;

        let mut cube_conf = CubeConfigPy::new();

        for attr_name in cube_conf.get_attrs() {
            cube_conf.attr(&config_py, attr_name)?;
        }

        Ok(cube_conf)
    });

    deferred.settle_with(&channel, move |mut cx| match conf_res {
        Ok(c) => c.to_object(&mut cx),
        Err(py_err) => cx.throw_from_python_error(py_err),
    });

    Ok(promise)
}

fn python_load_model(mut cx: FunctionContext) -> JsResult<JsPromise> {
    let model_file_name = cx.argument::<JsString>(0)?.value(&mut cx);
    let model_content = cx.argument::<JsString>(1)?.value(&mut cx);

    let (deferred, promise) = cx.promise();
    let channel = cx.channel();

    py_runtime_init(&mut cx, channel.clone())?;

    let conf_res = Python::with_gil(|py| -> PyResult<CubePythonModel> {
        extend_sys_path(py, &model_file_name)?;

        let cube_code = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/python/cube/src/__init__.py"
        ));
        PyModule::from_code_bound(py, cube_code, "__init__.py", "cube")?;

        let model_module = PyModule::from_code_bound(py, &model_content, &model_file_name, "")?;

        let mut collected_functions = CLReprObject::new(CLReprObjectKind::Object);
        let mut collected_variables = CLReprObject::new(CLReprObjectKind::Object);
        let mut collected_filters = CLReprObject::new(CLReprObjectKind::Object);

        if model_module.hasattr("template")? {
            let template = model_module.getattr("template")?;

            let functions_attr = template.getattr("functions")?;
            let functions = functions_attr.downcast::<PyDict>()?;
            for (local_key, local_value) in functions.iter() {
                if local_value.is_instance_of::<PyFunction>() {
                    let fun: Py<PyFunction> =
                        local_value.downcast::<PyFunction>()?.clone().unbind();
                    collected_functions.insert(
                        local_key.str()?.to_string(),
                        CLRepr::PythonRef(PythonRef::PyExternalFunction(fun)),
                    );
                }
            }

            let variables_attr = template.getattr("variables")?;
            let variables = variables_attr.downcast::<PyDict>()?;
            for (local_key, local_value) in variables.iter() {
                collected_variables.insert(
                    local_key.str()?.to_string(),
                    CLRepr::from_python_ref(&local_value)?,
                );
            }

            let filters_attr = template.getattr("filters")?;
            let filters = filters_attr.downcast::<PyDict>()?;
            for (local_key, local_value) in filters.iter() {
                let fun: Py<PyFunction> = local_value.downcast::<PyFunction>()?.clone().unbind();
                collected_filters.insert(
                    local_key.str()?.to_string(),
                    CLRepr::PythonRef(PythonRef::PyExternalFunction(fun)),
                );
            }
        }

        Ok(CubePythonModel::new(
            collected_functions,
            collected_variables,
            collected_filters,
        ))
    });

    deferred.settle_with(&channel, move |mut cx| match conf_res {
        Ok(c) => c.to_object(&mut cx),
        Err(py_err) => cx.throw_from_python_error(py_err),
    });

    Ok(promise)
}

pub fn python_register_module(cx: &mut ModuleContext) -> NeonResult<()> {
    #[cfg(target_os = "linux")]
    super::linux_dylib::load_python_symbols();

    cx.export_function("pythonLoadConfig", python_load_config)?;
    cx.export_function("pythonLoadModel", python_load_model)?;

    Ok(())
}
