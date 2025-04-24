use crate::cross::*;
use crate::python::cube_config::CubeConfigPy;
use crate::python::neon_py::*;
use crate::python::python_model::CubePythonModel;
use crate::python::runtime::py_runtime_init;
use neon::prelude::*;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyFunction, PyList, PyString, PyTuple};

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
        let cube_code = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/python/cube/src/__init__.py"
        ));
        PyModule::from_code(py, cube_code, "__init__.py", "cube")?;

        let config_module = PyModule::from_code(py, &file_content_arg, &options_file_name, "")?;
        let settings_py = if config_module.hasattr("config")? {
            config_module.getattr("config")?
        } else {
            // backward compatibility
            config_module.getattr("settings")?
        };

        let mut cube_conf = CubeConfigPy::new();

        for attr_name in cube_conf.get_attrs() {
            cube_conf.attr(settings_py, attr_name)?;
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
        let cube_code = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/python/cube/src/__init__.py"
        ));
        PyModule::from_code(py, cube_code, "__init__.py", "cube")?;

        let model_module = PyModule::from_code(py, &model_content, &model_file_name, "")?;

        let mut collected_functions = CLReprObject::new(CLReprObjectKind::Object);
        let mut collected_variables = CLReprObject::new(CLReprObjectKind::Object);
        let mut collected_filters = CLReprObject::new(CLReprObjectKind::Object);

        if model_module.hasattr("template")? {
            let template = model_module.getattr("template")?;

            let functions = template.getattr("functions")?.downcast::<PyDict>()?;
            for (local_key, local_value) in functions.iter() {
                if local_value.is_instance_of::<PyFunction>() {
                    let fun: Py<PyFunction> = local_value.downcast::<PyFunction>()?.into();
                    collected_functions.insert(
                        local_key.to_string(),
                        CLRepr::PythonRef(PythonRef::PyExternalFunction(fun)),
                    );
                }
            }

            let variables = template.getattr("variables")?.downcast::<PyDict>()?;
            for (local_key, local_value) in variables.iter() {
                collected_variables
                    .insert(local_key.to_string(), CLRepr::from_python_ref(local_value)?);
            }

            let filters = template.getattr("filters")?.downcast::<PyDict>()?;
            for (local_key, local_value) in filters.iter() {
                let fun: Py<PyFunction> = local_value.downcast::<PyFunction>()?.into();
                collected_filters.insert(
                    local_key.to_string(),
                    CLRepr::PythonRef(PythonRef::PyExternalFunction(fun)),
                );
            }
        } else {
            let inspect_module = py.import("inspect")?;
            let args = (model_module, inspect_module.getattr("isfunction")?);
            let functions_with_names = inspect_module
                .call_method1("getmembers", args)?
                .downcast::<PyList>()?;

            for function_details in functions_with_names.iter() {
                let function_details = function_details.downcast::<PyTuple>()?;
                let fun_name = function_details.get_item(0)?.downcast::<PyString>()?;
                let fun = function_details.get_item(1)?.downcast::<PyFunction>()?;

                let has_attr = fun.hasattr("cube_context_func")?;
                if has_attr {
                    let fun: Py<PyFunction> = fun.into();
                    collected_functions.insert(
                        fun_name.to_string(),
                        CLRepr::PythonRef(PythonRef::PyExternalFunction(fun)),
                    );
                }
            }
        };

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
