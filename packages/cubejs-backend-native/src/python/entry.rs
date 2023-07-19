use crate::python::cross::{CLRepr, CLReprObject};
use crate::python::cube_config::CubeConfigPy;
use crate::python::python_model::CubePythonModel;
use crate::python::runtime::py_runtime_init;
use crate::python::template;
use neon::prelude::*;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyFunction, PyList, PyString, PyTuple};

fn python_load_config(mut cx: FunctionContext) -> JsResult<JsPromise> {
    let config_file_content = cx.argument::<JsString>(0)?.value(&mut cx);

    let (deferred, promise) = cx.promise();
    let channel = cx.channel();

    py_runtime_init(&mut cx, channel.clone())?;

    let conf_res = Python::with_gil(|py| -> PyResult<CubeConfigPy> {
        let cube_conf_code = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/python/cube/src/conf/__init__.py"
        ));
        PyModule::from_code(py, cube_conf_code, "__init__.py", "cube.conf")?;

        let config_module = PyModule::from_code(py, &config_file_content, "config.py", "")?;
        let settings_py = if config_module.hasattr("__execution_context_locals")? {
            let execution_context_locals = config_module.getattr("__execution_context_locals")?;
            execution_context_locals.get_item("settings")?
        } else {
            config_module.getattr("settings")?
        };

        let mut cube_conf = CubeConfigPy::new();

        for attr_name in cube_conf.get_static_attrs() {
            cube_conf.static_attr(settings_py, attr_name)?;
        }

        cube_conf.apply_dynamic_functions(settings_py)?;

        Ok(cube_conf)
    });

    deferred.settle_with(&channel, move |mut cx| match conf_res {
        Ok(c) => c.to_object(&mut cx),
        Err(err) => cx.throw_error(format!("Python error: {}", err)),
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
        PyModule::from_code(py, &cube_code, "__init__.py", "cube")?;

        let model_module = PyModule::from_code(py, &model_content, &model_file_name, "")?;
        let mut collected_functions = CLReprObject::new();

        if model_module.hasattr("__execution_context_locals")? {
            let execution_context_locals = model_module
                .getattr("__execution_context_locals")?
                .downcast::<PyDict>()?;

            for (local_key, local_value) in execution_context_locals.iter() {
                if local_value.is_instance_of::<PyFunction>()? {
                    let has_attr = local_value.hasattr("cube_context_func")?;
                    if has_attr {
                        let fun: Py<PyFunction> = local_value.downcast::<PyFunction>()?.into();
                        collected_functions
                            .insert(local_key.to_string(), CLRepr::PyExternalFunction(fun));
                    }
                }
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
                    collected_functions
                        .insert(fun_name.to_string(), CLRepr::PyExternalFunction(fun));
                }
            }
        };

        Ok(CubePythonModel::new(collected_functions))
    });

    deferred.settle_with(&channel, move |mut cx| match conf_res {
        Ok(c) => c.to_object(&mut cx),
        Err(err) => cx.throw_error(format!("Python error: {}", err)),
    });

    Ok(promise)
}

pub fn python_register_module(cx: &mut ModuleContext) -> NeonResult<()> {
    #[cfg(target_os = "linux")]
    super::linux_dylib::load_python_symbols();

    cx.export_function("pythonLoadConfig", python_load_config)?;
    cx.export_function("pythonLoadModel", python_load_model)?;

    template::template_register_module(cx)?;

    Ok(())
}
