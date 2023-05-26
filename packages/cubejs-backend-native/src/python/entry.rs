use crate::python::cube_config::CubeConfigPy;
use crate::runtime;
use neon::prelude::*;
use once_cell::sync::OnceCell;
use pyo3::prelude::*;

fn py_runtime<'a, C: Context<'a>>(cx: &mut C) -> NeonResult<&()> {
    static PY_RUNTIME: OnceCell<()> = OnceCell::new();

    let runtime = runtime(cx)?;

    PY_RUNTIME.get_or_try_init(|| {
        pyo3::prepare_freethreaded_python();
        pyo3_asyncio::tokio::init_with_runtime(runtime).unwrap();

        Ok(())
    })
}

fn python_load_config(mut cx: FunctionContext) -> JsResult<JsPromise> {
    let config_file_content = cx.argument::<JsString>(0)?.value(&mut cx);

    let (deferred, promise) = cx.promise();
    let channel = cx.channel();

    py_runtime(&mut cx)?;

    let conf_res = Python::with_gil(|py| -> PyResult<CubeConfigPy> {
        let cube_conf_code = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/python/cube/src/conf/__init__.py"
        ));
        PyModule::from_code(py, cube_conf_code, "__init__.py", "cube.conf")?;

        let config_module = PyModule::from_code(py, &config_file_content, "config.py", "")?;
        let settings_py = config_module.getattr("settings")?;

        let mut cube_conf = CubeConfigPy::new();

        for attr_name in cube_conf.get_static_attrs() {
            cube_conf.static_from_attr(settings_py, attr_name)?;
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

pub fn python_register_module(mut cx: ModuleContext) -> NeonResult<()> {
    cx.export_function("pythonLoadConfig", python_load_config)?;

    Ok(())
}
