use crate::cross::*;
use crate::template::mj_value::*;
use crate::template::neon::NeonMiniJinjaContext;
use crate::utils::bind_method;

use log::trace;
use minijinja as mj;
use neon::prelude::*;
use std::cell::RefCell;

#[cfg(feature = "python")]
use pyo3::{exceptions::PyNotImplementedError, prelude::*, types::PyTuple, AsPyPointer};

struct JinjaEngine {
    inner: mj::Environment<'static>,
}

impl Finalize for JinjaEngine {}

impl JinjaEngine {
    fn new(cx: &mut FunctionContext) -> NeonResult<Self> {
        let options = cx.argument::<JsObject>(0)?;

        let debug_info = options
            .get_value(cx, "debugInfo")?
            .downcast_or_throw::<JsBoolean, _>(cx)?
            .value(cx);

        let mut engine = mj::Environment::new();
        engine.set_debug(debug_info);
        engine.add_function(
            "env_var",
            |var_name: String, var_default: Option<String>, _state: &minijinja::State| {
                if let Ok(value) = std::env::var(&var_name) {
                    return Ok(mj::value::Value::from(value));
                }

                if let Some(var_default) = var_default {
                    return Ok(mj::value::Value::from(var_default));
                }

                let err = minijinja::Error::new(
                    mj::ErrorKind::InvalidOperation,
                    format!("unknown env variable {}", var_name),
                );

                Err(err)
            },
        );
        engine.set_auto_escape_callback(|_name: &str| mj::AutoEscape::Json);

        #[cfg(feature = "python")]
        {
            let filters = options
                .get_value(cx, "filters")?
                .downcast_or_throw::<JsObject, _>(cx)?;

            let filter_names = filters.get_own_property_names(cx)?;
            for i in 0..filter_names.len(cx) {
                let filter_name: Handle<JsString> = filter_names.get(cx, i)?;
                let filter_fun = CLRepr::from_js_ref(filters.get_value(cx, filter_name)?, cx)?;

                let py_fun = match filter_fun {
                    CLRepr::PythonRef(py_ref) => match py_ref {
                        PythonRef::PyFunction(py_fun_ref)
                        | PythonRef::PyExternalFunction(py_fun_ref) => py_fun_ref,
                        other => {
                            return cx.throw_error(format!(
                            "minijinja::filter must be a function, actual: CLRepr::PythonRef({:?})",
                            other
                        ))
                        }
                    },
                    other => {
                        return cx.throw_error(format!(
                            "minijinja::filter must be a function, actual: {:?}",
                            other.kind()
                        ))
                    }
                };

                engine.add_filter(
                    filter_name.value(cx),
                    move |_state: &mj::State,
                          args: &[mj::value::Value]|
                          -> Result<mj::value::Value, mj::Error> {
                        let mut arguments = Vec::with_capacity(args.len());

                        for arg in args {
                            arguments.push(from_minijinja_value(arg)?);
                        }

                        let python_call_res = Python::with_gil(|py| {
                            let mut args_tuple = Vec::with_capacity(args.len());

                            for arg in arguments {
                                args_tuple.push(arg.into_py(py)?);
                            }

                            let tuple = PyTuple::new(py, args_tuple);

                            let call_res = py_fun.call1(py, tuple)?;

                            let is_coroutine =
                                unsafe { pyo3::ffi::PyCoro_CheckExact(call_res.as_ptr()) == 1 };
                            if is_coroutine {
                                Err(PyErr::new::<PyNotImplementedError, _>(
                                    "Calling async is not supported",
                                ))
                            } else {
                                CLRepr::from_python_ref(call_res.as_ref(py))
                            }
                        });
                        match python_call_res {
                            Ok(r) => Ok(to_minijinja_value(r)),
                            Err(err) => Err(mj::Error::new(
                                minijinja::ErrorKind::InvalidOperation,
                                format!("Error while calling filter: {}", err),
                            )),
                        }
                    },
                )
            }
        }

        Ok(Self { inner: engine })
    }
}

type BoxedJinjaEngine = JsBox<RefCell<JinjaEngine>>;

impl JinjaEngine {
    fn render_template(mut cx: FunctionContext) -> JsResult<JsString> {
        #[cfg(build = "debug")]
        trace!("JinjaEngine.render_template");

        let this = cx
            .this()
            .downcast_or_throw::<BoxedJinjaEngine, _>(&mut cx)?;

        let template_name = cx.argument::<JsString>(0)?;
        let template_compile_context = CLRepr::from_js_ref(cx.argument::<JsValue>(1)?, &mut cx)?;
        let template_python_context = CLRepr::from_js_ref(cx.argument::<JsValue>(2)?, &mut cx)?;

        let engine = &this.borrow().inner;
        let template = match engine.get_template(&template_name.value(&mut cx)) {
            Ok(t) => t,
            Err(err) => {
                trace!("jinja get template error: {:?}", err);

                return cx.throw_from_mj_error(err);
            }
        };

        let mut to_jinja_ctx = CLReprObject::new();
        to_jinja_ctx.insert("COMPILE_CONTEXT".to_string(), template_compile_context);

        if !template_python_context.is_null() {
            for (py_symbol_name, pysymbol_repr) in
                template_python_context.downcast_to_object().into_iter()
            {
                to_jinja_ctx.insert(py_symbol_name, pysymbol_repr);
            }
        }

        let compile_context = to_minijinja_value(CLRepr::Object(to_jinja_ctx));
        match template.render(compile_context) {
            Ok(r) => Ok(cx.string(r)),
            Err(err) => {
                trace!("jinja render template error: {:?}", err);

                cx.throw_from_mj_error(err)
            }
        }
    }

    fn load_template(mut cx: FunctionContext) -> JsResult<JsUndefined> {
        #[cfg(build = "debug")]
        trace!("JinjaEngine.load_template");

        let this = cx
            .this()
            .downcast_or_throw::<BoxedJinjaEngine, _>(&mut cx)?;

        let template_name = cx.argument::<JsString>(0)?;
        let template_content = cx.argument::<JsString>(1)?;

        if let Err(err) = this.borrow_mut().inner.add_template_owned(
            template_name.value(&mut cx),
            template_content.value(&mut cx),
        ) {
            trace!("jinja load error: {:?}", err);

            return cx.throw_from_mj_error(err);
        }

        Ok(cx.undefined())
    }

    fn js_new(mut cx: FunctionContext) -> JsResult<JsObject> {
        let engine = Self::new(&mut cx).or_else(|err| cx.throw_error(err.to_string()))?;

        let obj = cx.empty_object();
        let obj_this = cx.boxed(RefCell::new(engine)).upcast::<JsValue>();

        let render_template_fn = JsFunction::new(&mut cx, JinjaEngine::render_template)?;
        let render_template_fn = bind_method(&mut cx, render_template_fn, obj_this)?;
        obj.set(&mut cx, "renderTemplate", render_template_fn)?;

        let load_template_fn = JsFunction::new(&mut cx, JinjaEngine::load_template)?;
        let load_template_fn = bind_method(&mut cx, load_template_fn, obj_this)?;
        obj.set(&mut cx, "loadTemplate", load_template_fn)?;

        Ok(obj)
    }
}

pub fn template_register_module(cx: &mut ModuleContext) -> NeonResult<()> {
    cx.export_function("newJinjaEngine", JinjaEngine::js_new)?;

    Ok(())
}
