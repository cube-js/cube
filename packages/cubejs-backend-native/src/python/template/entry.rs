use crate::python::cross::{CLRepr, CLReprObject};
use crate::python::template::mj_value::to_minijinja_value;
use log::trace;
use minijinja as mj;
use neon::context::Context;
use neon::prelude::*;
use once_cell::sync::OnceCell;
use std::sync::Mutex;

fn template_engine<'a, C: Context<'a>>(
    _cx: &mut C,
) -> NeonResult<&'static Mutex<mj::Environment<'static>>> {
    static STATE: OnceCell<Mutex<mj::Environment>> = OnceCell::new();

    STATE.get_or_try_init(|| {
        let mut engine = mj::Environment::new();
        engine.set_debug(false);
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

        Ok(Mutex::new(engine))
    })
}

fn load_templates(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    let input = cx.argument::<JsArray>(0)?.to_vec(&mut cx)?;

    let mut engine = template_engine(&mut cx)?.lock().unwrap();
    for to_load in input {
        let to_load_obj: Handle<JsObject> = to_load.downcast_or_throw(&mut cx)?;

        if let Err(err) = engine.add_template_owned(
            to_load_obj
                .get_value(&mut cx, "fileName")?
                .to_string(&mut cx)?
                .value(&mut cx),
            to_load_obj
                .get_value(&mut cx, "content")?
                .to_string(&mut cx)?
                .value(&mut cx),
        ) {
            trace!("jinja load error: {:?}", err);

            return cx.throw_error(format!("{}", err));
        }
    }

    Ok(cx.undefined())
}

fn render_template(mut cx: FunctionContext) -> JsResult<JsString> {
    let template_name = cx.argument::<JsString>(0)?;
    let template_ctx = CLRepr::from_js_ref(cx.argument::<JsValue>(1)?, &mut cx)?;

    let engine = template_engine(&mut cx)?.lock().unwrap();

    let template = match engine.get_template(&template_name.value(&mut cx)) {
        Ok(t) => t,
        Err(err) => {
            trace!("jinja get template error: {:?}", err);

            return cx.throw_error(format!("{}", err));
        }
    };

    let mut ctx = CLReprObject::new();
    ctx.insert("COMPILE_CONTEXT".to_string(), template_ctx);

    let compile_context = to_minijinja_value(CLRepr::Object(ctx));
    match template.render(compile_context) {
        Ok(r) => Ok(cx.string(r)),
        Err(err) => {
            trace!("jinja render template error: {:?}", err);

            cx.throw_error(format!("{}", err))
        }
    }
}

pub fn template_register_module(cx: &mut ModuleContext) -> NeonResult<()> {
    cx.export_function("loadTemplates", load_templates)?;
    cx.export_function("renderTemplate", render_template)?;

    Ok(())
}
