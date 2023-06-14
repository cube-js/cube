use crate::python::cross::{CLRepr, CLReprObject};
use crate::python::template::mj_value::to_minijinja_value;
use log::{error, trace};
use minijinja as mj;
use neon::context::Context;
use neon::prelude::*;
use once_cell::sync::OnceCell;
use std::error::Error;
use std::sync::Mutex;

trait NeonMiniJinjaContext {
    fn throw_from_mj_error<T>(&mut self, err: mj::Error) -> NeonResult<T>;
}

impl<'a> NeonMiniJinjaContext for FunctionContext<'a> {
    fn throw_from_mj_error<T>(&mut self, err: mj::Error) -> NeonResult<T> {
        let codeblock = if let Some(source) = err.template_source() {
            let lines: Vec<_> = source.lines().enumerate().collect();
            let idx = err.line().unwrap_or(1).saturating_sub(1);
            let skip = idx.saturating_sub(3);

            let pre = lines.iter().skip(skip).take(3.min(idx)).collect::<Vec<_>>();
            let post = lines.iter().skip(idx + 1).take(3).collect::<Vec<_>>();

            let mut content = "".to_string();

            for (idx, line) in pre {
                content += &format!("{:>4} | {}\r\n", idx + 1, line);
            }

            if let Some(_span) = err.range() {
                // TODO(ovr): improve
                content += &format!(
                    "     i {}{} {}\r\n",
                    " ".repeat(0),
                    "^".repeat(24),
                    err.kind(),
                );
            } else {
                content += &format!("     | {}\r\n", "^".repeat(24));
            }

            for (idx, line) in post {
                content += &format!("{:>4} | {}\r\n", idx + 1, line);
            }

            format!("{}\r\n{}\r\n{}", "-".repeat(79), content, "-".repeat(79))
        } else {
            "".to_string()
        };

        if let Some(next_err) = err.source() {
            self.throw_error(format!(
                "{} caused by: {:#}\r\n{}",
                err, next_err, codeblock
            ))
        } else {
            self.throw_error(format!("{}\r\n{}", err, codeblock))
        }
    }
}

#[derive(Debug)]
struct EngineOptions {
    debug_info: bool,
}

static TEMPLATE_ENGINE: OnceCell<Mutex<mj::Environment>> = OnceCell::new();

fn init_template_engine<'a, C: Context<'a>>(_cx: &mut C, opts: EngineOptions) -> NeonResult<()> {
    let mut engine = mj::Environment::new();
    engine.set_debug(opts.debug_info);
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

    if let Err(_) = TEMPLATE_ENGINE.set(Mutex::new(engine)) {
        error!("Unable to init jinja engine, it was already started");
    }

    Ok(())
}

fn template_engine<'a, C: Context<'a>>(
    cx: &mut C,
) -> NeonResult<&'static Mutex<mj::Environment<'static>>> {
    if let Some(engine) = TEMPLATE_ENGINE.get() {
        Ok(engine)
    } else {
        cx.throw_error("Unable to get jinja engine: It was not initialized".to_string())
    }
}

fn init_jinja_engine(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    let options = cx.argument::<JsObject>(0)?;

    let debug_info: Handle<JsBoolean> = options
        .get_value(&mut cx, "debugInfo")?
        .downcast_or_throw(&mut cx)?;

    let options = EngineOptions {
        debug_info: debug_info.value(&mut cx),
    };
    init_template_engine(&mut cx, options)?;

    Ok(cx.undefined())
}

fn load_template(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    let template_name = cx.argument::<JsString>(0)?;
    let template_content = cx.argument::<JsString>(1)?;

    let mut engine = template_engine(&mut cx)?.lock().unwrap();

    if let Err(err) = engine.add_template_owned(
        template_name.value(&mut cx),
        template_content.value(&mut cx),
    ) {
        trace!("jinja load error: {:?}", err);

        return cx.throw_from_mj_error(err);
    }

    Ok(cx.undefined())
}

fn clear_templates(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    let mut engine = template_engine(&mut cx)?.lock().unwrap();
    engine.clear_templates();

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

            return cx.throw_from_mj_error(err);
        }
    };

    let mut ctx = CLReprObject::new();
    ctx.insert("COMPILE_CONTEXT".to_string(), template_ctx);

    let compile_context = to_minijinja_value(CLRepr::Object(ctx));
    match template.render(compile_context) {
        Ok(r) => Ok(cx.string(r)),
        Err(err) => {
            trace!("jinja render template error: {:?}", err);

            cx.throw_from_mj_error(err)
        }
    }
}

pub fn template_register_module(cx: &mut ModuleContext) -> NeonResult<()> {
    cx.export_function("initJinjaEngine", init_jinja_engine)?;
    cx.export_function("loadTemplate", load_template)?;
    cx.export_function("clearTemplates", clear_templates)?;
    cx.export_function("renderTemplate", render_template)?;

    Ok(())
}
