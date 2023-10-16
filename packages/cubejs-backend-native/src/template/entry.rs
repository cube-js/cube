use crate::cross::{CLRepr, CLReprObject};
use crate::template::mj_value::to_minijinja_value;
use crate::utils::bind_method;
use log::trace;
use minijinja as mj;
use neon::context::Context;
use neon::prelude::*;
use std::cell::RefCell;
use std::error::Error;

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

            content += &format!("{:>4} > {}\r\n", idx + 1, lines[idx].1);

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
