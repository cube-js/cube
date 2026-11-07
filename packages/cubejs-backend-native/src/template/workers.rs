use crate::template::neon_mj::*;
use cubesql::CubeError;

use log::{error, trace};
use minijinja as mj;
use neon::prelude::*;
use neon::types::Deferred;
use std::panic;

pub struct JinjaEngineWorkerJob {
    pub(crate) template_name: String,
    pub(crate) ctx: minijinja::value::Value,
    pub(crate) deferred: Deferred,
}

struct JinjaEngineWorker {
    _thread: std::thread::JoinHandle<()>,
}

impl JinjaEngineWorker {
    /// Renders a template, converting an unwinding panic into an error.
    ///
    /// Rendering executes user provided code (Python filters/functions via pyo3, custom
    /// value implementations), which can panic. Without catching it, the worker thread
    /// dies: the promise is rejected with a useless `Deferred` was dropped without being
    /// settled error and the pool silently shrinks. Once the last worker is gone, the job
    /// channel is closed and every next render fails with "sending into a closed channel".
    fn render_catch_panic(
        env: &mj::Environment,
        template_name: &str,
        ctx: mj::value::Value,
    ) -> Result<Result<String, mj::Error>, CubeError> {
        let render_block = panic::AssertUnwindSafe(|| {
            let template = env.get_template(template_name)?;

            template.render(ctx)
        });

        panic::catch_unwind(render_block).map_err(|panic_payload| {
            CubeError::panic_with_message(
                panic_payload,
                "Unexpected panic while rendering jinja template",
            )
        })
    }

    fn process_render(job: JinjaEngineWorkerJob, js_channel: &Channel, env: &mj::Environment) {
        let JinjaEngineWorkerJob {
            template_name,
            ctx,
            deferred,
        } = job;

        match Self::render_catch_panic(env, &template_name, ctx) {
            Ok(result) => {
                deferred.settle_with(js_channel, move |mut cx| -> NeonResult<Handle<JsString>> {
                    match result {
                        Ok(r) => Ok(cx.string(r)),
                        Err(err) => cx.throw_from_mj_error(err),
                    }
                });
            }
            Err(err) => {
                error!("{} (template: {})", err, template_name);

                deferred.settle_with(js_channel, move |mut cx| -> NeonResult<Handle<JsString>> {
                    cx.throw_error(err.to_string())
                });
            }
        }
    }

    fn new(
        id: usize,
        env: mj::Environment<'static>,
        js_channel: Channel,
        receiver: async_channel::Receiver<JinjaEngineWorkerJob>,
    ) -> Self {
        let thread = std::thread::spawn(move || loop {
            if let Ok(job) = receiver.recv_blocking() {
                Self::process_render(job, &js_channel, &env);
            } else {
                trace!(
                    "Closing jinja thread, id: {}, threadId: {:?}",
                    id,
                    // TODO: Use as_u64 when it will be stable - https://github.com/rust-lang/rust/issues/67939
                    std::thread::current().id()
                );

                return;
            }
        });

        Self { _thread: thread }
    }
}

pub struct JinjaEngineWorkerPool {
    workers_rx: async_channel::Sender<JinjaEngineWorkerJob>,
    _workers: Vec<JinjaEngineWorker>,
}

impl JinjaEngineWorkerPool {
    pub fn new(
        workers_count: usize,
        js_channel: Channel,
        jinja_engine: minijinja::Environment<'static>,
    ) -> Self {
        let (workers_rx, receiver) = async_channel::bounded::<JinjaEngineWorkerJob>(1_000);

        let mut workers = vec![];

        for id in 0..workers_count {
            workers.push(JinjaEngineWorker::new(
                id,
                jinja_engine.clone(),
                js_channel.clone(),
                receiver.clone(),
            ));
        }

        Self {
            _workers: workers,
            workers_rx,
        }
    }

    pub fn render(&self, job: JinjaEngineWorkerJob) -> Result<(), CubeError> {
        self.workers_rx
            .send_blocking(job)
            .map_err(|err| CubeError::internal(format!("Unable to schedule rendering: {}", err)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_environment() -> mj::Environment<'static> {
        let mut env = mj::Environment::new();

        env.add_function("panic_fn", || -> Result<mj::value::Value, mj::Error> {
            panic!("Boom from a function")
        });

        env.add_template("render.jinja", "Hello {{ name }}")
            .unwrap();
        env.add_template("panic.jinja", "{{ panic_fn() }}").unwrap();

        env
    }

    #[test]
    fn test_render() {
        let env = test_environment();

        let actual = JinjaEngineWorker::render_catch_panic(
            &env,
            "render.jinja",
            mj::context! { name => "world" },
        )
        .expect("Render must not panic")
        .expect("Render must not fail");

        assert_eq!(actual, "Hello world");
    }

    #[test]
    fn test_render_unknown_template() {
        let env = test_environment();

        let err = JinjaEngineWorker::render_catch_panic(
            &env,
            "unknown.jinja",
            mj::value::Value::UNDEFINED,
        )
        .expect("Unknown template must not panic")
        .expect_err("Unknown template must fail");

        assert_eq!(err.kind(), mj::ErrorKind::TemplateNotFound);
    }

    #[test]
    fn test_render_panic() {
        let env = test_environment();

        let err =
            JinjaEngineWorker::render_catch_panic(&env, "panic.jinja", mj::value::Value::UNDEFINED)
                .expect_err("Panic must be caught");

        assert_eq!(
            err.message,
            "Unexpected panic while rendering jinja template. Reason: Boom from a function"
        );

        // Environment must stay usable after a panic, because the worker is reused
        let actual = JinjaEngineWorker::render_catch_panic(
            &env,
            "render.jinja",
            mj::context! { name => "world" },
        )
        .expect("Render must not panic")
        .expect("Render must not fail");

        assert_eq!(actual, "Hello world");
    }
}
