use crate::template::neon::*;
use cubesql::CubeError;

use log::{error, trace};
use minijinja as mj;
use neon::prelude::*;
use neon::types::Deferred;

pub struct JinjaEngineWorkerJob {
    pub(crate) template_name: String,
    pub(crate) ctx: minijinja::value::Value,
    pub(crate) deferred: Deferred,
}

struct JinjaEngineWorker {
    _thread: std::thread::JoinHandle<()>,
}

impl JinjaEngineWorker {
    #[inline(always)]
    fn process_render(job: JinjaEngineWorkerJob, js_channel: &Channel, env: &mj::Environment) {
        let template = match env.get_template(&job.template_name) {
            Ok(t) => t,
            Err(err) => {
                job.deferred
                    .settle_with(&js_channel, move |mut cx| -> NeonResult<Handle<JsString>> {
                        cx.throw_from_mj_error(err)
                    });

                return;
            }
        };

        let result = template.render(job.ctx);
        job.deferred
            .settle_with(&js_channel, move |mut cx| -> NeonResult<Handle<JsString>> {
                match result {
                    Ok(r) => Ok(cx.string(r)),
                    Err(err) => cx.throw_from_mj_error(err),
                }
            });
    }

    fn new(
        id: usize,
        env: mj::Environment<'static>,
        js_channel: Channel,
        receiver: async_channel::Receiver<JinjaEngineWorkerJob>,
    ) -> Self {
        let thread = std::thread::spawn(move || loop {
            if let Ok(job) = receiver.recv_blocking() {
                if let Err(err) =
                    std::panic::catch_unwind(|| Self::process_render(job, &js_channel, &env))
                {
                    let internal_err = CubeError::panic(err);

                    error!("Panic while rendering jinja template: {}", internal_err);

                    job.deferred.settle_with(
                        &js_channel,
                        move |mut cx| -> NeonResult<Handle<JsString>> {
                            cx.throw_error(format!(
                                "Panic while rendering jinja template: {}",
                                internal_err
                            ))
                        },
                    );
                }
            } else {
                trace!(
                    "Closing jinja thread, id: {}, threadId: {}",
                    id,
                    std::thread::current().id().as_u64()
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
