use crate::python::cross::CLRepr;
use crate::runtime;
use log::{error, trace};
use neon::prelude::*;
use neon::types::Deferred;
use once_cell::sync::OnceCell;
use pyo3::prelude::*;
use pyo3::types::{PyFunction, PyTuple};
use pyo3::AsPyPointer;
use std::future::Future;
use std::pin::Pin;

struct PyScheduledFun {
    fun: Py<PyFunction>,
    args: Vec<CLRepr>,
    deferred: Deferred,
}

impl PyScheduledFun {
    pub fn split(self) -> (Py<PyFunction>, Vec<CLRepr>, Deferred) {
        (self.fun, self.args, self.deferred)
    }
}

enum PyScheduledFunResult {
    Poll(Pin<Box<dyn Future<Output = PyResult<PyObject>> + Send>>),
    Ready(CLRepr),
}

pub struct PyRuntime {
    sender: tokio::sync::mpsc::Sender<PyScheduledFun>,
}

impl PyRuntime {
    pub fn call_async_with_promise_callback(
        &self,
        fun: Py<PyFunction>,
        args: Vec<CLRepr>,
        deferred: Deferred,
    ) {
        let res = self.sender.blocking_send(PyScheduledFun {
            fun,
            args,
            deferred,
        });
        if let Err(err) = res {
            // TODO: We need to return this error to deferred, but for now
            // neon will handle this issue on Drop
            error!("Unable to schedule python function call: {}", err)
        }
    }

    pub fn new(js_channel: neon::event::Channel) -> Self {
        let (sender, mut receiver) = tokio::sync::mpsc::channel::<PyScheduledFun>(1024);

        std::thread::spawn(|| {
            std::thread::spawn(|| {
                pyo3_asyncio::tokio::get_runtime()
                    .block_on(pyo3_asyncio::tokio::re_exports::pending::<()>())
            });

            let res = Python::with_gil(|py| -> Result<(), PyErr> {
                pyo3_asyncio::tokio::run(py, async move {
                    loop {
                        if let Some(task) = receiver.recv().await {
                            let (fun, args, deferred) = task.split();
                            trace!("[py_runtime] task");

                            let task_result =
                                Python::with_gil(move |py| -> PyResult<PyScheduledFunResult> {
                                    let mut args_tuple = Vec::with_capacity(args.len());

                                    for arg in args {
                                        args_tuple.push(arg.into_py(py)?);
                                    }

                                    let args = PyTuple::new(py, args_tuple);
                                    let call_res = fun.call1(py, args)?;

                                    let is_coroutine = unsafe {
                                        pyo3::ffi::PyCoro_CheckExact(call_res.as_ptr()) == 1
                                    };
                                    if is_coroutine {
                                        let fut =
                                            pyo3_asyncio::tokio::into_future(call_res.as_ref(py))?;
                                        Ok(PyScheduledFunResult::Poll(Box::pin(fut)))
                                    } else {
                                        Ok(PyScheduledFunResult::Ready(CLRepr::from_python_ref(
                                            call_res.as_ref(py),
                                        )?))
                                    }
                                });
                            let task_result = match task_result {
                                Ok(r) => r,
                                Err(err) => {
                                    deferred.settle_with(
                                        &js_channel,
                                        move |mut cx| -> NeonResult<Handle<JsError>> {
                                            cx.throw_error(format!("Python error: {}", err))
                                        },
                                    );

                                    continue;
                                }
                            };

                            match task_result {
                                PyScheduledFunResult::Poll(fut) => {
                                    let js_channel_to_move = js_channel.clone();

                                    tokio::spawn(async move {
                                        let fut_res = fut.await;

                                        let res =
                                            Python::with_gil(move |py| -> Result<CLRepr, PyErr> {
                                                let res = match fut_res {
                                                    Ok(r) => CLRepr::from_python_ref(r.as_ref(py)),
                                                    Err(err) => Err(err),
                                                };

                                                res
                                            });

                                        deferred.settle_with(&js_channel_to_move, |mut cx| {
                                            let l = match res {
                                                Err(err) => {
                                                    cx.throw_error(format!("Python error: {}", err))
                                                }
                                                Ok(r) => r.into_js(&mut cx),
                                            };
                                            l
                                        });
                                    });
                                }
                                PyScheduledFunResult::Ready(r) => {
                                    deferred.settle_with(&js_channel, |mut cx| r.into_js(&mut cx));
                                }
                            };
                        }
                    }

                    #[allow(unreachable_code)]
                    Ok(())
                })
            });
            if let Err(err) = res {
                error!("Critical error while processing python calls: {}", err)
            }
        });

        Self { sender }
    }
}

static PY_RUNTIME: OnceCell<PyRuntime> = OnceCell::new();

pub fn py_runtime_init<'a, C: Context<'a>>(
    cx: &mut C,
    channel: neon::event::Channel,
) -> NeonResult<()> {
    if PY_RUNTIME.get().is_some() {
        return Ok(());
    }

    let runtime = runtime(cx)?;

    pyo3::prepare_freethreaded_python();
    // it's safe to unwrap
    pyo3_asyncio::tokio::init_with_runtime(runtime).unwrap();

    if let Err(_) = PY_RUNTIME.set(PyRuntime::new(channel)) {
        cx.throw_error(format!("Error on setting PyRuntime"))
    } else {
        Ok(())
    }
}

pub fn py_runtime<'a, C: Context<'a>>(cx: &mut C) -> NeonResult<&PyRuntime> {
    if let Some(runtime) = PY_RUNTIME.get() {
        Ok(runtime)
    } else {
        cx.throw_error("Unable to get PyRuntime: It was not initialized".to_string())
    }
}
