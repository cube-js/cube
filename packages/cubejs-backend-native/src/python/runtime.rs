use crate::cross::CLRepr;
use crate::python::neon_py::*;
use crate::tokio_runtime_node;
use cubesql::CubeError;
use log::{error, trace};
use neon::prelude::*;
use neon::types::Deferred;
use once_cell::sync::OnceCell;
use pyo3::prelude::*;
use pyo3::types::{PyFunction, PyTuple};
use std::fmt::Formatter;
use std::future::Future;
use std::pin::Pin;

#[derive(Debug)]
pub struct PyScheduledFun {
    fun: Py<PyFunction>,
    args: Vec<CLRepr>,
    callback: PyScheduledCallback,
}

pub enum PyScheduledCallback {
    NodeDeferred(Deferred),
    Channel(tokio::sync::oneshot::Sender<Result<CLRepr, CubeError>>),
}

impl std::fmt::Debug for PyScheduledCallback {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PyScheduledCallback::NodeDeferred(_) => write!(f, "NodeDeferred<hidden>"),
            PyScheduledCallback::Channel(_) => write!(f, "Channel<hidden>"),
        }
    }
}

impl PyScheduledFun {
    pub fn split(self) -> (Py<PyFunction>, Vec<CLRepr>, PyScheduledCallback) {
        (self.fun, self.args, self.callback)
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
            callback: PyScheduledCallback::NodeDeferred(deferred),
        });
        if let Err(err) = res {
            // TODO: We need to return this error to deferred, but for now
            // neon will handle this issue on Drop
            error!("Unable to schedule python function call: {}", err)
        }
    }

    pub async fn call_async(
        &self,
        fun: Py<PyFunction>,
        args: Vec<CLRepr>,
    ) -> Result<CLRepr, CubeError> {
        let (rx, tx) = tokio::sync::oneshot::channel();

        self.sender
            .send(PyScheduledFun {
                fun,
                args,
                callback: PyScheduledCallback::Channel(rx),
            })
            .await
            .map_err(|err| {
                CubeError::internal(format!("Unable to schedule python function call: {}", err))
            })?;

        tx.await?
    }

    fn process_task(
        task: PyScheduledFun,
        js_channel: &neon::event::Channel,
    ) -> Result<(), CubeError> {
        let (fun, args, callback) = task.split();

        let task_result = Python::with_gil(move |py| -> PyResult<PyScheduledFunResult> {
            let mut prep_tuple = Vec::with_capacity(args.len());
            let mut py_kwargs = None;

            for arg in args {
                if arg.is_kwarg() {
                    py_kwargs = Some(arg.into_py_dict(py)?);
                } else {
                    prep_tuple.push(arg.into_py(py)?);
                }
            }

            let py_args = PyTuple::new(py, prep_tuple);
            let call_res = fun.call(py, py_args, py_kwargs)?;

            let is_coroutine = unsafe { pyo3::ffi::PyCoro_CheckExact(call_res.as_ptr()) == 1 };
            if is_coroutine {
                let fut = pyo3_asyncio::tokio::into_future(call_res.as_ref(py))?;
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
                match callback {
                    PyScheduledCallback::NodeDeferred(deferred) => {
                        deferred.settle_with(
                            js_channel,
                            move |mut cx| -> NeonResult<Handle<JsError>> {
                                cx.throw_from_python_error(err)
                            },
                        );
                    }
                    PyScheduledCallback::Channel(chan) => {
                        let send_res =
                            chan.send(Err(CubeError::internal(format_python_error(err))));
                        if send_res.is_err() {
                            return Err(CubeError::internal(
                                "Unable to send result back to consumer".to_string(),
                            ));
                        }
                    }
                };

                return Ok(());
            }
        };

        match task_result {
            PyScheduledFunResult::Poll(fut) => {
                let js_channel_to_move = js_channel.clone();

                tokio::spawn(async move {
                    let fut_res = fut.await;

                    let res = Python::with_gil(move |py| -> Result<CLRepr, PyErr> {
                        let res = match fut_res {
                            Ok(r) => CLRepr::from_python_ref(r.as_ref(py)),
                            Err(err) => Err(err),
                        };

                        res
                    });

                    match callback {
                        PyScheduledCallback::NodeDeferred(deferred) => {
                            deferred.settle_with(&js_channel_to_move, |mut cx| match res {
                                Err(err) => cx.throw_error(format!("Python error: {}", err)),
                                Ok(r) => r.into_js(&mut cx),
                            });
                        }
                        PyScheduledCallback::Channel(chan) => {
                            let _ = match res {
                                Ok(r) => chan.send(Ok(r)),
                                Err(err) => {
                                    chan.send(Err(CubeError::internal(format_python_error(err))))
                                }
                            };
                        }
                    }
                });
            }
            PyScheduledFunResult::Ready(r) => match callback {
                PyScheduledCallback::NodeDeferred(deferred) => {
                    deferred.settle_with(js_channel, |mut cx| r.into_js(&mut cx));
                }
                PyScheduledCallback::Channel(chan) => {
                    if chan.send(Ok(r)).is_err() {
                        return Err(CubeError::internal(
                            "Unable to send result back to consumer".to_string(),
                        ));
                    }
                }
            },
        };

        Ok(())
    }

    pub fn new(js_channel: neon::event::Channel) -> Self {
        let (sender, mut receiver) = tokio::sync::mpsc::channel::<PyScheduledFun>(1024);

        trace!("New Python runtime");

        std::thread::spawn(|| {
            trace!("Initializing executor in a separate thread");

            std::thread::spawn(|| {
                pyo3_asyncio::tokio::get_runtime()
                    .block_on(pyo3_asyncio::tokio::re_exports::pending::<()>())
            });

            let res = Python::with_gil(|py| -> Result<(), PyErr> {
                pyo3_asyncio::tokio::run(py, async move {
                    loop {
                        if let Some(task) = receiver.recv().await {
                            trace!("New task");

                            if let Err(err) = Self::process_task(task, &js_channel) {
                                error!("Error while processing python task: {:?}", err)
                            };
                        }
                    }
                })
            });
            match res {
                Ok(_) => trace!("Python runtime loop was closed without error"),
                Err(err) => error!("Critical error while processing python call: {}", err),
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

    let runtime = tokio_runtime_node(cx)?;

    pyo3::prepare_freethreaded_python();
    // it's safe to unwrap
    pyo3_asyncio::tokio::init_with_runtime(runtime).unwrap();

    if PY_RUNTIME.set(PyRuntime::new(channel)).is_err() {
        cx.throw_error("Error on setting PyRuntime")
    } else {
        Ok(())
    }
}

pub fn py_runtime() -> Result<&'static PyRuntime, CubeError> {
    if let Some(runtime) = PY_RUNTIME.get() {
        Ok(runtime)
    } else {
        Err(CubeError::internal(
            "Unable to get PyRuntime: It was not initialized".to_string(),
        ))
    }
}
