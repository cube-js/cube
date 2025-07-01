use neon::prelude::*;
use pyo3::prelude::*;

pub(crate) fn format_python_error(py_err: PyErr) -> String {
    let err = format!("Python error: {}", py_err);

    let bt = Python::with_gil(move |py| -> PyResult<Option<String>> {
        if let Some(trace_back) = py_err.traceback(py) {
            Ok(Some(trace_back.format()?))
        } else {
            Ok(None)
        }
    });

    match bt {
        Ok(Some(trace_back)) => format!("{}\r\n{}", err, trace_back),
        Err(bt_err) => {
            log::trace!("Unable to extract backtrace with error: {}", bt_err);

            err
        }
        _ => err,
    }
}

pub(crate) trait NeonPythonContext<'a>: Context<'a> {
    fn throw_from_python_error<T>(&mut self, py_err: PyErr) -> NeonResult<T> {
        self.throw_error(format_python_error(py_err))
    }
}

impl<'a> NeonPythonContext<'a> for FunctionContext<'a> {}

impl<'a> NeonPythonContext<'a> for TaskContext<'a> {}
