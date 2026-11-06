use neon::prelude::*;
use pyo3::prelude::*;
use pyo3::types::PyTraceback;

// The cube package registers template functions through wrappers with these names (see
// `_in_template_context`): their frames are left out, errors read as without them
const TEMPLATE_CALL_FRAME_PREFIX: &str = "_cube_template_call";

fn skip_template_call_frames(
    trace_back: Bound<'_, PyTraceback>,
) -> PyResult<Bound<'_, PyTraceback>> {
    let mut current = trace_back;

    loop {
        let name: String = current
            .getattr("tb_frame")?
            .getattr("f_code")?
            .getattr("co_name")?
            .extract()?;
        if !name.starts_with(TEMPLATE_CALL_FRAME_PREFIX) {
            return Ok(current);
        }

        let next = current.getattr("tb_next")?;
        if next.is_none() {
            return Ok(current);
        }
        current = next.downcast_into::<PyTraceback>()?;
    }
}

pub(crate) fn format_python_error(py_err: PyErr) -> String {
    let err = format!("Python error: {}", py_err);

    let bt = Python::with_gil(move |py| -> PyResult<Option<String>> {
        if let Some(trace_back) = py_err.traceback_bound(py) {
            Ok(Some(skip_template_call_frames(trace_back)?.format()?))
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
