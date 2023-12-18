use crate::cross::*;
use pyo3::exceptions::PyNotImplementedError;
use pyo3::prelude::*;
use pyo3::types::{PyFunction, PyString, PyTuple};

pub fn python_fn_call_sync(py_fun: &Py<PyFunction>, arguments: Vec<CLRepr>) -> PyResult<CLRepr> {
    Python::with_gil(|py| {
        let mut args_tuple = Vec::with_capacity(arguments.len());

        for arg in arguments {
            args_tuple.push(arg.into_py(py)?);
        }

        let tuple = PyTuple::new(py, args_tuple);

        let call_res = py_fun.call1(py, tuple)?;

        let is_coroutine = unsafe { pyo3::ffi::PyCoro_CheckExact(call_res.as_ptr()) == 1 };
        if is_coroutine {
            Err(PyErr::new::<PyNotImplementedError, _>(
                "Calling function with async response is not supported",
            ))
        } else {
            CLRepr::from_python_ref(call_res.as_ref(py))
        }
    })
}

pub fn python_obj_call_sync(py_fun: &PyObject, arguments: Vec<CLRepr>) -> PyResult<CLRepr> {
    Python::with_gil(|py| {
        let mut args_tuple = Vec::with_capacity(arguments.len());

        for arg in arguments {
            args_tuple.push(arg.into_py(py)?);
        }

        let tuple = PyTuple::new(py, args_tuple);

        let call_res = py_fun.call1(py, tuple)?;

        let is_coroutine = unsafe { pyo3::ffi::PyCoro_CheckExact(call_res.as_ptr()) == 1 };
        if is_coroutine {
            Err(PyErr::new::<PyNotImplementedError, _>(
                "Calling object with async response is not supported",
            ))
        } else {
            CLRepr::from_python_ref(call_res.as_ref(py))
        }
    })
}

pub fn python_obj_method_call_sync<N>(
    py_fun: &PyObject,
    name: N,
    arguments: Vec<CLRepr>,
) -> PyResult<CLRepr>
where
    N: IntoPy<Py<PyString>>,
{
    Python::with_gil(|py| {
        let mut args_tuple = Vec::with_capacity(arguments.len());

        for arg in arguments {
            args_tuple.push(arg.into_py(py)?);
        }

        let tuple = PyTuple::new(py, args_tuple);

        let call_res = py_fun.call_method1(py, name, tuple)?;

        let is_coroutine = unsafe { pyo3::ffi::PyCoro_CheckExact(call_res.as_ptr()) == 1 };
        if is_coroutine {
            Err(PyErr::new::<PyNotImplementedError, _>(
                "Calling object method with async response is not supported",
            ))
        } else {
            CLRepr::from_python_ref(call_res.as_ref(py))
        }
    })
}
