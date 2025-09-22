use crate::cross::*;
use pyo3::exceptions::{PyNotImplementedError, PySystemError};
use pyo3::prelude::*;
use pyo3::types::{PyFunction, PyString, PyTuple};
use pyo3::{ffi, AsPyPointer};
use std::ffi::c_int;

pub fn python_fn_call_sync(py_fun: &Py<PyFunction>, arguments: Vec<CLRepr>) -> PyResult<CLRepr> {
    Python::with_gil(|py| {
        let mut args_tuple = Vec::with_capacity(arguments.len());

        for arg in arguments {
            args_tuple.push(arg.into_py(py)?);
        }

        let tuple = PyTuple::new(py, args_tuple);

        let call_res = py_fun.call1(py, tuple)?;

        if call_res.is_coroutine()? {
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

        if call_res.is_coroutine()? {
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

        if call_res.is_coroutine()? {
            Err(PyErr::new::<PyNotImplementedError, _>(
                "Calling object method with async response is not supported",
            ))
        } else {
            CLRepr::from_python_ref(call_res.as_ref(py))
        }
    })
}

pub trait PyAnyHelpers {
    fn is_sequence(&self) -> PyResult<bool>;

    fn is_coroutine(&self) -> PyResult<bool>;
}

pub(crate) fn internal_error_on_minusone(result: c_int) -> PyResult<()> {
    if result != -1 {
        Ok(())
    } else {
        Err(PyErr::new::<PySystemError, _>(
            "Error on call via ffi, result is -1",
        ))
    }
}

impl<T> PyAnyHelpers for T
where
    T: AsPyPointer,
{
    fn is_sequence(&self) -> PyResult<bool> {
        let ptr = self.as_ptr();
        if ptr.is_null() {
            return Err(PyErr::new::<PySystemError, _>(
                "Unable to verify that object is sequence, because ptr is null",
            ));
        }

        let v = unsafe { ffi::PySequence_Check(ptr) };
        internal_error_on_minusone(v)?;

        Ok(v != 0)
    }

    fn is_coroutine(&self) -> PyResult<bool> {
        let ptr = self.as_ptr();
        if ptr.is_null() {
            return Err(PyErr::new::<PySystemError, _>(
                "Unable to verify that object is coroutine, because ptr is null",
            ));
        }

        let v = unsafe { ffi::PyCoro_CheckExact(ptr) };
        internal_error_on_minusone(v)?;

        Ok(v != 0)
    }
}
