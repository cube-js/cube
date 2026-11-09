use crate::cross::*;
use pyo3::exceptions::{PyNotImplementedError, PySystemError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyFunction, PyString, PyTuple};
use std::ffi::c_int;

type PositionalAndKwargs<'py> = (Vec<Py<PyAny>>, Option<Bound<'py, PyDict>>);

pub(crate) fn split_positional_and_kwargs(
    py: Python<'_>,
    arguments: Vec<CLRepr>,
) -> PyResult<PositionalAndKwargs<'_>> {
    let mut args_tuple = Vec::with_capacity(arguments.len());
    let mut kwargs_dict_opt: Option<Bound<PyDict>> = None;

    for arg in arguments {
        if arg.is_kwarg() {
            kwargs_dict_opt = Some(arg.into_py_dict(py)?);
        } else {
            args_tuple.push(arg.into_py(py)?);
        }
    }

    Ok((args_tuple, kwargs_dict_opt))
}

pub fn python_fn_call_sync(py_fun: &Py<PyFunction>, arguments: Vec<CLRepr>) -> PyResult<CLRepr> {
    Python::attach(|py| {
        let (args_tuple, kwargs_dict_opt) = split_positional_and_kwargs(py, arguments)?;

        let tuple = PyTuple::new(py, args_tuple)?;

        let call_res = py_fun.call(py, tuple, kwargs_dict_opt.as_ref())?;

        if call_res.bind(py).is_coroutine()? {
            Err(PyErr::new::<PyNotImplementedError, _>(
                "Calling function with async response is not supported",
            ))
        } else {
            CLRepr::from_python_ref(call_res.bind(py))
        }
    })
}

pub fn python_obj_call_sync(py_fun: &Py<PyAny>, arguments: Vec<CLRepr>) -> PyResult<CLRepr> {
    Python::attach(|py| {
        let (args_tuple, kwargs_dict_opt) = split_positional_and_kwargs(py, arguments)?;

        let tuple = PyTuple::new(py, args_tuple)?;

        let call_res = py_fun.call(py, tuple, kwargs_dict_opt.as_ref())?;

        if call_res.bind(py).is_coroutine()? {
            Err(PyErr::new::<PyNotImplementedError, _>(
                "Calling object with async response is not supported",
            ))
        } else {
            CLRepr::from_python_ref(call_res.bind(py))
        }
    })
}

pub fn python_obj_method_call_sync<N>(
    py_fun: &Py<PyAny>,
    name: N,
    arguments: Vec<CLRepr>,
) -> PyResult<CLRepr>
where
    N: for<'py> IntoPyObject<'py, Target = PyString>,
{
    Python::attach(|py| {
        let (args_tuple, kwargs_dict_opt) = split_positional_and_kwargs(py, arguments)?;

        let tuple = PyTuple::new(py, args_tuple)?;

        let call_res = py_fun.call_method(py, name, tuple, kwargs_dict_opt.as_ref())?;

        if call_res.bind(py).is_coroutine()? {
            Err(PyErr::new::<PyNotImplementedError, _>(
                "Calling object method with async response is not supported",
            ))
        } else {
            CLRepr::from_python_ref(call_res.bind(py))
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

impl PyAnyHelpers for Bound<'_, PyAny> {
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
