use crate::cross::*;
use crate::python::python_fn_call_sync;
use crate::template::mj_value::{from_minijinja_value, to_minijinja_value};
use minijinja as mj;
use neon::prelude::*;

pub fn mj_inject_python_extension(
    cx: &mut FunctionContext,
    options: Handle<JsObject>,
    engine: &mut mj::Environment,
) -> NeonResult<()> {
    let filters = options
        .get_value(cx, "filters")?
        .downcast_or_throw::<JsObject, _>(cx)?;

    let filter_names = filters.get_own_property_names(cx)?;
    for i in 0..filter_names.len(cx) {
        let filter_name: Handle<JsString> = filter_names.get(cx, i)?;
        let filter_fun = CLRepr::from_js_ref(filters.get_value(cx, filter_name)?, cx)?;

        let py_fun = match filter_fun {
            CLRepr::PythonRef(py_ref) => match py_ref {
                PythonRef::PyFunction(py_fun_ref) | PythonRef::PyExternalFunction(py_fun_ref) => {
                    py_fun_ref
                }
                other => {
                    return cx.throw_error(format!(
                        "minijinja::filter must be a function, actual: CLRepr::PythonRef({:?})",
                        other
                    ))
                }
            },
            other => {
                return cx.throw_error(format!(
                    "minijinja::filter must be a function, actual: {:?}",
                    other.kind()
                ))
            }
        };

        engine.add_filter(
            filter_name.value(cx),
            move |_state: &mj::State,
                  args: &[mj::value::Value]|
                  -> Result<mj::value::Value, mj::Error> {
                let mut arguments = Vec::with_capacity(args.len());

                for arg in args {
                    arguments.push(from_minijinja_value(arg)?);
                }

                match python_fn_call_sync(&py_fun, arguments) {
                    Ok(r) => Ok(to_minijinja_value(r)),
                    Err(err) => Err(mj::Error::new(
                        minijinja::ErrorKind::InvalidOperation,
                        format!("Error while calling filter: {}", err),
                    )),
                }
            },
        );
    }

    Ok(())
}
