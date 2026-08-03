use crate::cross::*;
use minijinja::value as mjv;

#[cfg(feature = "python")]
mod python;
mod value;

pub fn to_minijinja_value(from: CLRepr) -> mjv::Value {
    match from {
        CLRepr::Tuple(inner) | CLRepr::Array(inner) => {
            mjv::Value::from_seq_object(value::JinjaSequenceObject { inner })
        }
        CLRepr::String(v, mode) => match mode {
            StringType::Normal => mjv::Value::from(v),
            StringType::Safe => mjv::Value::from_safe_string(v),
        },
        CLRepr::Float(v) => mjv::Value::from(v),
        CLRepr::Int(v) => mjv::Value::from(v),
        CLRepr::Bool(v) => mjv::Value::from(v),
        CLRepr::Null => mjv::Value::from(()),
        CLRepr::Object(inner) => mjv::Value::from_object(value::JinjaDictObject { inner }),
        #[cfg(feature = "python")]
        CLRepr::PythonRef(py_ref) => match py_ref {
            PythonRef::PyObject(inner) => {
                mjv::Value::from_object(python::JinjaPythonObject { inner })
            }
            PythonRef::PyFunction(inner) | PythonRef::PyExternalFunction(inner) => {
                mjv::Value::from_object(python::JinjaPythonFunction { inner })
            }
        },
        CLRepr::JsFunction(_) => panic!(
            "Converting from {:?} to minijinja::Value is not supported",
            CLReprKind::JsFunction
        ),
    }
}

#[cfg(feature = "python")]
pub use python::from_minijinja_value;
