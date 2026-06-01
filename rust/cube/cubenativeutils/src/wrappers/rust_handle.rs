use crate::CubeError;
use std::any::{type_name, Any};
use std::rc::Rc;

/// Type-erased handle to a Rust value passed across the JS boundary.
///
/// Stores `Rc<dyn Any>` so any concrete `T: Any + 'static` can be put inside
/// and later recovered with [`Self::downcast`]. Tesseract is single-threaded;
/// `Rc` is sufficient and avoids the atomic overhead of `Arc`.
#[derive(Clone)]
pub struct NativeRustHandle {
    inner: Rc<dyn Any + 'static>,
    type_name: &'static str,
}

impl NativeRustHandle {
    pub fn new<T: Any + 'static>(value: T) -> Self {
        Self {
            inner: Rc::new(value),
            type_name: type_name::<T>(),
        }
    }

    pub fn from_rc<T: Any + 'static>(value: Rc<T>) -> Self {
        Self {
            inner: value,
            type_name: type_name::<T>(),
        }
    }

    pub fn downcast<T: Any + 'static>(&self) -> Result<Rc<T>, CubeError> {
        Rc::clone(&self.inner).downcast::<T>().map_err(|_| {
            CubeError::internal(format!(
                "NativeRustHandle: cannot downcast from {} to {}",
                self.type_name,
                type_name::<T>()
            ))
        })
    }

    pub fn type_name(&self) -> &'static str {
        self.type_name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct Probe {
        value: i32,
        tag: String,
    }

    #[derive(Debug)]
    struct Other(#[allow(dead_code)] u8);

    #[test]
    fn new_and_downcast_roundtrip() {
        let handle = NativeRustHandle::new(Probe {
            value: 42,
            tag: "hello".to_string(),
        });
        let probe = handle.downcast::<Probe>().expect("downcast must succeed");
        assert_eq!(probe.value, 42);
        assert_eq!(probe.tag, "hello");
    }

    #[test]
    fn from_rc_preserves_identity() {
        let original = Rc::new(Probe {
            value: 7,
            tag: "x".to_string(),
        });
        let handle = NativeRustHandle::from_rc(original.clone());
        let recovered = handle.downcast::<Probe>().expect("downcast");
        assert!(Rc::ptr_eq(&original, &recovered));
    }

    #[test]
    fn downcast_wrong_type_reports_names() {
        let handle = NativeRustHandle::new(Probe {
            value: 0,
            tag: String::new(),
        });
        let err = handle.downcast::<Other>().expect_err("must fail");
        let msg = err.message;
        assert!(msg.contains("Probe"), "missing source type: {msg}");
        assert!(msg.contains("Other"), "missing target type: {msg}");
    }

    #[test]
    fn clone_shares_inner() {
        let handle = NativeRustHandle::new(Probe {
            value: 1,
            tag: String::new(),
        });
        let a = handle.downcast::<Probe>().unwrap();
        let cloned = handle.clone();
        let b = cloned.downcast::<Probe>().unwrap();
        assert!(Rc::ptr_eq(&a, &b));
        assert_eq!(Rc::strong_count(&a), 4);
        // 4 = original handle, cloned handle, plus the two Rc<Probe> we just downcast.
    }

    #[test]
    fn type_name_is_recorded() {
        let handle = NativeRustHandle::new(Probe {
            value: 0,
            tag: String::new(),
        });
        assert!(handle.type_name().ends_with("Probe"));
    }
}
