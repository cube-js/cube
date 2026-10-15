use std::fmt;
use std::rc::Rc;

/// Identity of a cube instance in a query. `Display` is its user-facing
/// path; `target` is the data-model cube it instantiates.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CubeId(Rc<CubeIdKind>);

#[derive(PartialEq, Eq, Hash, PartialOrd, Ord)]
enum CubeIdKind {
    Cube(String),
}

impl CubeId {
    pub fn cube(name: impl Into<String>) -> Self {
        Self(Rc::new(CubeIdKind::Cube(name.into())))
    }

    /// Name of the data-model cube, for cube evaluator and base tools calls.
    pub fn target(&self) -> &str {
        match self.0.as_ref() {
            CubeIdKind::Cube(name) => name,
        }
    }
}

impl fmt::Display for CubeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0.as_ref() {
            CubeIdKind::Cube(name) => f.write_str(name),
        }
    }
}

impl fmt::Debug for CubeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.to_string(), f)
    }
}
