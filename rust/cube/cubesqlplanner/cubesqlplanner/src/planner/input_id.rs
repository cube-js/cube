use std::fmt;

/// Identity of a plan subquery that other plan nodes read by. Unlike a
/// subquery name it is never rendered.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct InputId(usize);

impl InputId {
    pub fn new(value: usize) -> Self {
        Self(value)
    }
}

impl fmt::Display for InputId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{}", self.0)
    }
}
