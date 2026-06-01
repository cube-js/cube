use super::path::MemberPath;

/// Hierarchy: ordered list of dimensions on a cube.
#[derive(Clone)]
pub struct Hierarchy {
    pub name: String,
    pub levels: Vec<MemberPath>,

    /// View-only: the canonical "Cube.hierarchyName" reference under
    /// which this hierarchy was originally defined.
    pub alias_member: Option<String>,
}
