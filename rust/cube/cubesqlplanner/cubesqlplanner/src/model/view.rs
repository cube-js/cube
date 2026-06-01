use super::path::{CubeName, MemberPath};

/// View-only state on a `Cube` whose `is_view = true`.
///
/// Represents the **resolved** view: how members of underlying cubes
/// are surfaced through this view, plus join paths between the source
/// cubes. The build-time details (cubes/includes/excludes/prefix/split)
/// are out of scope — by the time we read the model they have already
/// been compiled into `included_members`.
#[derive(Clone)]
pub struct ViewSpec {
    /// Members surfaced by the view, after resolving includes/excludes.
    pub included_members: Vec<IncludedMember>,

    /// Join paths between underlying cubes, used by the planner to
    /// pick the right join chain for a member of the view.
    pub join_map: Vec<Vec<CubeName>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IncludedMemberKind {
    Measure,
    Dimension,
    Segment,
}

#[derive(Clone)]
pub struct IncludedMember {
    pub kind: IncludedMemberKind,
    /// Path to the source member on the underlying cube.
    pub source: MemberPath,
    /// Name as exposed by the view.
    pub name: String,
}
