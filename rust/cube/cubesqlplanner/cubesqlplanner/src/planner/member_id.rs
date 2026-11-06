use super::CubeId;
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::rc::Rc;

/// Identity of a member symbol. `full_name` is its rendered form.
#[derive(Clone)]
pub struct MemberId(Rc<MemberIdInner>);

struct MemberIdInner {
    kind: MemberIdKind,
    full_name: String,
}

#[derive(PartialEq, Eq, Hash, PartialOrd, Ord)]
enum MemberIdKind {
    Member { cube: CubeId, name: String },
    TimeDimension { base: MemberId, granularity: String },
    Expression { cube: CubeId, name: String },
}

impl MemberId {
    pub fn member(cube: CubeId, name: impl Into<String>) -> Self {
        let name = name.into();
        let full_name = format!("{}.{}", cube, name);
        Self::new(MemberIdKind::Member { cube, name }, full_name)
    }

    /// A time dimension without a granularity is identified as its `day` form.
    pub fn time_dimension(base: MemberId, granularity: Option<&str>) -> Self {
        let granularity = granularity.unwrap_or("day").to_string();
        let full_name = format!("{}_{}", base.full_name(), granularity);
        Self::new(MemberIdKind::TimeDimension { base, granularity }, full_name)
    }

    pub fn expression(cube: CubeId, name: impl Into<String>) -> Self {
        let name = name.into();
        let full_name = format!("expr:{}.{}", cube, name);
        Self::new(MemberIdKind::Expression { cube, name }, full_name)
    }

    fn new(kind: MemberIdKind, full_name: String) -> Self {
        Self(Rc::new(MemberIdInner { kind, full_name }))
    }

    pub fn cube(&self) -> &CubeId {
        match &self.0.kind {
            MemberIdKind::Member { cube, .. } | MemberIdKind::Expression { cube, .. } => cube,
            MemberIdKind::TimeDimension { base, .. } => base.cube(),
        }
    }

    /// The member a time dimension is built over; any other id is its own base.
    pub fn base(&self) -> &MemberId {
        match &self.0.kind {
            MemberIdKind::TimeDimension { base, .. } => base.base(),
            _ => self,
        }
    }

    /// Path of the member in the data model, for cube evaluator calls.
    pub fn target_path(&self) -> String {
        match &self.0.kind {
            MemberIdKind::Member { cube, name } | MemberIdKind::Expression { cube, name } => {
                format!("{}.{}", cube.target(), name)
            }
            MemberIdKind::TimeDimension { base, .. } => base.target_path(),
        }
    }

    pub fn full_name(&self) -> &String {
        &self.0.full_name
    }
}

impl PartialEq for MemberId {
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.0, &other.0) || self.0.kind == other.0.kind
    }
}

impl Eq for MemberId {}

impl Hash for MemberId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.kind.hash(state);
    }
}

impl PartialOrd for MemberId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MemberId {
    fn cmp(&self, other: &Self) -> Ordering {
        self.full_name()
            .cmp(other.full_name())
            .then_with(|| self.0.kind.cmp(&other.0.kind))
    }
}

impl fmt::Display for MemberId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.full_name())
    }
}

impl fmt::Debug for MemberId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.full_name(), f)
    }
}
