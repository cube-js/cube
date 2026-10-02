use std::fmt;
use std::rc::Rc;

/// Identity of a cube instance in a query. `Display` is its user-facing
/// path; `target` is the data-model cube it instantiates.
///
/// A data-model cube is one instance of itself. A cube reached through a
/// join that has to stay distinct from other routes to it — an aliased
/// join, or any join below one — is an instance of its own, identified by
/// the instance it is joined from and the name of that join.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CubeId(Rc<CubeIdKind>);

#[derive(PartialEq, Eq, Hash, PartialOrd, Ord)]
enum CubeIdKind {
    Cube(String),
    Joined {
        parent: CubeId,
        join: String,
        target: String,
    },
}

impl CubeId {
    pub fn cube(name: impl Into<String>) -> Self {
        Self(Rc::new(CubeIdKind::Cube(name.into())))
    }

    /// The instance of `target` reached from `parent` through the join
    /// `parent` knows as `join` — its alias, or the joined cube name.
    pub fn joined(parent: CubeId, join: impl Into<String>, target: impl Into<String>) -> Self {
        Self(Rc::new(CubeIdKind::Joined {
            parent,
            join: join.into(),
            target: target.into(),
        }))
    }

    /// Name of the data-model cube, for cube evaluator and base tools calls.
    pub fn target(&self) -> &str {
        match self.0.as_ref() {
            CubeIdKind::Cube(name) => name,
            CubeIdKind::Joined { target, .. } => target,
        }
    }

    pub fn is_joined(&self) -> bool {
        matches!(self.0.as_ref(), CubeIdKind::Joined { .. })
    }

    pub fn parent(&self) -> Option<&CubeId> {
        match self.0.as_ref() {
            CubeIdKind::Cube(_) => None,
            CubeIdKind::Joined { parent, .. } => Some(parent),
        }
    }

    /// The data-model cube the chain of joins starts from.
    pub fn root(&self) -> &CubeId {
        match self.0.as_ref() {
            CubeIdKind::Cube(_) => self,
            CubeIdKind::Joined { parent, .. } => parent.root(),
        }
    }

    /// The last segment of the user-facing path: the cube name, or the name
    /// of the join this instance is reached through.
    pub fn segment(&self) -> &str {
        match self.0.as_ref() {
            CubeIdKind::Cube(name) => name,
            CubeIdKind::Joined { join, .. } => join,
        }
    }

    /// The joined instances from the root down to this one, root excluded.
    pub fn joined_chain(&self) -> Vec<CubeId> {
        let mut chain = vec![];
        let mut current = self;
        while let Some(parent) = current.parent() {
            chain.push(current.clone());
            current = parent;
        }
        chain.reverse();
        chain
    }
}

impl fmt::Display for CubeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0.as_ref() {
            CubeIdKind::Cube(name) => f.write_str(name),
            CubeIdKind::Joined { parent, join, .. } => write!(f, "{}.{}", parent, join),
        }
    }
}

impl fmt::Debug for CubeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.to_string(), f)
    }
}
