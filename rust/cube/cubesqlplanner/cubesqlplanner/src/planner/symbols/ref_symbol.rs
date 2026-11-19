use super::common::CompiledMemberPath;
use super::deps::{self, symbol_deps, DepVisitor, DepVisitorMut, SymbolDeps};
use super::MemberSymbol;
use crate::planner::SqlCall;
use crate::utils::debug::DebugSql;
use cubenativeutils::CubeError;
use std::ops::ControlFlow;
use std::rc::Rc;

/// What a reference stands for.
///
/// `Column` and `Literal` are terminal. Their `origin` is not a dependency —
/// its SQL is what the reference replaces — but stays the reference member.
#[derive(Clone)]
pub enum RefTarget {
    /// Another member, rendered in place of the reference.
    Member(Rc<MemberSymbol>),
    /// A column of one of the enclosing select's sources.
    Column {
        source: Option<String>,
        name: String,
        origin: Rc<MemberSymbol>,
    },
    /// A value rendered as a string literal.
    Literal {
        value: String,
        origin: Rc<MemberSymbol>,
    },
}

impl SymbolDeps for RefTarget {
    fn visit_deps(&self, visitor: &mut dyn DepVisitor) -> ControlFlow<()> {
        match self {
            Self::Member(member) => visitor.symbol(member),
            Self::Column { .. } | Self::Literal { .. } => ControlFlow::Continue(()),
        }
    }

    fn visit_deps_mut(&mut self, visitor: &mut dyn DepVisitorMut) -> Result<(), CubeError> {
        match self {
            Self::Member(member) => visitor.symbol(member),
            Self::Column { .. } | Self::Literal { .. } => Ok(()),
        }
    }
}

/// One way to read a view dimension included through `join_paths`: the
/// member the dimension stands for along one of the paths.
#[derive(Clone)]
pub struct SharedTarget {
    /// The path as the view writes it (`orders.event_day`).
    pub path: String,
    /// The cube the path starts at.
    pub root: String,
    pub target: Rc<MemberSymbol>,
    /// The dimension's mask, compiled against the cube this path reaches.
    pub mask_sql: Option<Rc<SqlCall>>,
}

/// `MemberSymbol::Ref` body: computes nothing. A `Member` target is a view
/// member rendering its target under its own name (optionally masked); a
/// terminal target takes its origin's identity and renders the column/value.
#[derive(Clone)]
pub struct RefSymbol {
    pub(super) compiled_path: CompiledMemberPath,
    pub(super) target: RefTarget,
    pub(super) mask_sql: Option<Rc<SqlCall>>,
    /// Set on a view dimension included through `join_paths`: every member it
    /// may stand for, one per path, the first being `target`. Not dependencies:
    /// a query is bound to one of them (`bind_shared`) before it is planned.
    pub(super) shared: Option<Rc<Vec<SharedTarget>>>,
    /// Bound from a shared view dimension to one of its paths: it keeps the
    /// shared dimension's identity, which legs reading it through different
    /// paths are merged by.
    pub(super) bound_shared: bool,
}

symbol_deps! {
    RefSymbol {
        target: dep,
        mask_sql: dep,
        compiled_path: skip,
        shared: skip,
        bound_shared: skip,
    }
}

impl RefSymbol {
    /// Builds a reference from the member's SQL, which must be a direct
    /// reference to exactly one other member.
    pub fn try_new(
        compiled_path: CompiledMemberPath,
        sql: &SqlCall,
        mask_sql: Option<Rc<SqlCall>>,
    ) -> Result<Rc<Self>, CubeError> {
        let target = sql.resolve_direct_reference().ok_or_else(|| {
            CubeError::internal(format!(
                "Member '{}' is not a direct reference to another member",
                compiled_path.full_name()
            ))
        })?;
        Ok(Rc::new(Self {
            compiled_path,
            target: RefTarget::Member(target),
            mask_sql,
            shared: None,
            bound_shared: false,
        }))
    }

    /// This reference as a view dimension shared through `join_paths`, standing
    /// for `targets` - the first of which must be its current target.
    pub fn with_shared_targets(&self, targets: Vec<SharedTarget>) -> Rc<Self> {
        Rc::new(Self {
            shared: Some(Rc::new(targets)),
            ..self.clone()
        })
    }

    /// The members a view dimension included through `join_paths` may stand
    /// for, one per path in declared order.
    pub fn shared_targets(&self) -> Option<&[SharedTarget]> {
        self.shared.as_ref().map(|targets| targets.as_slice())
    }

    pub fn is_shared(&self) -> bool {
        self.shared.is_some()
    }

    /// A shared view dimension, or one bound to a path: either way, matched
    /// across subqueries by its own id rather than by the member it reads.
    pub fn has_shared_identity(&self) -> bool {
        self.is_shared() || self.bound_shared
    }

    /// The plain reference to the member of the `index`-th shared path, under
    /// this dimension's own name.
    pub fn bind_shared(&self, index: usize) -> Result<Rc<Self>, CubeError> {
        let shared = self.shared_targets().unwrap_or_default();
        let bound = shared.get(index).ok_or_else(|| {
            CubeError::internal(format!(
                "Member '{}' has no shared path {}",
                self.full_name(),
                index
            ))
        })?;
        Ok(Rc::new(Self {
            compiled_path: self.compiled_path.clone(),
            target: RefTarget::Member(bound.target.clone()),
            mask_sql: bound.mask_sql.clone(),
            shared: None,
            bound_shared: true,
        }))
    }

    /// A reference reading `origin` from `name` of `source`.
    pub fn new_column(origin: &Rc<MemberSymbol>, source: Option<String>, name: String) -> Rc<Self> {
        Rc::new(Self {
            compiled_path: origin.compiled_path().clone(),
            target: RefTarget::Column {
                source,
                name,
                origin: origin.clone(),
            },
            mask_sql: None,
            shared: None,
            bound_shared: false,
        })
    }

    /// A reference rendering `origin` as the string literal `value`.
    pub fn new_literal(origin: &Rc<MemberSymbol>, value: String) -> Rc<Self> {
        Rc::new(Self {
            compiled_path: origin.compiled_path().clone(),
            target: RefTarget::Literal {
                value,
                origin: origin.clone(),
            },
            mask_sql: None,
            shared: None,
            bound_shared: false,
        })
    }

    pub fn compiled_path(&self) -> &CompiledMemberPath {
        &self.compiled_path
    }

    pub fn full_name(&self) -> String {
        self.compiled_path.full_name().clone()
    }

    pub fn id(&self) -> &crate::planner::MemberId {
        self.compiled_path.id()
    }

    pub fn target(&self) -> &RefTarget {
        &self.target
    }

    pub fn target_member(&self) -> Option<&Rc<MemberSymbol>> {
        match &self.target {
            RefTarget::Member(member) => Some(member),
            RefTarget::Column { .. } | RefTarget::Literal { .. } => None,
        }
    }

    /// True for a `Column` or `Literal` target.
    pub fn is_terminal(&self) -> bool {
        self.target_member().is_none()
    }

    /// The member this reference stands for: the target member, or the
    /// origin of a terminal target.
    pub fn referenced_member(&self) -> &Rc<MemberSymbol> {
        match &self.target {
            RefTarget::Member(member) => member,
            RefTarget::Column { origin, .. } | RefTarget::Literal { origin, .. } => origin,
        }
    }

    pub fn mask_sql(&self) -> &Option<Rc<SqlCall>> {
        &self.mask_sql
    }

    pub fn get_dependencies(&self) -> Vec<Rc<MemberSymbol>> {
        deps::collect_deps(self)
    }
}

impl DebugSql for RefSymbol {
    fn debug_sql(&self, expand_deps: bool) -> String {
        match &self.target {
            RefTarget::Member(member) => {
                if expand_deps {
                    member.debug_sql(true)
                } else {
                    format!("{{{}}}", member.full_name())
                }
            }
            RefTarget::Column { source, name, .. } => match source {
                Some(source) => format!("{{REF:{}.{}}}", source, name),
                None => format!("{{REF:{}}}", name),
            },
            RefTarget::Literal { value, .. } => format!("{{REF:'{}'}}", value),
        }
    }
}
