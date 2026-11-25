use super::common::CompiledMemberPath;
use super::deps::{self, symbol_deps, DepVisitor, DepVisitorMut, SymbolDeps};
use super::MemberSymbol;
use crate::planner::{MemberId, SqlCall};
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

/// `MemberSymbol::Ref` body: computes nothing. A `Member` target is a view
/// member rendering its target under its own name (optionally masked); a
/// terminal target takes its origin's identity and renders the column/value.
#[derive(Clone)]
pub struct RefSymbol {
    pub(super) compiled_path: CompiledMemberPath,
    pub(super) target: RefTarget,
    pub(super) mask_sql: Option<Rc<SqlCall>>,
    pub(super) dimension_overrides: Vec<DimensionOverride>,
}

/// A view dimension that means another dimension for the view measure that
/// declares it.
#[derive(Clone)]
pub struct DimensionOverride {
    /// The view dimension's own id, not the id of the member it references.
    pub dimension: MemberId,
    pub target: Rc<MemberSymbol>,
}

symbol_deps! {
    RefSymbol {
        target: dep,
        mask_sql: dep,
        compiled_path: skip,
        // Not dependencies: a measure joins an override's target only when the
        // query uses the view dimension it overrides.
        dimension_overrides: skip,
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
            dimension_overrides: vec![],
        }))
    }

    pub fn with_dimension_overrides(
        &self,
        dimension_overrides: Vec<DimensionOverride>,
    ) -> Rc<Self> {
        Rc::new(Self {
            dimension_overrides,
            ..self.clone()
        })
    }

    /// The same member, under its own name and mask, standing for `target`.
    pub fn with_target(&self, target: Rc<MemberSymbol>) -> Rc<Self> {
        Rc::new(Self {
            target: RefTarget::Member(target),
            ..self.clone()
        })
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
            dimension_overrides: vec![],
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
            dimension_overrides: vec![],
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

    pub fn dimension_overrides(&self) -> &[DimensionOverride] {
        &self.dimension_overrides
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
