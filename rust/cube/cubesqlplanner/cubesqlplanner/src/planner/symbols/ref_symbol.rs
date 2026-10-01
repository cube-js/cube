use super::common::CompiledMemberPath;
use super::deps::{self, symbol_deps, DepVisitor, DepVisitorMut, SymbolDeps};
use super::MemberSymbol;
use crate::planner::SqlCall;
use crate::utils::debug::DebugSql;
use cubenativeutils::CubeError;
use std::ops::ControlFlow;
use std::rc::Rc;

/// What a reference stands for.
#[derive(Clone)]
pub enum RefTarget {
    /// Another member, rendered in place of the reference.
    Member(Rc<MemberSymbol>),
}

impl SymbolDeps for RefTarget {
    fn visit_deps(&self, visitor: &mut dyn DepVisitor) -> ControlFlow<()> {
        match self {
            Self::Member(member) => visitor.symbol(member),
        }
    }

    fn visit_deps_mut(&mut self, visitor: &mut dyn DepVisitorMut) -> Result<(), CubeError> {
        match self {
            Self::Member(member) => visitor.symbol(member),
        }
    }
}

/// `MemberSymbol::Ref` body: a member with an identity of its own — a view
/// member — whose value is its target's. It computes nothing: it renders the
/// target under its own name, optionally masked by its own mask.
#[derive(Clone)]
pub struct RefSymbol {
    pub(super) compiled_path: CompiledMemberPath,
    pub(super) target: RefTarget,
    pub(super) mask_sql: Option<Rc<SqlCall>>,
}

symbol_deps! {
    RefSymbol {
        target: dep,
        mask_sql: dep,
        compiled_path: skip,
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
        }))
    }

    pub fn compiled_path(&self) -> &CompiledMemberPath {
        &self.compiled_path
    }

    pub fn full_name(&self) -> String {
        self.compiled_path.full_name().clone()
    }

    pub fn target(&self) -> &RefTarget {
        &self.target
    }

    pub fn target_member(&self) -> Option<&Rc<MemberSymbol>> {
        match &self.target {
            RefTarget::Member(member) => Some(member),
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
        }
    }
}
