use crate::cube_bridge::member_sql::MemberSql;
use std::fmt;
use std::rc::Rc;

/// Compiled SQL expression for a model member.
///
/// First-iteration shape: stores the raw bridge `MemberSql` reference
/// produced by the schema-compiler. The intended final shape is an enum
/// distinguishing `Eager(SqlCall)` (compiled once when the source uses no
/// per-request context) from `Lazy(MemberSql)` (compiled per request,
/// because the source reads FILTER_PARAMS / FILTER_GROUP / SECURITY_CONTEXT).
/// Classification by `MemberSql::args_names` and the eager pre-compile
/// step will land in a follow-up iteration.
#[derive(Clone)]
pub struct Expression {
    source: Rc<dyn MemberSql>,
}

impl Expression {
    pub fn new(source: Rc<dyn MemberSql>) -> Self {
        Self { source }
    }

    pub fn source(&self) -> &Rc<dyn MemberSql> {
        &self.source
    }

    pub fn args_names(&self) -> &Vec<String> {
        self.source.args_names()
    }

    /// Whether the source signature includes any per-request placeholder
    /// (FILTER_PARAMS / FILTER_GROUP / SECURITY_CONTEXT and aliases). This
    /// is the classifier that will gate eager pre-compilation later.
    pub fn is_lazy(&self) -> bool {
        self.args_names().iter().any(|a| {
            matches!(
                a.as_str(),
                "FILTER_PARAMS"
                    | "FILTER_GROUP"
                    | "SECURITY_CONTEXT"
                    | "security_context"
                    | "securityContext"
            )
        })
    }
}

impl fmt::Debug for Expression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Expression")
            .field("args_names", self.args_names())
            .finish()
    }
}
