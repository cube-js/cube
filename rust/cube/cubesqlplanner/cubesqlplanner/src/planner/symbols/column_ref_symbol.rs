use super::common::CompiledMemberPath;
use super::deps::symbol_deps;
use super::MemberSymbol;
use crate::utils::debug::DebugSql;
use std::rc::Rc;

/// What a reference stands for in the SQL it renders into: a column of
/// one of the enclosing select's sources, or a value the query pinned
/// for the member.
#[derive(Clone, Debug)]
pub enum ReferenceValue {
    Column {
        source: Option<String>,
        name: String,
    },
    Literal(String),
}

/// `MemberSymbol::ColumnRef` body: a member that is *read* instead of
/// computed, because the select it belongs to takes it from a source
/// that already produced it.
///
/// A reference is a leaf: it renders as the column or literal it names
/// and nothing else. Anything that would still apply to the member —
/// an aggregation, a granularity truncation, a mask — is expressed by
/// a symbol standing above this one in the dependency tree, never by
/// the reference itself.
///
/// `origin` is the member the reference stands for. It carries the
/// symbol's identity — `full_name`, `alias`, `cube_name` all come from
/// it, so column resolution, member-position lookup and filter
/// matching keep working on a substituted symbol. It is deliberately
/// not a dependency: the origin's SQL is precisely what this symbol
/// replaces.
#[derive(Clone)]
pub struct ColumnRefSymbol {
    pub(super) value: ReferenceValue,
    pub(super) origin: Rc<MemberSymbol>,
}

symbol_deps! {
    ColumnRefSymbol {
        value: skip,
        origin: skip,
    }
}

impl ColumnRefSymbol {
    pub fn new(origin: Rc<MemberSymbol>, value: ReferenceValue) -> Rc<Self> {
        Rc::new(Self { value, origin })
    }

    pub fn value(&self) -> &ReferenceValue {
        &self.value
    }

    pub fn origin(&self) -> &Rc<MemberSymbol> {
        &self.origin
    }

    pub fn compiled_path(&self) -> &CompiledMemberPath {
        self.origin.compiled_path()
    }

    pub fn full_name(&self) -> String {
        self.compiled_path().full_name().clone()
    }

    pub fn alias(&self) -> String {
        self.compiled_path().alias().clone()
    }

    pub fn name(&self) -> String {
        self.compiled_path().name().clone()
    }

    pub fn cube_name(&self) -> String {
        self.compiled_path().cube_name().clone()
    }
}

impl DebugSql for ColumnRefSymbol {
    fn debug_sql(&self, _expand_deps: bool) -> String {
        match &self.value {
            ReferenceValue::Column { source, name } => match source {
                Some(source) => format!("{{REF:{}.{}}}", source, name),
                None => format!("{{REF:{}}}", name),
            },
            ReferenceValue::Literal(value) => format!("{{REF:'{}'}}", value),
        }
    }
}
