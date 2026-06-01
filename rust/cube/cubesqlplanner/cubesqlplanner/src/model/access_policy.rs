use super::expression::Expression;
use super::path::MemberPath;

#[derive(Clone)]
pub struct AccessPolicy {
    pub role: Option<String>,
    pub group: Option<String>,
    pub groups: Vec<String>,

    pub conditions: Vec<AccessCondition>,
    pub row_level: Option<RowLevelAccess>,
    pub member_level: Option<MemberLevelAccess>,
    pub member_masking: Option<MemberMasking>,
}

#[derive(Clone)]
pub struct AccessCondition {
    /// `if` callback — evaluated against request context to decide
    /// whether the policy applies to a given query.
    pub predicate: Expression,
}

#[derive(Clone)]
pub struct RowLevelAccess {
    pub filters: Vec<AccessFilter>,
}

#[derive(Clone)]
pub enum AccessFilter {
    Member {
        member: MemberPath,
        operator: String,
        values: Vec<String>,
    },
    And(Vec<AccessFilter>),
    Or(Vec<AccessFilter>),
}

/// Resolved member-level access. `includes` and `excludes` are the
/// `includesMembers` / `excludesMembers` arrays the schema-compiler
/// builds — `'*'` is already expanded to a concrete member list and
/// the missing field becomes an empty vec, so callers don't have to
/// re-resolve the "all" wildcard.
#[derive(Clone)]
pub struct MemberLevelAccess {
    pub includes: Vec<MemberPath>,
    pub excludes: Vec<MemberPath>,
}

#[derive(Clone)]
pub struct MemberMasking {
    pub includes: Vec<MemberPath>,
    pub excludes: Vec<MemberPath>,
}
