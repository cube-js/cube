use super::expression::Expression;
use super::path::MemberPath;

#[derive(Clone)]
pub struct Segment {
    pub path: MemberPath,
    pub sql: Expression,
    pub owned_by_cube: bool,
}
