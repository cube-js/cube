mod test_context;

#[cfg(feature = "integration-cubestore")]
pub(crate) mod cubestore_service;
#[cfg(feature = "integration-postgres")]
pub(crate) mod integration_context;
#[cfg(feature = "integration-postgres")]
pub(crate) mod pg_service;

pub use test_context::TestContext;

use crate::planner::{CubeId, MemberId};

/// The id a member renders as `cube.member`, or `expr:cube.member` for an
/// expression.
pub fn member_id(full_name: &str) -> MemberId {
    if let Some(expression) = full_name.strip_prefix("expr:") {
        let (cube, name) = expression.split_once('.').unwrap();
        MemberId::expression(CubeId::cube(cube), name)
    } else {
        let (cube, name) = full_name.split_once('.').unwrap();
        MemberId::member(CubeId::cube(cube), name)
    }
}
