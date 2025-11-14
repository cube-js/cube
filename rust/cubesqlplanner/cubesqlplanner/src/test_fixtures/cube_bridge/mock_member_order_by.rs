use crate::cube_bridge::member_order_by::MemberOrderBy;
use crate::cube_bridge::member_sql::MemberSql;
use crate::test_fixtures::cube_bridge::MockMemberSql;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;
use typed_builder::TypedBuilder;

/// Mock implementation of MemberOrderBy for testing
#[derive(Debug, Clone, TypedBuilder)]
pub struct MockMemberOrderBy {
    sql: String,
    #[builder(default = "asc".to_string())]
    dir: String,
}

impl MemberOrderBy for MockMemberOrderBy {
    fn sql(&self) -> Result<Rc<dyn MemberSql>, CubeError> {
        Ok(Rc::new(MockMemberSql::new(&self.sql)?))
    }

    fn dir(&self) -> Result<String, CubeError> {
        Ok(self.dir.clone())
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_member_order_by() {
        let order = MockMemberOrderBy::builder()
            .sql("{CUBE.created_at}".to_string())
            .dir("desc".to_string())
            .build();

        assert!(order.sql().is_ok());
        assert_eq!(order.dir().unwrap(), "desc");
    }

    #[test]
    fn test_mock_member_order_by_default_dir() {
        let order = MockMemberOrderBy::builder()
            .sql("{CUBE.name}".to_string())
            .build();

        assert_eq!(order.dir().unwrap(), "asc");
    }
}
