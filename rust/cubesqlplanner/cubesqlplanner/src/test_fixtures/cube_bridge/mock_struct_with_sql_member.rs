use crate::cube_bridge::member_sql::MemberSql;
use crate::cube_bridge::struct_with_sql_member::StructWithSqlMember;
use crate::test_fixtures::cube_bridge::MockMemberSql;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;
use typed_builder::TypedBuilder;

/// Mock implementation of StructWithSqlMember for testing
#[derive(Debug, Clone, TypedBuilder)]
pub struct MockStructWithSqlMember {
    sql: String,
}

impl StructWithSqlMember for MockStructWithSqlMember {
    fn sql(&self) -> Result<Rc<dyn MemberSql>, CubeError> {
        Ok(Rc::new(MockMemberSql::new(&self.sql)?))
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_struct_with_sql_member() {
        let item = MockStructWithSqlMember::builder()
            .sql("{CUBE.field} > 0".to_string())
            .build();

        assert!(item.sql().is_ok());
        let sql = item.sql().unwrap();
        assert_eq!(sql.args_names(), &vec!["CUBE"]);
    }
}
