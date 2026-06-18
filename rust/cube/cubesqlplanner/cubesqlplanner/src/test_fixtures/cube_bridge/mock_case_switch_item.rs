use crate::cube_bridge::case_switch_item::{CaseSwitchItem, CaseSwitchItemStatic};
use crate::cube_bridge::member_sql::MemberSql;
use crate::impl_static_data;
use crate::test_fixtures::cube_bridge::MockMemberSql;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;
use typed_builder::TypedBuilder;

#[derive(Debug, Clone, TypedBuilder)]
pub struct MockCaseSwitchItem {
    value: String,
    sql: String,
}

impl_static_data!(MockCaseSwitchItem, CaseSwitchItemStatic, value);

impl CaseSwitchItem for MockCaseSwitchItem {
    crate::impl_static_data_method!(CaseSwitchItemStatic);

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
    fn test_mock_case_switch_item() {
        let item = MockCaseSwitchItem::builder()
            .value("1".to_string())
            .sql("{CUBE.active_sql}".to_string())
            .build();

        assert_eq!(item.static_data().value, "1");
        assert!(item.sql().is_ok());
    }
}
