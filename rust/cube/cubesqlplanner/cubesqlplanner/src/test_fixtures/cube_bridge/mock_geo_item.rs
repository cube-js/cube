use crate::cube_bridge::geo_item::GeoItem;
use crate::cube_bridge::member_sql::MemberSql;
use crate::test_fixtures::cube_bridge::MockMemberSql;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;
use typed_builder::TypedBuilder;

#[derive(Debug, TypedBuilder)]
pub struct MockGeoItem {
    sql: String,
}

impl GeoItem for MockGeoItem {
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
    fn test_mock_geo_item() {
        let geo_item = MockGeoItem::builder()
            .sql("{CUBE.latitude}".to_string())
            .build();
        let sql = geo_item.sql().unwrap();
        assert_eq!(sql.args_names(), &vec!["CUBE"]);
    }
}
