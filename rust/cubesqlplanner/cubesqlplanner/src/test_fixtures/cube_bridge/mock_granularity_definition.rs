use crate::cube_bridge::granularity_definition::{
    GranularityDefinition, GranularityDefinitionStatic,
};
use crate::cube_bridge::member_sql::MemberSql;
use crate::impl_static_data;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;
use typed_builder::TypedBuilder;

#[derive(Clone, TypedBuilder)]
pub struct MockGranularityDefinition {
    #[builder(setter(into))]
    interval: String,
    #[builder(default, setter(strip_option(fallback = origin_opt), into))]
    origin: Option<String>,
    #[builder(default, setter(strip_option(fallback = offset_opt), into))]
    offset: Option<String>,
    #[builder(default, setter(strip_option))]
    sql: Option<Rc<dyn MemberSql>>,
}

impl_static_data!(
    MockGranularityDefinition,
    GranularityDefinitionStatic,
    interval,
    origin,
    offset
);

impl GranularityDefinition for MockGranularityDefinition {
    crate::impl_static_data_method!(GranularityDefinitionStatic);

    fn sql(&self) -> Result<Option<Rc<dyn MemberSql>>, CubeError> {
        Ok(self.sql.clone())
    }

    fn has_sql(&self) -> Result<bool, CubeError> {
        Ok(self.sql.is_some())
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_granularity_basic() {
        let granularity = MockGranularityDefinition::builder()
            .interval("month")
            .build();

        let static_data = granularity.static_data();
        assert_eq!(static_data.interval, "month");
        assert_eq!(static_data.origin, None);
        assert_eq!(static_data.offset, None);
        assert!(granularity.sql().unwrap().is_none());
    }

    #[test]
    fn test_mock_granularity_with_origin_and_offset() {
        let granularity = MockGranularityDefinition::builder()
            .interval("week")
            .origin("2020-01-01")
            .offset("3 days")
            .build();

        let static_data = granularity.static_data();
        assert_eq!(static_data.interval, "week");
        assert_eq!(static_data.origin, Some("2020-01-01".to_string()));
        assert_eq!(static_data.offset, Some("3 days".to_string()));
    }
}
