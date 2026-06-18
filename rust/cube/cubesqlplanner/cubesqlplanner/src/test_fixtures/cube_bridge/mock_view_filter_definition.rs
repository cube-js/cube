use crate::cube_bridge::view_filter_definition::{
    ViewFilterDefinition, ViewFilterDefinitionStatic,
};
use crate::impl_static_data;
use std::any::Any;
use std::rc::Rc;
use typed_builder::TypedBuilder;

#[derive(Clone, Debug, TypedBuilder)]
pub struct MockViewFilterDefinition {
    operator: String,
    member_reference: String,
    #[builder(default)]
    values_references: Option<Vec<Option<String>>>,
    #[builder(default)]
    unless_references: Option<Vec<String>>,
}

impl_static_data!(
    MockViewFilterDefinition,
    ViewFilterDefinitionStatic,
    operator,
    member_reference,
    values_references,
    unless_references
);

impl ViewFilterDefinition for MockViewFilterDefinition {
    crate::impl_static_data_method!(ViewFilterDefinitionStatic);

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_minimal_filter() {
        let filter = MockViewFilterDefinition::builder()
            .operator("set".to_string())
            .member_reference("orders.currency".to_string())
            .build();

        let data = filter.static_data();
        assert_eq!(data.operator, "set");
        assert_eq!(data.member_reference, "orders.currency");
        assert!(data.values_references.is_none());
        assert!(data.unless_references.is_none());
    }

    #[test]
    fn test_filter_with_values_and_unless() {
        let filter = MockViewFilterDefinition::builder()
            .operator("equals".to_string())
            .member_reference("orders.currency".to_string())
            .values_references(Some(vec![Some("USD".to_string())]))
            .unless_references(Some(vec![
                "orders.currency".to_string(),
                "orders.country".to_string(),
            ]))
            .build();

        let data = filter.static_data();
        assert_eq!(data.operator, "equals");
        assert_eq!(data.member_reference, "orders.currency");
        assert_eq!(
            data.values_references.as_ref().unwrap(),
            &vec![Some("USD".to_string())]
        );
        assert_eq!(
            data.unless_references.as_ref().unwrap(),
            &vec!["orders.currency".to_string(), "orders.country".to_string()]
        );
    }

    #[test]
    fn test_filter_values_keep_nulls() {
        let filter = MockViewFilterDefinition::builder()
            .operator("in".to_string())
            .member_reference("orders.status".to_string())
            .values_references(Some(vec![
                Some("draft".to_string()),
                Some("paid".to_string()),
                None,
            ]))
            .build();

        let values = filter.static_data().values_references.unwrap();
        assert_eq!(values.len(), 3);
        assert_eq!(values[0], Some("draft".to_string()));
        assert_eq!(values[1], Some("paid".to_string()));
        assert_eq!(values[2], None);
    }
}
