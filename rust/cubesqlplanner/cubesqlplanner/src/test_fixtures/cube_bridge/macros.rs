/// Macro to implement static_data() method for mock bridge types
///
/// # Usage
/// ```ignore
/// impl_static_data!(
///     MockDimensionDefinition,
///     DimensionDefinitionStatic,
///     dimension_type,
///     owned_by_cube,
///     multi_stage
/// );
/// ```
///
/// This generates a method that creates StaticData struct on the fly from struct fields
#[macro_export]
macro_rules! impl_static_data {
    // Pattern: impl_static_data!(MockType, StaticType, field1, field2, ...)
    ($mock_type:ty, $static_type:path, $($field:ident),* $(,)?) => {
        impl $mock_type {
            pub fn static_data(&self) -> $static_type {
                $static_type {
                    $($field: self.$field.clone()),*
                }
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};
    use typed_builder::TypedBuilder;

    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
    pub struct TestStatic {
        pub name: String,
        pub value: Option<i32>,
    }

    #[derive(TypedBuilder)]
    pub struct MockTest {
        #[builder(default = "test".to_string())]
        name: String,
        #[builder(default)]
        value: Option<i32>,
    }

    impl_static_data!(MockTest, TestStatic, name, value);

    #[test]
    fn test_static_data_macro() {
        let mock = MockTest::builder()
            .name("hello".to_string())
            .value(Some(42))
            .build();

        let static_data = mock.static_data();
        assert_eq!(static_data.name, "hello");
        assert_eq!(static_data.value, Some(42));
    }

    #[test]
    fn test_static_data_macro_with_defaults() {
        let mock = MockTest::builder().build();

        let static_data = mock.static_data();
        assert_eq!(static_data.name, "test");
        assert_eq!(static_data.value, None);
    }
}