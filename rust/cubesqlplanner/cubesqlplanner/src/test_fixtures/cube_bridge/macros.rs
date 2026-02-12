/// Macro to implement static_data() helper method for mock bridge types
///
/// This macro generates a helper method that returns an owned StaticData struct.
/// The helper is used by the trait's static_data() method which applies Box::leak.
///
/// ```
#[macro_export]
macro_rules! impl_static_data {
    // Pattern: impl_static_data!(MockType, StaticType, field1, field2, ...)
    ($mock_type:ty, $static_type:path, $($field:ident),* $(,)?) => {
        // Helper method that returns owned StaticData
        impl $mock_type {
            pub fn static_data(&self) -> $static_type {
                $static_type {
                    $($field: self.$field.clone()),*
                }
            }
        }
    };
}

/// Macro to implement the trait's static_data() method using Box::leak
///
/// This macro should be used INSIDE the trait implementation block to generate
/// the static_data() method that returns &'static references.
///
/// # Memory Leak Explanation
/// This macro uses `Box::leak(Box::new(...))` to convert owned values into static
/// references. This intentionally leaks memory, which is acceptable because:
/// - Mock objects are only used in tests with short lifetimes
/// - Tests typically create a small number of mock objects
/// - The leaked memory is minimal and reclaimed when the test process exits
/// - This approach significantly simplifies test code by avoiding complex lifetime management
///
/// ```
#[macro_export]
macro_rules! impl_static_data_method {
    ($static_type:path) => {
        fn static_data(&self) -> &$static_type {
            // Intentional memory leak for test mocks - see macro documentation
            // This converts the owned StaticData from the helper method into a &'static reference
            // required by the trait. The leak is acceptable because:
            // 1. Test mocks have short lifetimes (duration of test)
            // 2. Small number of instances created
            // 3. Memory reclaimed when test process exits
            Box::leak(Box::new(Self::static_data(self)))
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

    pub trait TestTrait {
        fn static_data(&self) -> &TestStatic;
    }

    #[derive(TypedBuilder)]
    pub struct MockTest {
        #[builder(default = "test".to_string())]
        name: String,
        #[builder(default)]
        value: Option<i32>,
    }

    impl_static_data!(MockTest, TestStatic, name, value);

    impl TestTrait for MockTest {
        impl_static_data_method!(TestStatic);
    }

    #[test]
    fn test_static_data_helper_method() {
        let mock = MockTest::builder()
            .name("hello".to_string())
            .value(Some(42))
            .build();

        let static_data = mock.static_data();
        assert_eq!(static_data.name, "hello");
        assert_eq!(static_data.value, Some(42));
    }

    #[test]
    fn test_static_data_trait_method() {
        let mock = MockTest::builder()
            .name("world".to_string())
            .value(Some(123))
            .build();

        // Call trait method
        let static_data: &TestStatic = TestTrait::static_data(&mock);
        assert_eq!(static_data.name, "world");
        assert_eq!(static_data.value, Some(123));
    }

    #[test]
    fn test_static_data_macro_with_defaults() {
        let mock = MockTest::builder().build();

        let static_data = mock.static_data();
        assert_eq!(static_data.name, "test");
        assert_eq!(static_data.value, None);
    }
}
