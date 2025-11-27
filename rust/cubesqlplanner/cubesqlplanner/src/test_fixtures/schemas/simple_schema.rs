use crate::test_fixtures::cube_bridge::{
    MockCubeDefinition, MockDimensionDefinition, MockJoinItemDefinition, MockMeasureDefinition,
    MockSchema, MockSchemaBuilder,
};

/// Creates a simple schema with orders and customers cubes
///
/// This schema demonstrates:
/// - Two cubes with basic dimensions and measures
/// - Single many-to-one join from orders to customers
/// - Standard measure types (count, max, min)
pub fn create_simple_schema() -> MockSchema {
    MockSchemaBuilder::new()
        // customers cube
        .add_cube("customers")
        .cube_def(
            MockCubeDefinition::builder()
                .name("customers".to_string())
                .sql("SELECT * FROM customers".to_string())
                .build(),
        )
        .add_dimension(
            "id",
            MockDimensionDefinition::builder()
                .dimension_type("number".to_string())
                .sql("id".to_string())
                .primary_key(Some(true))
                .build(),
        )
        .add_dimension(
            "name",
            MockDimensionDefinition::builder()
                .dimension_type("string".to_string())
                .sql("name".to_string())
                .build(),
        )
        .add_dimension(
            "city",
            MockDimensionDefinition::builder()
                .dimension_type("string".to_string())
                .sql("city".to_string())
                .build(),
        )
        .add_dimension(
            "created_at",
            MockDimensionDefinition::builder()
                .dimension_type("time".to_string())
                .sql("created_at".to_string())
                .build(),
        )
        .add_measure(
            "count",
            MockMeasureDefinition::builder()
                .measure_type("count".to_string())
                .sql("COUNT(*)".to_string())
                .build(),
        )
        .add_measure(
            "max_age",
            MockMeasureDefinition::builder()
                .measure_type("max".to_string())
                .sql("age".to_string())
                .build(),
        )
        .add_measure(
            "min_age",
            MockMeasureDefinition::builder()
                .measure_type("min".to_string())
                .sql("age".to_string())
                .build(),
        )
        .finish_cube()
        // orders cube
        .add_cube("orders")
        .cube_def(
            MockCubeDefinition::builder()
                .name("orders".to_string())
                .sql("SELECT * FROM orders".to_string())
                .build(),
        )
        .add_dimension(
            "id",
            MockDimensionDefinition::builder()
                .dimension_type("number".to_string())
                .sql("id".to_string())
                .primary_key(Some(true))
                .build(),
        )
        .add_dimension(
            "status",
            MockDimensionDefinition::builder()
                .dimension_type("string".to_string())
                .sql("status".to_string())
                .build(),
        )
        .add_dimension(
            "priority",
            MockDimensionDefinition::builder()
                .dimension_type("string".to_string())
                .sql("priority".to_string())
                .build(),
        )
        .add_dimension(
            "created_at",
            MockDimensionDefinition::builder()
                .dimension_type("time".to_string())
                .sql("created_at".to_string())
                .build(),
        )
        .add_measure(
            "count",
            MockMeasureDefinition::builder()
                .measure_type("count".to_string())
                .sql("COUNT(*)".to_string())
                .build(),
        )
        .add_measure(
            "max_amount",
            MockMeasureDefinition::builder()
                .measure_type("max".to_string())
                .sql("amount".to_string())
                .build(),
        )
        .add_measure(
            "min_amount",
            MockMeasureDefinition::builder()
                .measure_type("min".to_string())
                .sql("amount".to_string())
                .build(),
        )
        .add_join(
            "customers",
            MockJoinItemDefinition::builder()
                .relationship("many_to_one".to_string())
                .sql("{orders}.customer_id = {customers.id}".to_string())
                .build(),
        )
        .finish_cube()
        .build()
}
