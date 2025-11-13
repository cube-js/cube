use crate::test_fixtures::cube_bridge::{
    MockCubeDefinition, MockCubeEvaluator, MockDimensionDefinition, MockMeasureDefinition,
    MockSecurityContext, MockSegmentDefinition, MockSqlUtils,
};
use std::collections::HashMap;
use std::rc::Rc;

/// Mock schema containing cubes with their measures and dimensions
pub struct MockSchema {
    cubes: HashMap<String, MockCube>,
}

/// Single cube with its definition and members
pub struct MockCube {
    pub definition: MockCubeDefinition,
    pub measures: HashMap<String, Rc<MockMeasureDefinition>>,
    pub dimensions: HashMap<String, Rc<MockDimensionDefinition>>,
    pub segments: HashMap<String, Rc<MockSegmentDefinition>>,
}

impl MockSchema {
    /// Get cube by name
    pub fn get_cube(&self, name: &str) -> Option<&MockCube> {
        self.cubes.get(name)
    }

    /// Get dimension by cube name and dimension name
    pub fn get_dimension(
        &self,
        cube_name: &str,
        dimension_name: &str,
    ) -> Option<Rc<MockDimensionDefinition>> {
        self.cubes
            .get(cube_name)
            .and_then(|cube| cube.dimensions.get(dimension_name).cloned())
    }

    /// Get measure by cube name and measure name
    pub fn get_measure(
        &self,
        cube_name: &str,
        measure_name: &str,
    ) -> Option<Rc<MockMeasureDefinition>> {
        self.cubes
            .get(cube_name)
            .and_then(|cube| cube.measures.get(measure_name).cloned())
    }

    /// Get segment by cube name and segment name
    pub fn get_segment(
        &self,
        cube_name: &str,
        segment_name: &str,
    ) -> Option<Rc<MockSegmentDefinition>> {
        self.cubes
            .get(cube_name)
            .and_then(|cube| cube.segments.get(segment_name).cloned())
    }

    /// Get all cube names
    pub fn cube_names(&self) -> Vec<&String> {
        self.cubes.keys().collect()
    }

    /// Create a MockCubeEvaluator from this schema
    pub fn create_evaluator(self) -> Rc<MockCubeEvaluator> {
        Rc::new(MockCubeEvaluator::new(self))
    }

    /// Create a MockCubeEvaluator with primary keys from this schema
    pub fn create_evaluator_with_primary_keys(
        self,
        primary_keys: std::collections::HashMap<String, Vec<String>>,
    ) -> Rc<MockCubeEvaluator> {
        Rc::new(MockCubeEvaluator::with_primary_keys(self, primary_keys))
    }
}

/// Builder for MockSchema with fluent API
pub struct MockSchemaBuilder {
    cubes: HashMap<String, MockCube>,
}

impl MockSchemaBuilder {
    /// Create a new schema builder
    pub fn new() -> Self {
        Self {
            cubes: HashMap::new(),
        }
    }

    /// Add a cube and return a cube builder
    pub fn add_cube(self, name: impl Into<String>) -> MockCubeBuilder {
        MockCubeBuilder {
            schema_builder: self,
            cube_name: name.into(),
            cube_definition: None,
            measures: HashMap::new(),
            dimensions: HashMap::new(),
            segments: HashMap::new(),
        }
    }

    /// Build the final schema
    pub fn build(self) -> MockSchema {
        MockSchema { cubes: self.cubes }
    }
}

impl Default for MockSchemaBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for a single cube within a schema
pub struct MockCubeBuilder {
    schema_builder: MockSchemaBuilder,
    cube_name: String,
    cube_definition: Option<MockCubeDefinition>,
    measures: HashMap<String, Rc<MockMeasureDefinition>>,
    dimensions: HashMap<String, Rc<MockDimensionDefinition>>,
    segments: HashMap<String, Rc<MockSegmentDefinition>>,
}

impl MockCubeBuilder {
    /// Set the cube definition
    pub fn cube_def(mut self, definition: MockCubeDefinition) -> Self {
        self.cube_definition = Some(definition);
        self
    }

    /// Add a dimension to the cube
    pub fn add_dimension(
        mut self,
        name: impl Into<String>,
        definition: MockDimensionDefinition,
    ) -> Self {
        self.dimensions
            .insert(name.into(), Rc::new(definition));
        self
    }

    /// Add a measure to the cube
    pub fn add_measure(
        mut self,
        name: impl Into<String>,
        definition: MockMeasureDefinition,
    ) -> Self {
        self.measures.insert(name.into(), Rc::new(definition));
        self
    }

    /// Add a segment to the cube
    pub fn add_segment(
        mut self,
        name: impl Into<String>,
        definition: MockSegmentDefinition,
    ) -> Self {
        self.segments.insert(name.into(), Rc::new(definition));
        self
    }

    /// Finish building this cube and return to schema builder
    pub fn finish_cube(mut self) -> MockSchemaBuilder {
        let cube_def = self.cube_definition.unwrap_or_else(|| {
            // Create default cube definition with the cube name
            MockCubeDefinition::builder()
                .name(self.cube_name.clone())
                .sql_table(format!("public.{}", self.cube_name))
                .build()
        });

        let cube = MockCube {
            definition: cube_def,
            measures: self.measures,
            dimensions: self.dimensions,
            segments: self.segments,
        };

        self.schema_builder.cubes.insert(self.cube_name, cube);
        self.schema_builder
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cube_bridge::segment_definition::SegmentDefinition;

    #[test]
    fn test_basic_schema() {
        let schema = MockSchemaBuilder::new()
            .add_cube("users")
            .add_dimension(
                "id",
                MockDimensionDefinition::builder()
                    .dimension_type("number".to_string())
                    .sql("id".to_string())
                    .build(),
            )
            .add_dimension(
                "name",
                MockDimensionDefinition::builder()
                    .dimension_type("string".to_string())
                    .sql("name".to_string())
                    .build(),
            )
            .add_measure(
                "count",
                MockMeasureDefinition::builder()
                    .measure_type("count".to_string())
                    .sql("COUNT(*)".to_string())
                    .build(),
            )
            .finish_cube()
            .build();

        assert!(schema.get_cube("users").is_some());
        assert!(schema.get_dimension("users", "id").is_some());
        assert!(schema.get_dimension("users", "name").is_some());
        assert!(schema.get_measure("users", "count").is_some());
    }

    #[test]
    fn test_multiple_cubes() {
        let schema = MockSchemaBuilder::new()
            .add_cube("users")
            .add_dimension(
                "id",
                MockDimensionDefinition::builder()
                    .dimension_type("number".to_string())
                    .sql("id".to_string())
                    .build(),
            )
            .finish_cube()
            .add_cube("orders")
            .add_dimension(
                "id",
                MockDimensionDefinition::builder()
                    .dimension_type("number".to_string())
                    .sql("id".to_string())
                    .build(),
            )
            .add_measure(
                "total",
                MockMeasureDefinition::builder()
                    .measure_type("sum".to_string())
                    .sql("amount".to_string())
                    .build(),
            )
            .finish_cube()
            .build();

        assert_eq!(schema.cube_names().len(), 2);
        assert!(schema.get_cube("users").is_some());
        assert!(schema.get_cube("orders").is_some());
        assert!(schema.get_dimension("orders", "id").is_some());
        assert!(schema.get_measure("orders", "total").is_some());
    }

    #[test]
    fn test_cube_with_custom_definition() {
        let schema = MockSchemaBuilder::new()
            .add_cube("users")
            .cube_def(
                MockCubeDefinition::builder()
                    .name("users".to_string())
                    .sql_table("public.app_users".to_string())
                    .sql_alias(Some("u".to_string()))
                    .build(),
            )
            .add_dimension(
                "id",
                MockDimensionDefinition::builder()
                    .dimension_type("number".to_string())
                    .sql("id".to_string())
                    .build(),
            )
            .finish_cube()
            .build();

        let cube = schema.get_cube("users").unwrap();
        assert_eq!(cube.definition.static_data().name, "users");
        assert_eq!(
            cube.definition.static_data().sql_alias,
            Some("u".to_string())
        );
    }

    #[test]
    fn test_schema_lookups() {
        let schema = MockSchemaBuilder::new()
            .add_cube("users")
            .add_dimension(
                "visitor_id",
                MockDimensionDefinition::builder()
                    .dimension_type("number".to_string())
                    .sql("visitor_id".to_string())
                    .build(),
            )
            .add_dimension(
                "source",
                MockDimensionDefinition::builder()
                    .dimension_type("string".to_string())
                    .sql("source".to_string())
                    .build(),
            )
            .add_dimension(
                "created_at",
                MockDimensionDefinition::builder()
                    .dimension_type("time".to_string())
                    .sql("created_at".to_string())
                    .build(),
            )
            .finish_cube()
            .build();

        // Test dimension lookups
        let visitor_id = schema.get_dimension("users", "visitor_id").unwrap();
        assert_eq!(visitor_id.static_data().dimension_type, "number");

        let source = schema.get_dimension("users", "source").unwrap();
        assert_eq!(source.static_data().dimension_type, "string");

        let created_at = schema.get_dimension("users", "created_at").unwrap();
        assert_eq!(created_at.static_data().dimension_type, "time");

        // Test missing dimension
        assert!(schema.get_dimension("users", "nonexistent").is_none());
        assert!(schema.get_dimension("nonexistent_cube", "id").is_none());
    }

    #[test]
    fn test_complex_schema() {
        let schema = MockSchemaBuilder::new()
            .add_cube("users")
            .add_dimension(
                "id",
                MockDimensionDefinition::builder()
                    .dimension_type("number".to_string())
                    .sql("id".to_string())
                    .build(),
            )
            .add_dimension(
                "name",
                MockDimensionDefinition::builder()
                    .dimension_type("string".to_string())
                    .sql("name".to_string())
                    .build(),
            )
            .add_measure(
                "count",
                MockMeasureDefinition::builder()
                    .measure_type("count".to_string())
                    .sql("COUNT(*)".to_string())
                    .build(),
            )
            .finish_cube()
            .add_cube("orders")
            .add_dimension(
                "id",
                MockDimensionDefinition::builder()
                    .dimension_type("number".to_string())
                    .sql("id".to_string())
                    .build(),
            )
            .add_dimension(
                "user_id",
                MockDimensionDefinition::builder()
                    .dimension_type("number".to_string())
                    .sql("user_id".to_string())
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
                "total_amount",
                MockMeasureDefinition::builder()
                    .measure_type("sum".to_string())
                    .sql("amount".to_string())
                    .build(),
            )
            .finish_cube()
            .add_cube("cards")
            .add_dimension(
                "id",
                MockDimensionDefinition::builder()
                    .dimension_type("number".to_string())
                    .sql("id".to_string())
                    .build(),
            )
            .add_measure(
                "count",
                MockMeasureDefinition::builder()
                    .measure_type("count".to_string())
                    .sql("COUNT(*)".to_string())
                    .build(),
            )
            .finish_cube()
            .build();

        // Verify all cubes exist
        assert_eq!(schema.cube_names().len(), 3);
        assert!(schema.get_cube("users").is_some());
        assert!(schema.get_cube("orders").is_some());
        assert!(schema.get_cube("cards").is_some());

        // Verify measures across cubes
        assert!(schema.get_measure("users", "count").is_some());
        assert!(schema.get_measure("orders", "count").is_some());
        assert!(schema.get_measure("orders", "total_amount").is_some());
        assert!(schema.get_measure("cards", "count").is_some());

        // Verify dimensions
        assert!(schema.get_dimension("users", "name").is_some());
        assert!(schema.get_dimension("orders", "user_id").is_some());
    }

    #[test]
    fn test_schema_with_segments() {
        use crate::test_fixtures::cube_bridge::MockSegmentDefinition;

        let schema = MockSchemaBuilder::new()
            .add_cube("users")
            .add_dimension(
                "id",
                MockDimensionDefinition::builder()
                    .dimension_type("number".to_string())
                    .sql("id".to_string())
                    .build(),
            )
            .add_measure(
                "count",
                MockMeasureDefinition::builder()
                    .measure_type("count".to_string())
                    .sql("COUNT(*)".to_string())
                    .build(),
            )
            .add_segment(
                "active",
                MockSegmentDefinition::builder()
                    .sql("{CUBE.status} = 'active'".to_string())
                    .build(),
            )
            .add_segment(
                "premium",
                MockSegmentDefinition::builder()
                    .sql("{CUBE.is_premium} = true".to_string())
                    .segment_type(Some("filter".to_string()))
                    .build(),
            )
            .finish_cube()
            .build();

        // Verify cube exists
        assert!(schema.get_cube("users").is_some());

        // Verify segments
        let active_segment = schema.get_segment("users", "active").unwrap();
        let sql = active_segment.sql().unwrap();
        assert_eq!(sql.args_names(), &vec!["CUBE"]);

        let premium_segment = schema.get_segment("users", "premium").unwrap();
        assert_eq!(
            premium_segment.static_data().segment_type,
            Some("filter".to_string())
        );

        // Verify missing segment
        assert!(schema.get_segment("users", "nonexistent").is_none());
        assert!(schema.get_segment("nonexistent_cube", "active").is_none());
    }

    #[test]
    fn test_complete_schema_with_all_members() {
        use crate::test_fixtures::cube_bridge::MockSegmentDefinition;

        let schema = MockSchemaBuilder::new()
            .add_cube("orders")
            .add_dimension(
                "id",
                MockDimensionDefinition::builder()
                    .dimension_type("number".to_string())
                    .sql("id".to_string())
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
                "total_amount",
                MockMeasureDefinition::builder()
                    .measure_type("sum".to_string())
                    .sql("amount".to_string())
                    .build(),
            )
            .add_segment(
                "completed",
                MockSegmentDefinition::builder()
                    .sql("{CUBE.status} = 'completed'".to_string())
                    .build(),
            )
            .add_segment(
                "high_value",
                MockSegmentDefinition::builder()
                    .sql("{CUBE.amount} > 1000".to_string())
                    .build(),
            )
            .finish_cube()
            .build();

        let cube = schema.get_cube("orders").unwrap();

        // Verify all member types exist
        assert_eq!(cube.dimensions.len(), 3);
        assert_eq!(cube.measures.len(), 2);
        assert_eq!(cube.segments.len(), 2);

        // Verify lookups work for all member types
        assert!(schema.get_dimension("orders", "status").is_some());
        assert!(schema.get_measure("orders", "count").is_some());
        assert!(schema.get_segment("orders", "completed").is_some());
        assert!(schema.get_segment("orders", "high_value").is_some());
    }
}