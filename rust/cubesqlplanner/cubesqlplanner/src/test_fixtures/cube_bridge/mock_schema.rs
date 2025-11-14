use crate::test_fixtures::cube_bridge::{
    MockCubeDefinition, MockCubeEvaluator, MockDimensionDefinition, MockMeasureDefinition,
    MockSegmentDefinition,
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
    #[allow(dead_code)]
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

    /// Add a view and return a view builder
    pub fn add_view(self, name: impl Into<String>) -> MockViewBuilder {
        MockViewBuilder {
            schema_builder: self,
            view_name: name.into(),
            view_cubes: Vec::new(),
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
        self.dimensions.insert(name.into(), Rc::new(definition));
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

/// Represents a cube to include in a view
pub struct ViewCube {
    /// Join path to the cube (e.g., "visitors" or "visitors.visitor_checkins")
    pub join_path: String,
    /// Member names to include, empty vec means include all
    pub includes: Vec<String>,
}

/// Builder for a view within a schema
pub struct MockViewBuilder {
    schema_builder: MockSchemaBuilder,
    view_name: String,
    view_cubes: Vec<ViewCube>,
    measures: HashMap<String, Rc<MockMeasureDefinition>>,
    dimensions: HashMap<String, Rc<MockDimensionDefinition>>,
    segments: HashMap<String, Rc<MockSegmentDefinition>>,
}

impl MockViewBuilder {
    /// Add a cube to include in this view
    pub fn include_cube(mut self, join_path: impl Into<String>, includes: Vec<String>) -> Self {
        self.view_cubes.push(ViewCube {
            join_path: join_path.into(),
            includes,
        });
        self
    }

    /// Add a custom dimension to the view
    pub fn add_dimension(
        mut self,
        name: impl Into<String>,
        definition: MockDimensionDefinition,
    ) -> Self {
        self.dimensions.insert(name.into(), Rc::new(definition));
        self
    }

    /// Add a custom measure to the view
    pub fn add_measure(
        mut self,
        name: impl Into<String>,
        definition: MockMeasureDefinition,
    ) -> Self {
        self.measures.insert(name.into(), Rc::new(definition));
        self
    }

    /// Add a custom segment to the view
    #[allow(dead_code)]
    pub fn add_segment(
        mut self,
        name: impl Into<String>,
        definition: MockSegmentDefinition,
    ) -> Self {
        self.segments.insert(name.into(), Rc::new(definition));
        self
    }

    /// Finish building this view and return to schema builder
    pub fn finish_view(mut self) -> MockSchemaBuilder {
        let mut all_dimensions = self.dimensions;
        let mut all_measures = self.measures;
        let mut all_segments = self.segments;

        // Process each included cube
        for view_cube in &self.view_cubes {
            let join_path_parts: Vec<&str> = view_cube.join_path.split('.').collect();
            let target_cube_name = join_path_parts.last().unwrap();

            // Get the target cube from schema
            if let Some(source_cube) = self.schema_builder.cubes.get(*target_cube_name) {
                // Determine which members to include
                let members_to_include: Vec<String> = if view_cube.includes.is_empty() {
                    // Include all members
                    let mut all_members = Vec::new();
                    all_members.extend(source_cube.dimensions.keys().cloned());
                    all_members.extend(source_cube.measures.keys().cloned());
                    all_members.extend(source_cube.segments.keys().cloned());
                    all_members
                } else {
                    view_cube.includes.clone()
                };

                // Add dimensions
                for member_name in &members_to_include {
                    if let Some(dimension) = source_cube.dimensions.get(member_name) {
                        let view_member_sql =
                            format!("{{{}.{}}}", view_cube.join_path, member_name);

                        // Check for duplicates
                        if all_dimensions.contains_key(member_name) {
                            panic!(
                                "Duplicate member '{}' in view '{}'. Members must be unique.",
                                member_name, self.view_name
                            );
                        }

                        all_dimensions.insert(
                            member_name.clone(),
                            Rc::new(
                                MockDimensionDefinition::builder()
                                    .dimension_type(dimension.static_data().dimension_type.clone())
                                    .sql(view_member_sql)
                                    .build(),
                            ),
                        );
                    }
                }

                // Add measures
                for member_name in &members_to_include {
                    if let Some(measure) = source_cube.measures.get(member_name) {
                        let view_member_sql =
                            format!("{{{}.{}}}", view_cube.join_path, member_name);

                        // Check for duplicates
                        if all_measures.contains_key(member_name) {
                            panic!(
                                "Duplicate member '{}' in view '{}'. Members must be unique.",
                                member_name, self.view_name
                            );
                        }

                        all_measures.insert(
                            member_name.clone(),
                            Rc::new(
                                MockMeasureDefinition::builder()
                                    .measure_type(measure.static_data().measure_type.clone())
                                    .sql(view_member_sql)
                                    .build(),
                            ),
                        );
                    }
                }

                // Add segments
                for member_name in &members_to_include {
                    if source_cube.segments.contains_key(member_name) {
                        let view_member_sql =
                            format!("{{{}.{}}}", view_cube.join_path, member_name);

                        // Check for duplicates
                        if all_segments.contains_key(member_name) {
                            panic!(
                                "Duplicate member '{}' in view '{}'. Members must be unique.",
                                member_name, self.view_name
                            );
                        }

                        all_segments.insert(
                            member_name.clone(),
                            Rc::new(
                                MockSegmentDefinition::builder()
                                    .sql(view_member_sql)
                                    .build(),
                            ),
                        );
                    }
                }
            }
        }

        // Create view cube definition with is_view = true
        let view_def = MockCubeDefinition::builder()
            .name(self.view_name.clone())
            .is_view(Some(true))
            .build();

        let view_cube = MockCube {
            definition: view_def,
            measures: all_measures,
            dimensions: all_dimensions,
            segments: all_segments,
        };

        self.schema_builder.cubes.insert(self.view_name, view_cube);
        self.schema_builder
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cube_bridge::dimension_definition::DimensionDefinition;
    use crate::cube_bridge::measure_definition::MeasureDefinition;
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

    #[test]
    fn test_view_with_includes_all() {
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
            .add_view("users_view")
            .include_cube("users", vec![]) // Include all members
            .finish_view()
            .build();

        // Verify view exists and is marked as view
        let view_cube = schema.get_cube("users_view").unwrap();
        assert_eq!(view_cube.definition.static_data().is_view, Some(true));

        // Verify all members were included
        assert_eq!(view_cube.dimensions.len(), 2);
        assert_eq!(view_cube.measures.len(), 1);

        // Verify member SQL references original cube
        let id_dim = schema.get_dimension("users_view", "id").unwrap();
        let id_sql = id_dim.sql().unwrap().unwrap();
        assert_eq!(id_sql.args_names(), &vec!["users"]);

        let count_measure = schema.get_measure("users_view", "count").unwrap();
        let count_sql = count_measure.sql().unwrap().unwrap();
        assert_eq!(count_sql.args_names(), &vec!["users"]);
    }

    #[test]
    fn test_view_with_specific_includes() {
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
            .add_view("users_view")
            .include_cube("users", vec!["id".to_string(), "count".to_string()])
            .finish_view()
            .build();

        let view_cube = schema.get_cube("users_view").unwrap();

        // Only specified members should be included
        assert_eq!(view_cube.dimensions.len(), 1);
        assert_eq!(view_cube.measures.len(), 1);

        assert!(schema.get_dimension("users_view", "id").is_some());
        assert!(schema.get_dimension("users_view", "name").is_none());
        assert!(schema.get_measure("users_view", "count").is_some());
    }

    #[test]
    fn test_view_with_custom_members() {
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
            .finish_cube()
            .add_view("users_view")
            .include_cube("users", vec!["id".to_string()])
            .add_dimension(
                "custom_dim",
                MockDimensionDefinition::builder()
                    .dimension_type("string".to_string())
                    .sql("CUSTOM_SQL".to_string())
                    .build(),
            )
            .add_measure(
                "custom_measure",
                MockMeasureDefinition::builder()
                    .measure_type("sum".to_string())
                    .sql("custom_value".to_string())
                    .build(),
            )
            .finish_view()
            .build();

        let view_cube = schema.get_cube("users_view").unwrap();

        // Should have both included and custom members
        assert_eq!(view_cube.dimensions.len(), 2); // id + custom_dim
        assert_eq!(view_cube.measures.len(), 1); // custom_measure

        assert!(schema.get_dimension("users_view", "id").is_some());
        assert!(schema.get_dimension("users_view", "custom_dim").is_some());
        assert!(schema.get_measure("users_view", "custom_measure").is_some());
    }

    #[test]
    fn test_view_with_join_path() {
        let schema = MockSchemaBuilder::new()
            .add_cube("orders")
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
            .add_view("orders_view")
            .include_cube("users.orders", vec!["id".to_string(), "count".to_string()])
            .finish_view()
            .build();

        // Verify member SQL uses full join path
        let id_dim = schema.get_dimension("orders_view", "id").unwrap();
        let id_sql = id_dim.sql().unwrap().unwrap();
        assert_eq!(id_sql.args_names(), &vec!["users"]);

        let count_measure = schema.get_measure("orders_view", "count").unwrap();
        let count_sql = count_measure.sql().unwrap().unwrap();
        assert_eq!(count_sql.args_names(), &vec!["users"]);
    }

    #[test]
    fn test_view_with_multiple_long_join_paths() {
        use crate::test_fixtures::cube_bridge::{MockSecurityContext, MockSqlUtils};
        use std::rc::Rc;

        let schema = MockSchemaBuilder::new()
            .add_cube("visitors")
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
            .add_cube("visitor_checkins")
            .add_dimension(
                "checkin_id",
                MockDimensionDefinition::builder()
                    .dimension_type("number".to_string())
                    .sql("id".to_string())
                    .build(),
            )
            .add_measure(
                "checkin_count",
                MockMeasureDefinition::builder()
                    .measure_type("count".to_string())
                    .sql("COUNT(*)".to_string())
                    .build(),
            )
            .finish_cube()
            .add_view("multi_path_view")
            .include_cube(
                "visitors.visitor_checkins",
                vec!["checkin_id".to_string(), "checkin_count".to_string()],
            )
            .include_cube("visitors", vec!["id".to_string(), "count".to_string()])
            .finish_view()
            .build();

        let view_cube = schema.get_cube("multi_path_view").unwrap();

        // Verify all members from both cubes are included
        assert_eq!(view_cube.dimensions.len(), 2); // checkin_id + id
        assert_eq!(view_cube.measures.len(), 2); // checkin_count + count

        // Verify SQL for members from first include (with long join path)
        // SQL template should contain full path: {visitors.visitor_checkins.checkin_id}
        let checkin_id_dim = schema
            .get_dimension("multi_path_view", "checkin_id")
            .unwrap();
        let checkin_id_sql = checkin_id_dim.sql().unwrap().unwrap();

        // Compile template and check symbol_paths structure
        let (_template, args) = checkin_id_sql
            .compile_template_sql(Rc::new(MockSqlUtils), Rc::new(MockSecurityContext))
            .unwrap();

        // Should have exactly one symbol path
        assert_eq!(
            args.symbol_paths.len(),
            1,
            "Should have exactly one symbol path"
        );

        // The symbol path should be ["visitors", "visitor_checkins", "checkin_id"]
        assert_eq!(
            args.symbol_paths[0],
            vec!["visitors", "visitor_checkins", "checkin_id"],
            "Symbol path should be visitors.visitor_checkins.checkin_id"
        );

        let checkin_count_measure = schema
            .get_measure("multi_path_view", "checkin_count")
            .unwrap();
        let checkin_count_sql = checkin_count_measure.sql().unwrap().unwrap();

        let (_template, args) = checkin_count_sql
            .compile_template_sql(Rc::new(MockSqlUtils), Rc::new(MockSecurityContext))
            .unwrap();

        assert_eq!(
            args.symbol_paths.len(),
            1,
            "Should have exactly one symbol path"
        );
        assert_eq!(
            args.symbol_paths[0],
            vec!["visitors", "visitor_checkins", "checkin_count"],
            "Symbol path should be visitors.visitor_checkins.checkin_count"
        );

        // Verify SQL for members from second include (simple path)
        // SQL template should be: {visitors.id}
        let id_dim = schema.get_dimension("multi_path_view", "id").unwrap();
        let id_sql = id_dim.sql().unwrap().unwrap();

        let (_template, args) = id_sql
            .compile_template_sql(Rc::new(MockSqlUtils), Rc::new(MockSecurityContext))
            .unwrap();

        assert_eq!(
            args.symbol_paths.len(),
            1,
            "Should have exactly one symbol path"
        );
        assert_eq!(
            args.symbol_paths[0],
            vec!["visitors", "id"],
            "Symbol path should be visitors.id"
        );

        let count_measure = schema.get_measure("multi_path_view", "count").unwrap();
        let count_sql = count_measure.sql().unwrap().unwrap();

        let (_template, args) = count_sql
            .compile_template_sql(Rc::new(MockSqlUtils), Rc::new(MockSecurityContext))
            .unwrap();

        assert_eq!(
            args.symbol_paths.len(),
            1,
            "Should have exactly one symbol path"
        );
        assert_eq!(
            args.symbol_paths[0],
            vec!["visitors", "count"],
            "Symbol path should be visitors.count"
        );
    }

    #[test]
    #[should_panic(expected = "Duplicate member 'id' in view 'multi_view'")]
    fn test_view_duplicate_members_panic() {
        MockSchemaBuilder::new()
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
            .finish_cube()
            .add_view("multi_view")
            .include_cube("users", vec![])
            .include_cube("orders", vec![])
            .finish_view()
            .build();
    }
}

