use crate::cube_bridge::cube_definition::CubeDefinition;
use crate::cube_bridge::dimension_definition::DimensionDefinition;
use crate::cube_bridge::evaluator::{CubeEvaluator, CubeEvaluatorStatic};
use crate::cube_bridge::granularity_definition::GranularityDefinition;
use crate::cube_bridge::measure_definition::MeasureDefinition;
use crate::cube_bridge::member_sql::MemberSql;
use crate::cube_bridge::pre_aggregation_description::PreAggregationDescription;
use crate::cube_bridge::segment_definition::SegmentDefinition;
use crate::impl_static_data;
use crate::test_fixtures::cube_bridge::mock_schema::MockSchema;
use cubenativeutils::CubeError;
use std::any::Any;
use std::collections::HashMap;
use std::rc::Rc;

/// Mock implementation of CubeEvaluator for testing
pub struct MockCubeEvaluator {
    schema: MockSchema,
    primary_keys: HashMap<String, Vec<String>>,
}

impl MockCubeEvaluator {
    /// Create a new MockCubeEvaluator with the given schema
    pub fn new(schema: MockSchema) -> Self {
        Self {
            schema,
            primary_keys: HashMap::new(),
        }
    }

    /// Create a new MockCubeEvaluator with schema and primary keys
    pub fn with_primary_keys(
        schema: MockSchema,
        primary_keys: HashMap<String, Vec<String>>,
    ) -> Self {
        Self {
            schema,
            primary_keys,
        }
    }

    /// Parse a path string like "cube.member" into ["cube", "member"]
    /// Returns error if the path doesn't exist in schema for the given type
    fn parse_and_validate_path(
        &self,
        path_type: &str,
        path: &str,
    ) -> Result<Vec<String>, CubeError> {
        let parts: Vec<String> = path.split('.').map(|s| s.to_string()).collect();

        if parts.len() != 2 {
            return Err(CubeError::user(format!(
                "Invalid path format: '{}'. Expected format: 'cube.member'",
                path
            )));
        }

        let cube_name = &parts[0];
        let member_name = &parts[1];

        // Check if cube exists
        if self.schema.get_cube(cube_name).is_none() {
            return Err(CubeError::user(format!("Cube '{}' not found", cube_name)));
        }

        // Validate member exists for the given type
        let exists = match path_type {
            "measure" | "measures" => self.schema.get_measure(cube_name, member_name).is_some(),
            "dimension" | "dimensions" => {
                self.schema.get_dimension(cube_name, member_name).is_some()
            }
            "segment" | "segments" => self.schema.get_segment(cube_name, member_name).is_some(),
            _ => {
                return Err(CubeError::user(format!(
                    "Unknown path type: '{}'. Expected: measure, dimension, or segment",
                    path_type
                )))
            }
        };

        if !exists {
            return Err(CubeError::user(format!(
                "{} '{}' not found in cube '{}'",
                path_type, member_name, cube_name
            )));
        }

        Ok(parts)
    }
}

impl_static_data!(MockCubeEvaluator, CubeEvaluatorStatic, primary_keys);

impl CubeEvaluator for MockCubeEvaluator {
    crate::impl_static_data_method!(CubeEvaluatorStatic);

    fn parse_path(&self, path_type: String, path: String) -> Result<Vec<String>, CubeError> {
        self.parse_and_validate_path(&path_type, &path)
    }

    fn measure_by_path(
        &self,
        measure_path: String,
    ) -> Result<Rc<dyn MeasureDefinition>, CubeError> {
        let parts = self.parse_and_validate_path("measure", &measure_path)?;
        let cube_name = &parts[0];
        let measure_name = &parts[1];

        self.schema
            .get_measure(cube_name, measure_name)
            .map(|m| m as Rc<dyn MeasureDefinition>)
            .ok_or_else(|| {
                CubeError::user(format!(
                    "Measure '{}' not found in cube '{}'",
                    measure_name, cube_name
                ))
            })
    }

    fn dimension_by_path(
        &self,
        dimension_path: String,
    ) -> Result<Rc<dyn DimensionDefinition>, CubeError> {
        let parts = self.parse_and_validate_path("dimension", &dimension_path)?;
        let cube_name = &parts[0];
        let dimension_name = &parts[1];

        self.schema
            .get_dimension(cube_name, dimension_name)
            .map(|d| d as Rc<dyn DimensionDefinition>)
            .ok_or_else(|| {
                CubeError::user(format!(
                    "Dimension '{}' not found in cube '{}'",
                    dimension_name, cube_name
                ))
            })
    }

    fn segment_by_path(
        &self,
        segment_path: String,
    ) -> Result<Rc<dyn SegmentDefinition>, CubeError> {
        let parts = self.parse_and_validate_path("segment", &segment_path)?;
        let cube_name = &parts[0];
        let segment_name = &parts[1];

        self.schema
            .get_segment(cube_name, segment_name)
            .map(|s| s as Rc<dyn SegmentDefinition>)
            .ok_or_else(|| {
                CubeError::user(format!(
                    "Segment '{}' not found in cube '{}'",
                    segment_name, cube_name
                ))
            })
    }

    fn cube_from_path(&self, cube_path: String) -> Result<Rc<dyn CubeDefinition>, CubeError> {
        self.schema
            .get_cube(&cube_path)
            .map(|c| Rc::new(c.definition.clone()) as Rc<dyn CubeDefinition>)
            .ok_or_else(|| CubeError::user(format!("Cube '{}' not found", cube_path)))
    }

    fn is_measure(&self, path: Vec<String>) -> Result<bool, CubeError> {
        if path.len() != 2 {
            return Ok(false);
        }
        Ok(self.schema.get_measure(&path[0], &path[1]).is_some())
    }

    fn is_dimension(&self, path: Vec<String>) -> Result<bool, CubeError> {
        if path.len() != 2 {
            return Ok(false);
        }
        Ok(self.schema.get_dimension(&path[0], &path[1]).is_some())
    }

    fn is_segment(&self, path: Vec<String>) -> Result<bool, CubeError> {
        if path.len() != 2 {
            return Ok(false);
        }
        Ok(self.schema.get_segment(&path[0], &path[1]).is_some())
    }

    fn cube_exists(&self, name: String) -> Result<bool, CubeError> {
        Ok(self.schema.get_cube(&name).is_some())
    }

    fn resolve_granularity(
        &self,
        _path: Vec<String>,
    ) -> Result<Rc<dyn GranularityDefinition>, CubeError> {
        todo!("resolve_granularity is not implemented in MockCubeEvaluator")
    }

    fn pre_aggregations_for_cube_as_array(
        &self,
        _cube_name: String,
    ) -> Result<Vec<Rc<dyn PreAggregationDescription>>, CubeError> {
        todo!("pre_aggregations_for_cube_as_array is not implemented in MockCubeEvaluator")
    }

    fn has_pre_aggregation_description_by_name(&self) -> Result<bool, CubeError> {
        todo!("has_pre_aggregation_description_by_name is not implemented in MockCubeEvaluator")
    }

    fn pre_aggregation_description_by_name(
        &self,
        _cube_name: String,
        _name: String,
    ) -> Result<Option<Rc<dyn PreAggregationDescription>>, CubeError> {
        todo!("pre_aggregation_description_by_name is not implemented in MockCubeEvaluator")
    }

    fn evaluate_rollup_references(
        &self,
        _cube: String,
        _sql: Rc<dyn MemberSql>,
    ) -> Result<Vec<String>, CubeError> {
        todo!("evaluate_rollup_references is not implemented in MockCubeEvaluator")
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::cube_bridge::{
        MockDimensionDefinition, MockMeasureDefinition, MockSchemaBuilder, MockSegmentDefinition,
    };

    fn create_test_schema() -> MockSchema {
        MockSchemaBuilder::new()
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
            .add_segment(
                "active",
                MockSegmentDefinition::builder()
                    .sql("{CUBE.status} = 'active'".to_string())
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
            .build()
    }

    #[test]
    fn test_parse_path_measure() {
        let schema = create_test_schema();
        let evaluator = MockCubeEvaluator::new(schema);

        let result = evaluator.parse_path("measure".to_string(), "users.count".to_string());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec!["users", "count"]);
    }

    #[test]
    fn test_parse_path_dimension() {
        let schema = create_test_schema();
        let evaluator = MockCubeEvaluator::new(schema);

        let result = evaluator.parse_path("dimension".to_string(), "users.name".to_string());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec!["users", "name"]);
    }

    #[test]
    fn test_parse_path_segment() {
        let schema = create_test_schema();
        let evaluator = MockCubeEvaluator::new(schema);

        let result = evaluator.parse_path("segment".to_string(), "users.active".to_string());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec!["users", "active"]);
    }

    #[test]
    fn test_parse_path_invalid_format() {
        let schema = create_test_schema();
        let evaluator = MockCubeEvaluator::new(schema);

        let result = evaluator.parse_path("measure".to_string(), "invalid".to_string());
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("Invalid path format"));
    }

    #[test]
    fn test_parse_path_cube_not_found() {
        let schema = create_test_schema();
        let evaluator = MockCubeEvaluator::new(schema);

        let result =
            evaluator.parse_path("measure".to_string(), "nonexistent.count".to_string());
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("Cube 'nonexistent' not found"));
    }

    #[test]
    fn test_parse_path_member_not_found() {
        let schema = create_test_schema();
        let evaluator = MockCubeEvaluator::new(schema);

        let result =
            evaluator.parse_path("measure".to_string(), "users.nonexistent".to_string());
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .message
            .contains("measure 'nonexistent' not found"));
    }

    #[test]
    fn test_measure_by_path() {
        let schema = create_test_schema();
        let evaluator = MockCubeEvaluator::new(schema);

        let measure = evaluator.measure_by_path("users.count".to_string()).unwrap();
        assert_eq!(measure.static_data().measure_type, "count");
    }

    #[test]
    fn test_dimension_by_path() {
        let schema = create_test_schema();
        let evaluator = MockCubeEvaluator::new(schema);

        let dimension = evaluator
            .dimension_by_path("users.name".to_string())
            .unwrap();
        assert_eq!(dimension.static_data().dimension_type, "string");
    }

    #[test]
    fn test_segment_by_path() {
        let schema = create_test_schema();
        let evaluator = MockCubeEvaluator::new(schema);

        let segment = evaluator.segment_by_path("users.active".to_string()).unwrap();
        // Verify it's a valid segment
        assert!(segment.sql().is_ok());
    }

    #[test]
    fn test_cube_from_path() {
        let schema = create_test_schema();
        let evaluator = MockCubeEvaluator::new(schema);

        let cube = evaluator.cube_from_path("users".to_string()).unwrap();
        assert_eq!(cube.static_data().name, "users");
    }

    #[test]
    fn test_cube_from_path_not_found() {
        let schema = create_test_schema();
        let evaluator = MockCubeEvaluator::new(schema);

        let result = evaluator.cube_from_path("nonexistent".to_string());
        assert!(result.is_err());
        if let Err(err) = result {
            assert!(err.message.contains("Cube 'nonexistent' not found"));
        }
    }

    #[test]
    fn test_is_measure() {
        let schema = create_test_schema();
        let evaluator = MockCubeEvaluator::new(schema);

        assert!(evaluator
            .is_measure(vec!["users".to_string(), "count".to_string()])
            .unwrap());
        assert!(!evaluator
            .is_measure(vec!["users".to_string(), "name".to_string()])
            .unwrap());
        assert!(!evaluator
            .is_measure(vec!["users".to_string(), "nonexistent".to_string()])
            .unwrap());
    }

    #[test]
    fn test_is_dimension() {
        let schema = create_test_schema();
        let evaluator = MockCubeEvaluator::new(schema);

        assert!(evaluator
            .is_dimension(vec!["users".to_string(), "name".to_string()])
            .unwrap());
        assert!(!evaluator
            .is_dimension(vec!["users".to_string(), "count".to_string()])
            .unwrap());
        assert!(!evaluator
            .is_dimension(vec!["users".to_string(), "nonexistent".to_string()])
            .unwrap());
    }

    #[test]
    fn test_is_segment() {
        let schema = create_test_schema();
        let evaluator = MockCubeEvaluator::new(schema);

        assert!(evaluator
            .is_segment(vec!["users".to_string(), "active".to_string()])
            .unwrap());
        assert!(!evaluator
            .is_segment(vec!["users".to_string(), "count".to_string()])
            .unwrap());
        assert!(!evaluator
            .is_segment(vec!["users".to_string(), "nonexistent".to_string()])
            .unwrap());
    }

    #[test]
    fn test_cube_exists() {
        let schema = create_test_schema();
        let evaluator = MockCubeEvaluator::new(schema);

        assert!(evaluator.cube_exists("users".to_string()).unwrap());
        assert!(evaluator.cube_exists("orders".to_string()).unwrap());
        assert!(!evaluator.cube_exists("nonexistent".to_string()).unwrap());
    }

    #[test]
    fn test_with_primary_keys() {
        let schema = create_test_schema();
        let mut primary_keys = HashMap::new();
        primary_keys.insert("users".to_string(), vec!["id".to_string()]);
        primary_keys.insert(
            "orders".to_string(),
            vec!["id".to_string(), "user_id".to_string()],
        );

        let evaluator = MockCubeEvaluator::with_primary_keys(schema, primary_keys.clone());

        let static_data = evaluator.static_data();
        assert_eq!(static_data.primary_keys, primary_keys);
    }

    #[test]
    fn test_multiple_cubes() {
        let schema = create_test_schema();
        let evaluator = MockCubeEvaluator::new(schema);

        // Test users cube
        assert!(evaluator.cube_exists("users".to_string()).unwrap());
        assert!(evaluator
            .is_measure(vec!["users".to_string(), "count".to_string()])
            .unwrap());
        assert!(evaluator
            .is_dimension(vec!["users".to_string(), "name".to_string()])
            .unwrap());

        // Test orders cube
        assert!(evaluator.cube_exists("orders".to_string()).unwrap());
        assert!(evaluator
            .is_measure(vec!["orders".to_string(), "total".to_string()])
            .unwrap());
        assert!(evaluator
            .is_dimension(vec!["orders".to_string(), "id".to_string()])
            .unwrap());
    }

    #[test]
    #[should_panic(expected = "resolve_granularity is not implemented")]
    fn test_resolve_granularity_panics() {
        let schema = create_test_schema();
        let evaluator = MockCubeEvaluator::new(schema);

        let _ = evaluator.resolve_granularity(vec!["users".to_string(), "created_at".to_string()]);
    }

    #[test]
    #[should_panic(expected = "pre_aggregations_for_cube_as_array is not implemented")]
    fn test_pre_aggregations_for_cube_panics() {
        let schema = create_test_schema();
        let evaluator = MockCubeEvaluator::new(schema);

        let _ = evaluator.pre_aggregations_for_cube_as_array("users".to_string());
    }

    #[test]
    #[should_panic(expected = "pre_aggregation_description_by_name is not implemented")]
    fn test_pre_aggregation_by_name_panics() {
        let schema = create_test_schema();
        let evaluator = MockCubeEvaluator::new(schema);

        let _ = evaluator.pre_aggregation_description_by_name(
            "users".to_string(),
            "main".to_string(),
        );
    }

    #[test]
    #[should_panic(expected = "evaluate_rollup_references is not implemented")]
    fn test_evaluate_rollup_references_panics() {
        let schema = create_test_schema();
        let evaluator = MockCubeEvaluator::new(schema);

        use crate::test_fixtures::cube_bridge::MockMemberSql;
        let sql = Rc::new(MockMemberSql::new("{CUBE.id}").unwrap());
        let _ = evaluator.evaluate_rollup_references("users".to_string(), sql);
    }
}