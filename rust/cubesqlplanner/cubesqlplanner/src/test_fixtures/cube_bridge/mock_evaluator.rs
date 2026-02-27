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
use crate::test_fixtures::cube_bridge::{MockBaseTools, MockJoinGraph, MockSecurityContext};
use cubenativeutils::CubeError;
use std::any::Any;
use std::collections::HashMap;
use std::rc::Rc;

pub struct MockCubeEvaluator {
    schema: MockSchema,
    primary_keys: HashMap<String, Vec<String>>,
    #[allow(dead_code)]
    join_graph: Option<Rc<MockJoinGraph>>,
}

impl MockCubeEvaluator {
    #[allow(dead_code)]
    pub fn new(schema: MockSchema) -> Self {
        Self {
            schema,
            primary_keys: HashMap::new(),
            join_graph: None,
        }
    }

    pub fn with_primary_keys(
        schema: MockSchema,
        primary_keys: HashMap<String, Vec<String>>,
    ) -> Self {
        Self {
            schema,
            primary_keys,
            join_graph: None,
        }
    }

    pub fn with_join_graph(
        schema: MockSchema,
        primary_keys: HashMap<String, Vec<String>>,
        join_graph: MockJoinGraph,
    ) -> Self {
        Self {
            schema,
            primary_keys,
            join_graph: Some(Rc::new(join_graph)),
        }
    }

    #[allow(dead_code)]
    pub fn join_graph(&self) -> Option<Rc<MockJoinGraph>> {
        self.join_graph.clone()
    }

    pub fn measures_for_cube(
        &self,
        cube_name: &str,
    ) -> HashMap<String, Rc<crate::test_fixtures::cube_bridge::MockMeasureDefinition>> {
        self.schema
            .get_cube(cube_name)
            .map(|cube| cube.measures.clone())
            .unwrap_or_default()
    }

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

        if self.schema.get_cube(cube_name).is_none() {
            return Err(CubeError::user(format!("Cube '{}' not found", cube_name)));
        }

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
        path: Vec<String>,
    ) -> Result<Rc<dyn GranularityDefinition>, CubeError> {
        if path.len() != 4 {
            return Err(CubeError::user(format!(
                "Invalid granularity path: expected 4 parts (cube.dimension.granularities.granularity), got {}",
                path.len()
            )));
        }

        if path[2] != "granularities" {
            return Err(CubeError::user(format!(
                "Invalid granularity path: expected 'granularities' at position 2, got '{}'",
                path[2]
            )));
        }

        let granularity = &path[3];

        // Check custom granularities in schema first
        if let Some(custom) = self.schema.get_granularity(&path[0], &path[1], granularity) {
            return Ok(custom as Rc<dyn GranularityDefinition>);
        }

        // Fall back to predefined granularities
        let predefined = [
            "second", "minute", "hour", "day", "week", "month", "quarter", "year",
        ];

        if predefined.contains(&granularity.as_str()) {
            use crate::test_fixtures::cube_bridge::MockGranularityDefinition;
            Ok(Rc::new(
                MockGranularityDefinition::builder()
                    .interval(format!("1 {}", granularity))
                    .build(),
            ) as Rc<dyn GranularityDefinition>)
        } else {
            Err(CubeError::user(format!(
                "Granularity '{}' not found",
                granularity
            )))
        }
    }

    fn pre_aggregations_for_cube_as_array(
        &self,
        cube_name: String,
    ) -> Result<Vec<Rc<dyn PreAggregationDescription>>, CubeError> {
        Ok(self
            .schema
            .get_pre_aggregations_for_cube(&cube_name)
            .map(|pre_aggs| {
                pre_aggs
                    .into_iter()
                    .map(|(_, pre_agg)| pre_agg as Rc<dyn PreAggregationDescription>)
                    .collect()
            })
            .unwrap_or_default())
    }

    fn has_pre_aggregation_description_by_name(&self) -> Result<bool, CubeError> {
        Ok(true)
    }

    fn pre_aggregation_description_by_name(
        &self,
        cube_name: String,
        name: String,
    ) -> Result<Option<Rc<dyn PreAggregationDescription>>, CubeError> {
        Ok(self
            .schema
            .get_pre_aggregation(&cube_name, &name)
            .map(|pre_agg| pre_agg as Rc<dyn PreAggregationDescription>))
    }

    fn evaluate_rollup_references(
        &self,
        _cube: String,
        sql: Rc<dyn MemberSql>,
    ) -> Result<Vec<String>, CubeError> {
        // Simple implementation for mock: extract symbol paths from compiled template
        // For YAML schemas, rollups are already provided as strings like "visitors.for_join"
        // which MockMemberSql parses into symbol_paths like [["visitors", "for_join"]]
        let (_template, args) = sql.compile_template_sql(
            Rc::new(MockBaseTools::default()),
            Rc::new(MockSecurityContext),
        )?;

        // Convert symbol paths back to dot-separated strings
        Ok(args
            .symbol_paths
            .iter()
            .map(|path| path.join("."))
            .collect())
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::cube_bridge::MockSchema;

    fn create_custom_granularity_schema() -> MockSchema {
        MockSchema::from_yaml_file("common/custom_granularity_test.yaml")
    }

    fn resolve(
        evaluator: &MockCubeEvaluator,
        granularity: &str,
    ) -> Result<Rc<dyn GranularityDefinition>, CubeError> {
        evaluator.resolve_granularity(vec![
            "orders".to_string(),
            "created_at".to_string(),
            "granularities".to_string(),
            granularity.to_string(),
        ])
    }

    #[test]
    fn test_resolve_predefined_granularity() {
        let schema = create_custom_granularity_schema();
        let evaluator = schema.create_evaluator();

        let result = resolve(&evaluator, "day").expect("should resolve predefined granularity");
        assert_eq!(result.static_data().interval, "1 day");
        assert_eq!(result.static_data().origin, None);
        assert_eq!(result.static_data().offset, None);
    }

    #[test]
    fn test_resolve_custom_granularity() {
        let schema = create_custom_granularity_schema();
        let evaluator = schema.create_evaluator();

        let result = resolve(&evaluator, "half_year").expect("should resolve custom granularity");
        assert_eq!(result.static_data().interval, "6 months");
        assert_eq!(result.static_data().origin, Some("2024-01-01".to_string()));
        assert_eq!(result.static_data().offset, None);
    }

    #[test]
    fn test_resolve_custom_granularity_with_offset() {
        let schema = create_custom_granularity_schema();
        let evaluator = schema.create_evaluator();

        let result = resolve(&evaluator, "fiscal_year").expect("should resolve custom granularity");
        assert_eq!(result.static_data().interval, "1 year");
        assert_eq!(result.static_data().offset, Some("1 month".to_string()));
    }

    #[test]
    fn test_resolve_unknown_granularity_error() {
        let schema = create_custom_granularity_schema();
        let evaluator = schema.create_evaluator();

        let result = resolve(&evaluator, "nonexistent");
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(
            err.message.contains("Granularity 'nonexistent' not found"),
            "unexpected error: {}",
            err.message
        );
    }
}
