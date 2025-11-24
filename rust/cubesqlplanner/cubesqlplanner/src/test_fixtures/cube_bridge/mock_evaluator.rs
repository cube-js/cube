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
use crate::test_fixtures::cube_bridge::MockJoinGraph;
use cubenativeutils::CubeError;
use std::any::Any;
use std::collections::HashMap;
use std::rc::Rc;

pub struct MockCubeEvaluator {
    schema: MockSchema,
    primary_keys: HashMap<String, Vec<String>>,
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

        if parts.len() != 2 && parts.len() != 3 {
            return Err(CubeError::user(format!(
                "Invalid path format: '{}'. Expected format: 'cube.member' or 'cube.time_dimension.granularity'",
                path
            )));
        }

        let cube_name = &parts[0];
        let member_name = &parts[1];

        if self.schema.get_cube(cube_name).is_none() {
            return Err(CubeError::user(format!("Cube '{}' not found", cube_name)));
        }

        if parts.len() == 3 {
            if path_type != "dimension" && path_type != "dimensions" {
                return Err(CubeError::user(format!(
                    "Granularity can only be specified for dimensions, not for {}",
                    path_type
                )));
            }

            if let Some(dimension) = self.schema.get_dimension(cube_name, member_name) {
                if dimension.static_data().dimension_type != "time" {
                    return Err(CubeError::user(format!(
                        "Granularity can only be specified for time dimensions, but '{}' is of type '{}'",
                        member_name,
                        dimension.static_data().dimension_type
                    )));
                }
                return Ok(parts);
            } else {
                return Err(CubeError::user(format!(
                    "Dimension '{}' not found in cube '{}'",
                    member_name, cube_name
                )));
            }
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

        let valid_granularities = [
            "second", "minute", "hour", "day", "week", "month", "quarter", "year",
        ];

        if !valid_granularities.contains(&granularity.as_str()) {
            return Err(CubeError::user(format!(
                "Unsupported granularity: '{}'. Supported: second, minute, hour, day, week, month, quarter, year",
                granularity
            )));
        }

        use crate::test_fixtures::cube_bridge::MockGranularityDefinition;
        Ok(Rc::new(
            MockGranularityDefinition::builder()
                .interval(granularity.clone())
                .build(),
        ) as Rc<dyn GranularityDefinition>)
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
