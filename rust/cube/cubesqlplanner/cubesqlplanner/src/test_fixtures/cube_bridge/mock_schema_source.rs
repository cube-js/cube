use crate::cube_bridge::access_policy_definition::AccessPolicyDefinition;
use crate::cube_bridge::cube_definition::{CubeDefinition, CubeDefinitionStatic};
use crate::cube_bridge::cube_join_definition::CubeJoinDefinition;
use crate::cube_bridge::dimension_definition::DimensionDefinition;
use crate::cube_bridge::measure_definition::MeasureDefinition;
use crate::cube_bridge::member_sql::MemberSql;
use crate::cube_bridge::pre_aggregation_description::PreAggregationDescription;
use crate::cube_bridge::schema_source::{SchemaSource, SchemaSourceStatic};
use crate::cube_bridge::segment_definition::SegmentDefinition;
use crate::cube_bridge::view_filter_definition::ViewFilterDefinition;
use crate::cube_bridge::view_included_member::ViewIncludedMember;
use crate::test_fixtures::cube_bridge::{
    MockCubeDefinition, MockDimensionDefinition, MockMeasureDefinition,
    MockPreAggregationDescription, MockSchema, MockSegmentDefinition,
};
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

/// `CubeDefinition` adapter exposing real members from a `MockSchema`.
///
/// `MockCubeDefinition` on its own returns empty vectors for
/// `measures()`/`dimensions()`/etc — they exist on `MockSchema` as
/// separate maps. This wrapper holds Rcs to the actual members and
/// surfaces them through the trait so `SchemaModelBuilder` sees the
/// real model.
pub struct MockSchemaSourceCube {
    definition: Rc<MockCubeDefinition>,
    measures: Vec<Rc<dyn MeasureDefinition>>,
    dimensions: Vec<Rc<dyn DimensionDefinition>>,
    segments: Vec<Rc<dyn SegmentDefinition>>,
    pre_aggregations: Vec<Rc<dyn PreAggregationDescription>>,
}

impl MockSchemaSourceCube {
    pub fn new(
        definition: Rc<MockCubeDefinition>,
        measures: Vec<Rc<MockMeasureDefinition>>,
        dimensions: Vec<Rc<MockDimensionDefinition>>,
        segments: Vec<Rc<MockSegmentDefinition>>,
        pre_aggregations: Vec<Rc<MockPreAggregationDescription>>,
    ) -> Self {
        Self {
            definition,
            measures: measures
                .into_iter()
                .map(|m| m as Rc<dyn MeasureDefinition>)
                .collect(),
            dimensions: dimensions
                .into_iter()
                .map(|d| d as Rc<dyn DimensionDefinition>)
                .collect(),
            segments: segments
                .into_iter()
                .map(|s| s as Rc<dyn SegmentDefinition>)
                .collect(),
            pre_aggregations: pre_aggregations
                .into_iter()
                .map(|p| p as Rc<dyn PreAggregationDescription>)
                .collect(),
        }
    }
}

impl CubeDefinition for MockSchemaSourceCube {
    fn static_data(&self) -> &CubeDefinitionStatic {
        <MockCubeDefinition as CubeDefinition>::static_data(&self.definition)
    }

    fn has_sql_table(&self) -> Result<bool, CubeError> {
        self.definition.has_sql_table()
    }
    fn sql_table(&self) -> Result<Option<Rc<dyn MemberSql>>, CubeError> {
        self.definition.sql_table()
    }
    fn has_sql(&self) -> Result<bool, CubeError> {
        self.definition.has_sql()
    }
    fn sql(&self) -> Result<Option<Rc<dyn MemberSql>>, CubeError> {
        self.definition.sql()
    }

    fn measures(&self) -> Result<Vec<Rc<dyn MeasureDefinition>>, CubeError> {
        Ok(self.measures.clone())
    }

    fn dimensions(&self) -> Result<Vec<Rc<dyn DimensionDefinition>>, CubeError> {
        Ok(self.dimensions.clone())
    }

    fn segments(&self) -> Result<Vec<Rc<dyn SegmentDefinition>>, CubeError> {
        Ok(self.segments.clone())
    }

    fn has_joins(&self) -> Result<bool, CubeError> {
        Ok(false)
    }
    fn joins(&self) -> Result<Option<Vec<Rc<dyn CubeJoinDefinition>>>, CubeError> {
        // Raw cube joins for Model (post-`prepareJoins` shape) are a
        // separate bridge from MockJoinItemDefinition that MockSchema
        // currently uses for the graph builder. Left as None — the
        // model build still succeeds; tests targeting Model.joins
        // will need to extend this wrapper.
        Ok(None)
    }

    fn has_pre_aggregations(&self) -> Result<bool, CubeError> {
        Ok(!self.pre_aggregations.is_empty())
    }
    fn pre_aggregations(
        &self,
    ) -> Result<Option<Vec<Rc<dyn PreAggregationDescription>>>, CubeError> {
        if self.pre_aggregations.is_empty() {
            Ok(None)
        } else {
            Ok(Some(self.pre_aggregations.clone()))
        }
    }

    fn has_access_policies(&self) -> Result<bool, CubeError> {
        Ok(false)
    }
    fn access_policies(&self) -> Result<Option<Vec<Rc<dyn AccessPolicyDefinition>>>, CubeError> {
        Ok(None)
    }

    fn has_included_members(&self) -> Result<bool, CubeError> {
        Ok(false)
    }
    fn included_members(&self) -> Result<Option<Vec<Rc<dyn ViewIncludedMember>>>, CubeError> {
        Ok(None)
    }

    fn has_default_filters(&self) -> Result<bool, CubeError> {
        self.definition.has_default_filters()
    }
    fn default_filters(&self) -> Result<Option<Vec<Rc<dyn ViewFilterDefinition>>>, CubeError> {
        self.definition.default_filters()
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self
    }
}

/// `SchemaSource` adapter on top of `MockSchema`, used by `MockSchema::build_model()`.
pub struct MockSchemaSource {
    static_data: SchemaSourceStatic,
    cubes: Vec<Rc<dyn CubeDefinition>>,
}

impl MockSchemaSource {
    pub fn from_schema(schema: &MockSchema) -> Self {
        // Collect primary keys from dimensions marked primary_key=true.
        let mut primary_keys = std::collections::HashMap::new();
        for (cube_name, cube) in schema.cubes_iter() {
            let pks: Vec<String> = cube
                .dimensions
                .iter()
                .filter(|(_, d)| d.static_data().primary_key == Some(true))
                .map(|(n, _)| n.clone())
                .collect();
            if !pks.is_empty() {
                primary_keys.insert(cube_name.clone(), pks);
            }
        }

        // Build a Rc<dyn CubeDefinition> per cube via the wrapper.
        let cubes: Vec<Rc<dyn CubeDefinition>> = schema
            .cubes_iter()
            .map(|(_, cube)| {
                let wrapper = MockSchemaSourceCube::new(
                    Rc::new(cube.definition.clone()),
                    cube.measures.values().cloned().collect(),
                    cube.dimensions.values().cloned().collect(),
                    cube.segments.values().cloned().collect(),
                    cube.pre_aggregations
                        .iter()
                        .map(|(_, p)| p.clone())
                        .collect(),
                );
                Rc::new(wrapper) as Rc<dyn CubeDefinition>
            })
            .collect();

        Self {
            static_data: SchemaSourceStatic { primary_keys },
            cubes,
        }
    }
}

impl SchemaSource for MockSchemaSource {
    fn static_data(&self) -> &SchemaSourceStatic {
        &self.static_data
    }

    fn cubes(&self) -> Result<Vec<Rc<dyn CubeDefinition>>, CubeError> {
        Ok(self.cubes.clone())
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_model_from_simple_yaml_fixture() {
        let schema = MockSchema::from_yaml_file("common/integration_basic.yaml");
        let model = schema
            .build_model()
            .expect("model build should succeed for the fixture");

        // Sanity: cubes from the YAML are present, members carry names
        // (not the empty-string default that would slip through if the
        // YAML→Mock builder didn't stamp them).
        assert!(
            !model.cubes.is_empty(),
            "model should have at least one cube"
        );
        for cube in model.cubes_iter() {
            assert_eq!(cube.name.as_str(), cube.name.as_str().trim());
            for measure in cube.measures_iter() {
                assert!(
                    !measure.path.name().is_empty(),
                    "measure on cube `{}` has empty name — YAML→Mock pipeline lost it",
                    cube.name
                );
            }
            for dimension in cube.dimensions_iter() {
                assert!(
                    !dimension.path.name().is_empty(),
                    "dimension on cube `{}` has empty name — YAML→Mock pipeline lost it",
                    cube.name
                );
            }
        }
    }

    #[test]
    fn builds_model_with_rank_and_number_agg_measures() {
        use crate::model::{MeasureType, MultiStageKind};

        // `rank` / `numberAgg` are multi-stage-only measure types; the
        // build must not reject them with "Unknown measure type".
        let schema = MockSchema::from_yaml_file("common/measure_kind_tests.yaml");
        let model = schema
            .build_model()
            .expect("model build should succeed for rank/numberAgg measures");

        let cube = model
            .cube_by_str("test_measures")
            .expect("test_measures cube present");

        let rank = cube.measure("rank_measure").expect("rank_measure present");
        assert_eq!(rank.measure_type, MeasureType::Rank);

        let number_agg = cube.measure("number_agg").expect("number_agg present");
        assert_eq!(number_agg.measure_type, MeasureType::NumberAgg);

        // A multi-stage `rank` measure resolves to a filtering stage.
        let ms_schema = MockSchema::from_yaml_file("common/integration_multi_stage.yaml");
        let ms_model = ms_schema
            .build_model()
            .expect("multi-stage model build should succeed");
        let rank_spec = ms_model
            .cube_by_str("orders")
            .and_then(|c| c.measure("amount_rank"))
            .expect("orders.amount_rank present")
            .multi_stage
            .as_ref()
            .expect("amount_rank is multi-stage");
        assert_eq!(rank_spec.kind, MultiStageKind::Filtering);
    }
}
