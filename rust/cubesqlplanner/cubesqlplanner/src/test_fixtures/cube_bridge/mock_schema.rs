use crate::test_fixtures::cube_bridge::{
    parse_schema_yaml, MockBaseTools, MockCubeDefinition, MockCubeEvaluator,
    MockDimensionDefinition, MockDriverTools, MockJoinGraph, MockJoinItemDefinition,
    MockMeasureDefinition, MockSegmentDefinition, MockSqlTemplatesRender,
};
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

#[derive(Clone)]
pub struct MockSchema {
    cubes: HashMap<String, MockCube>,
}

#[derive(Clone)]
pub struct MockCube {
    pub definition: MockCubeDefinition,
    pub measures: HashMap<String, Rc<MockMeasureDefinition>>,
    pub dimensions: HashMap<String, Rc<MockDimensionDefinition>>,
    pub segments: HashMap<String, Rc<MockSegmentDefinition>>,
}

impl MockSchema {
    pub fn from_yaml(yaml: &str) -> Result<Self, CubeError> {
        parse_schema_yaml(yaml)
    }

    pub fn get_cube(&self, name: &str) -> Option<&MockCube> {
        self.cubes.get(name)
    }

    pub fn get_dimension(
        &self,
        cube_name: &str,
        dimension_name: &str,
    ) -> Option<Rc<MockDimensionDefinition>> {
        self.cubes
            .get(cube_name)
            .and_then(|cube| cube.dimensions.get(dimension_name).cloned())
    }

    pub fn get_measure(
        &self,
        cube_name: &str,
        measure_name: &str,
    ) -> Option<Rc<MockMeasureDefinition>> {
        self.cubes
            .get(cube_name)
            .and_then(|cube| cube.measures.get(measure_name).cloned())
    }

    pub fn get_segment(
        &self,
        cube_name: &str,
        segment_name: &str,
    ) -> Option<Rc<MockSegmentDefinition>> {
        self.cubes
            .get(cube_name)
            .and_then(|cube| cube.segments.get(segment_name).cloned())
    }

    pub fn cube_names(&self) -> Vec<&String> {
        self.cubes.keys().collect()
    }

    pub fn create_evaluator(self) -> Rc<MockCubeEvaluator> {
        let mut primary_keys = std::collections::HashMap::new();

        for (cube_name, cube) in &self.cubes {
            let mut pk_dimensions = Vec::new();

            for (dim_name, dimension) in &cube.dimensions {
                if dimension.static_data().primary_key == Some(true) {
                    pk_dimensions.push(dim_name.clone());
                }
            }

            pk_dimensions.sort();

            if !pk_dimensions.is_empty() {
                primary_keys.insert(cube_name.clone(), pk_dimensions);
            }
        }

        Rc::new(MockCubeEvaluator::with_primary_keys(self, primary_keys))
    }

    pub fn create_base_tools(&self) -> Result<MockBaseTools, CubeError> {
        let join_graph = Rc::new(self.create_join_graph()?);
        let driver_tools = Rc::new(MockDriverTools::new());
        let result = MockBaseTools::builder()
            .join_graph(join_graph)
            .driver_tools(driver_tools)
            .build();
        Ok(result)
    }

    #[allow(dead_code)]
    pub fn create_evaluator_with_primary_keys(
        self,
        primary_keys: std::collections::HashMap<String, Vec<String>>,
    ) -> Rc<MockCubeEvaluator> {
        Rc::new(MockCubeEvaluator::with_primary_keys(self, primary_keys))
    }

    pub fn create_join_graph(&self) -> Result<MockJoinGraph, CubeError> {
        let cubes: Vec<Rc<MockCubeDefinition>> = self
            .cubes
            .values()
            .map(|mock_cube| Rc::new(mock_cube.definition.clone()))
            .collect();

        let mut primary_keys = HashMap::new();
        for (cube_name, cube) in &self.cubes {
            let mut pk_dimensions = Vec::new();
            for (dim_name, dimension) in &cube.dimensions {
                if dimension.static_data().primary_key == Some(true) {
                    pk_dimensions.push(dim_name.clone());
                }
            }
            pk_dimensions.sort();
            if !pk_dimensions.is_empty() {
                primary_keys.insert(cube_name.clone(), pk_dimensions);
            }
        }

        let evaluator = MockCubeEvaluator::with_primary_keys(self.clone(), primary_keys);

        let mut join_graph = MockJoinGraph::new();
        join_graph.compile(&cubes, &evaluator)?;

        Ok(join_graph)
    }

    pub fn create_evaluator_with_join_graph(self) -> Result<Rc<MockCubeEvaluator>, CubeError> {
        let mut primary_keys = HashMap::new();
        for (cube_name, cube) in &self.cubes {
            let mut pk_dimensions = Vec::new();
            for (dim_name, dimension) in &cube.dimensions {
                if dimension.static_data().primary_key == Some(true) {
                    pk_dimensions.push(dim_name.clone());
                }
            }
            pk_dimensions.sort();
            if !pk_dimensions.is_empty() {
                primary_keys.insert(cube_name.clone(), pk_dimensions);
            }
        }

        let join_graph = self.create_join_graph()?;

        Ok(Rc::new(MockCubeEvaluator::with_join_graph(
            self,
            primary_keys,
            join_graph,
        )))
    }
}

pub struct MockSchemaBuilder {
    cubes: HashMap<String, MockCube>,
}

impl MockSchemaBuilder {
    pub fn new() -> Self {
        Self {
            cubes: HashMap::new(),
        }
    }

    pub fn add_cube(self, name: impl Into<String>) -> MockCubeBuilder {
        MockCubeBuilder {
            schema_builder: self,
            cube_name: name.into(),
            cube_definition: None,
            measures: HashMap::new(),
            dimensions: HashMap::new(),
            segments: HashMap::new(),
            joins: HashMap::new(),
        }
    }

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

    pub fn build(self) -> MockSchema {
        MockSchema { cubes: self.cubes }
    }
}

impl Default for MockSchemaBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub struct MockCubeBuilder {
    schema_builder: MockSchemaBuilder,
    cube_name: String,
    cube_definition: Option<MockCubeDefinition>,
    measures: HashMap<String, Rc<MockMeasureDefinition>>,
    dimensions: HashMap<String, Rc<MockDimensionDefinition>>,
    segments: HashMap<String, Rc<MockSegmentDefinition>>,
    joins: HashMap<String, MockJoinItemDefinition>,
}

impl MockCubeBuilder {
    pub fn cube_def(mut self, definition: MockCubeDefinition) -> Self {
        self.cube_definition = Some(definition);
        self
    }

    pub fn add_dimension(
        mut self,
        name: impl Into<String>,
        definition: MockDimensionDefinition,
    ) -> Self {
        self.dimensions.insert(name.into(), Rc::new(definition));
        self
    }

    pub fn add_measure(
        mut self,
        name: impl Into<String>,
        definition: MockMeasureDefinition,
    ) -> Self {
        self.measures.insert(name.into(), Rc::new(definition));
        self
    }

    pub fn add_segment(
        mut self,
        name: impl Into<String>,
        definition: MockSegmentDefinition,
    ) -> Self {
        self.segments.insert(name.into(), Rc::new(definition));
        self
    }

    pub fn add_join(mut self, name: impl Into<String>, definition: MockJoinItemDefinition) -> Self {
        self.joins.insert(name.into(), definition);
        self
    }

    pub fn finish_cube(mut self) -> MockSchemaBuilder {
        let mut cube_def = self.cube_definition.unwrap_or_else(|| {
            MockCubeDefinition::builder()
                .name(self.cube_name.clone())
                .sql_table(format!("public.{}", self.cube_name))
                .build()
        });

        let mut all_joins = cube_def.joins().clone();
        all_joins.extend(self.joins);

        let static_data = cube_def.static_data();
        cube_def = MockCubeDefinition::builder()
            .name(static_data.name.clone())
            .sql_alias(static_data.sql_alias.clone())
            .is_view(static_data.is_view)
            .is_calendar(static_data.is_calendar)
            .join_map(static_data.join_map.clone())
            .joins(all_joins)
            .build();

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

pub struct ViewCube {
    pub join_path: String,
    pub includes: Vec<String>,
}

pub struct MockViewBuilder {
    schema_builder: MockSchemaBuilder,
    view_name: String,
    view_cubes: Vec<ViewCube>,
    measures: HashMap<String, Rc<MockMeasureDefinition>>,
    dimensions: HashMap<String, Rc<MockDimensionDefinition>>,
    segments: HashMap<String, Rc<MockSegmentDefinition>>,
}

impl MockViewBuilder {
    pub fn include_cube(mut self, join_path: impl Into<String>, includes: Vec<String>) -> Self {
        self.view_cubes.push(ViewCube {
            join_path: join_path.into(),
            includes,
        });
        self
    }

    pub fn add_dimension(
        mut self,
        name: impl Into<String>,
        definition: MockDimensionDefinition,
    ) -> Self {
        self.dimensions.insert(name.into(), Rc::new(definition));
        self
    }

    pub fn add_measure(
        mut self,
        name: impl Into<String>,
        definition: MockMeasureDefinition,
    ) -> Self {
        self.measures.insert(name.into(), Rc::new(definition));
        self
    }

    #[allow(dead_code)]
    pub fn add_segment(
        mut self,
        name: impl Into<String>,
        definition: MockSegmentDefinition,
    ) -> Self {
        self.segments.insert(name.into(), Rc::new(definition));
        self
    }

    pub fn finish_view(mut self) -> MockSchemaBuilder {
        let mut all_dimensions = self.dimensions;
        let mut all_measures = self.measures;
        let mut all_segments = self.segments;

        for view_cube in &self.view_cubes {
            let join_path_parts: Vec<&str> = view_cube.join_path.split('.').collect();
            let target_cube_name = join_path_parts.last().unwrap();

            if let Some(source_cube) = self.schema_builder.cubes.get(*target_cube_name) {
                let members_to_include: Vec<String> = if view_cube.includes.is_empty() {
                    let mut all_members = Vec::new();
                    all_members.extend(source_cube.dimensions.keys().cloned());
                    all_members.extend(source_cube.measures.keys().cloned());
                    all_members.extend(source_cube.segments.keys().cloned());
                    all_members
                } else {
                    view_cube.includes.clone()
                };

                for member_name in &members_to_include {
                    if let Some(dimension) = source_cube.dimensions.get(member_name) {
                        let view_member_sql =
                            format!("{{{}.{}}}", view_cube.join_path, member_name);

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

                for member_name in &members_to_include {
                    if let Some(measure) = source_cube.measures.get(member_name) {
                        let view_member_sql =
                            format!("{{{}.{}}}", view_cube.join_path, member_name);

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

                for member_name in &members_to_include {
                    if source_cube.segments.contains_key(member_name) {
                        let view_member_sql =
                            format!("{{{}.{}}}", view_cube.join_path, member_name);

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
    use crate::cube_bridge::join_item_definition::JoinItemDefinition;
    use crate::cube_bridge::measure_definition::MeasureDefinition;
    use crate::cube_bridge::segment_definition::SegmentDefinition;

    #[test]
    fn test_complex_schema_with_join_relationships() {
        let schema = MockSchemaBuilder::new()
            .add_cube("countries")
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
            .finish_cube()
            .add_cube("users")
            .add_dimension(
                "id",
                MockDimensionDefinition::builder()
                    .dimension_type("number".to_string())
                    .sql("id".to_string())
                    .primary_key(Some(true))
                    .build(),
            )
            .add_dimension(
                "country_id",
                MockDimensionDefinition::builder()
                    .dimension_type("number".to_string())
                    .sql("country_id".to_string())
                    .build(),
            )
            .add_join(
                "countries",
                MockJoinItemDefinition::builder()
                    .relationship("many_to_one".to_string())
                    .sql("{CUBE}.country_id = {countries.id}".to_string())
                    .build(),
            )
            .finish_cube()
            .add_cube("orders")
            .add_dimension(
                "id",
                MockDimensionDefinition::builder()
                    .dimension_type("number".to_string())
                    .sql("id".to_string())
                    .primary_key(Some(true))
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
            .add_join(
                "users",
                MockJoinItemDefinition::builder()
                    .relationship("many_to_one".to_string())
                    .sql("{CUBE}.user_id = {users.id}".to_string())
                    .build(),
            )
            .finish_cube()
            .build();

        // Verify all cubes exist
        assert_eq!(schema.cube_names().len(), 3);

        // Verify countries has no joins
        let countries_cube = schema.get_cube("countries").unwrap();
        assert_eq!(countries_cube.definition.joins().len(), 0);

        // Verify users has join to countries
        let users_cube = schema.get_cube("users").unwrap();
        assert_eq!(users_cube.definition.joins().len(), 1);
        assert!(users_cube.definition.get_join("countries").is_some());

        // Verify orders has join to users
        let orders_cube = schema.get_cube("orders").unwrap();
        assert_eq!(orders_cube.definition.joins().len(), 1);
        assert!(orders_cube.definition.get_join("users").is_some());

        // Verify join SQL
        let orders_users_join = orders_cube.definition.get_join("users").unwrap();
        let sql = orders_users_join.sql().unwrap();
        assert_eq!(sql.args_names(), &vec!["CUBE", "users"]);
    }

    #[test]
    fn test_cube_with_multiple_joins_via_builder() {
        let schema = MockSchemaBuilder::new()
            .add_cube("orders")
            .add_dimension(
                "id",
                MockDimensionDefinition::builder()
                    .dimension_type("number".to_string())
                    .sql("id".to_string())
                    .build(),
            )
            .add_join(
                "users",
                MockJoinItemDefinition::builder()
                    .relationship("many_to_one".to_string())
                    .sql("{CUBE}.user_id = {users.id}".to_string())
                    .build(),
            )
            .add_join(
                "products",
                MockJoinItemDefinition::builder()
                    .relationship("many_to_one".to_string())
                    .sql("{CUBE}.product_id = {products.id}".to_string())
                    .build(),
            )
            .add_join(
                "warehouses",
                MockJoinItemDefinition::builder()
                    .relationship("many_to_one".to_string())
                    .sql("{CUBE}.warehouse_id = {warehouses.id}".to_string())
                    .build(),
            )
            .finish_cube()
            .build();

        let orders_cube = schema.get_cube("orders").unwrap();
        assert_eq!(orders_cube.definition.joins().len(), 3);
        assert!(orders_cube.definition.get_join("users").is_some());
        assert!(orders_cube.definition.get_join("products").is_some());
        assert!(orders_cube.definition.get_join("warehouses").is_some());
    }

    #[test]
    fn test_schema_with_join_graph_integration() {
        use crate::cube_bridge::join_hints::JoinHintItem;

        // Small schema: orders -> users (one join)
        let schema = MockSchemaBuilder::new()
            .add_cube("users")
            .add_dimension(
                "id",
                MockDimensionDefinition::builder()
                    .dimension_type("number".to_string())
                    .sql("id".to_string())
                    .primary_key(Some(true))
                    .build(),
            )
            .finish_cube()
            .add_cube("orders")
            .add_dimension(
                "id",
                MockDimensionDefinition::builder()
                    .dimension_type("number".to_string())
                    .sql("id".to_string())
                    .primary_key(Some(true))
                    .build(),
            )
            .add_dimension(
                "user_id",
                MockDimensionDefinition::builder()
                    .dimension_type("number".to_string())
                    .sql("user_id".to_string())
                    .build(),
            )
            .add_join(
                "users",
                MockJoinItemDefinition::builder()
                    .relationship("many_to_one".to_string())
                    .sql("{CUBE}.user_id = {users.id}".to_string())
                    .build(),
            )
            .finish_cube()
            .build();

        // Verify create_join_graph() succeeds
        let join_graph_result = schema.create_join_graph();
        assert!(
            join_graph_result.is_ok(),
            "create_join_graph should succeed"
        );

        // Verify create_evaluator_with_join_graph() succeeds
        let evaluator_result = schema.create_evaluator_with_join_graph();
        assert!(
            evaluator_result.is_ok(),
            "create_evaluator_with_join_graph should succeed"
        );
        let evaluator = evaluator_result.unwrap();

        // Verify evaluator.join_graph() returns Some(graph)
        assert!(
            evaluator.join_graph().is_some(),
            "Evaluator should have join graph"
        );
        let graph = evaluator.join_graph().unwrap();

        // Verify graph.build_join() works
        let cubes = vec![
            JoinHintItem::Single("orders".to_string()),
            JoinHintItem::Single("users".to_string()),
        ];
        let join_def_result = graph.build_join(cubes);
        assert!(
            join_def_result.is_ok(),
            "graph.build_join should succeed for orders -> users"
        );
    }
}
