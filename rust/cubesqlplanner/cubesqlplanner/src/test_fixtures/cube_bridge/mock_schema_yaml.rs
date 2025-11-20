use crate::test_fixtures::cube_bridge::{
    MockCubeDefinition, MockDimensionDefinition, MockJoinItemDefinition, MockMeasureDefinition,
    MockSchema, MockSchemaBuilder, MockSegmentDefinition,
};
use cubenativeutils::CubeError;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct YamlSchema {
    cubes: Vec<YamlCube>,
    #[serde(default)]
    views: Vec<YamlView>,
}

#[derive(Debug, Deserialize)]
struct YamlCube {
    name: String,
    sql: String,
    #[serde(default)]
    public: Option<bool>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    joins: Vec<YamlJoin>,
    #[serde(default)]
    dimensions: Vec<YamlDimension>,
    #[serde(default)]
    measures: Vec<YamlMeasure>,
    #[serde(default)]
    segments: Vec<YamlSegment>,
}

#[derive(Debug, Deserialize)]
struct YamlJoin {
    name: String,
    sql: String,
    relationship: String,
}

#[derive(Debug, Deserialize)]
struct YamlDimension {
    name: String,
    sql: String,
    #[serde(rename = "type")]
    dimension_type: String,
    #[serde(default)]
    primary_key: Option<bool>,
    #[serde(default)]
    sub_query: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct YamlMeasure {
    name: String,
    #[serde(rename = "type")]
    measure_type: String,
    #[serde(default)]
    sql: Option<String>,
    #[serde(default)]
    filters: Vec<YamlFilter>,
    #[serde(default)]
    format: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    multi_stage: Option<bool>,
    #[serde(default)]
    time_shift: Vec<YamlTimeShift>,
}

#[derive(Debug, Deserialize)]
struct YamlFilter {
    sql: String,
}

#[derive(Debug, Deserialize)]
struct YamlTimeShift {
    time_dimension: String,
    interval: String,
    #[serde(rename = "type")]
    shift_type: String,
}

#[derive(Debug, Deserialize)]
struct YamlSegment {
    name: String,
    sql: String,
}

#[derive(Debug, Deserialize)]
struct YamlView {
    name: String,
    cubes: Vec<YamlViewCube>,
}

#[derive(Debug, Deserialize)]
struct YamlViewCube {
    join_path: String,
    #[serde(default)]
    includes: Vec<String>,
    #[serde(default)]
    prefix: Option<bool>,
}

pub fn parse_schema_yaml(yaml: &str) -> Result<MockSchema, CubeError> {
    let schema: YamlSchema = serde_yaml::from_str(yaml)
        .map_err(|e| CubeError::user(format!("Failed to parse YAML: {}", e)))?;

    let mut builder = MockSchemaBuilder::new();

    for cube in schema.cubes {
        let cube_def = MockCubeDefinition::builder()
            .name(cube.name.clone())
            .sql_table(cube.sql.clone())
            .build();

        let mut cube_builder = builder.add_cube(cube.name).cube_def(cube_def);

        for join in cube.joins {
            let join_def = MockJoinItemDefinition::builder()
                .relationship(join.relationship)
                .sql(join.sql)
                .build();
            cube_builder = cube_builder.add_join(join.name, join_def);
        }

        for dimension in cube.dimensions {
            let dim_def = MockDimensionDefinition::builder()
                .dimension_type(dimension.dimension_type)
                .sql(dimension.sql)
                .primary_key(dimension.primary_key)
                .sub_query(dimension.sub_query)
                .build();

            cube_builder = cube_builder.add_dimension(dimension.name, dim_def);
        }

        for measure in cube.measures {
            let sql = measure
                .sql
                .unwrap_or_else(|| match measure.measure_type.as_str() {
                    "count" => "COUNT(*)".to_string(),
                    _ => "".to_string(),
                });

            let measure_def = MockMeasureDefinition::builder()
                .measure_type(measure.measure_type)
                .sql(sql)
                .build();

            cube_builder = cube_builder.add_measure(measure.name, measure_def);
        }

        for segment in cube.segments {
            let segment_def = MockSegmentDefinition::builder().sql(segment.sql).build();
            cube_builder = cube_builder.add_segment(segment.name, segment_def);
        }

        builder = cube_builder.finish_cube();
    }

    for view in schema.views {
        let mut view_builder = builder.add_view(view.name);

        for view_cube in view.cubes {
            view_builder = view_builder.include_cube(view_cube.join_path, view_cube.includes);
        }

        builder = view_builder.finish_view();
    }

    Ok(builder.build())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cube_bridge::dimension_definition::DimensionDefinition;
    use crate::cube_bridge::measure_definition::MeasureDefinition;

    #[test]
    fn test_parse_basic_cube() {
        let yaml = r#"
cubes:
  - name: orders
    sql: "SELECT * FROM orders"
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: status
        sql: status
        type: string
    measures:
      - name: count
        type: count
"#;

        let schema = parse_schema_yaml(yaml).unwrap();
        assert!(schema.get_cube("orders").is_some());

        let id_dim = schema.get_dimension("orders", "id").unwrap();
        assert_eq!(id_dim.static_data().dimension_type, "number");
        assert_eq!(id_dim.static_data().primary_key, Some(true));

        let count_measure = schema.get_measure("orders", "count").unwrap();
        assert_eq!(count_measure.static_data().measure_type, "count");
    }

    #[test]
    fn test_parse_cube_with_joins() {
        let yaml = r#"
cubes:
  - name: orders
    sql: "SELECT * FROM orders"
    joins:
      - name: users
        sql: "{CUBE}.user_id = {users.id}"
        relationship: many_to_one
    dimensions:
      - name: id
        sql: id
        type: number
"#;

        let schema = parse_schema_yaml(yaml).unwrap();
        let cube = schema.get_cube("orders").unwrap();
        assert_eq!(cube.definition.joins().len(), 1);
        assert!(cube.definition.get_join("users").is_some());
    }

    #[test]
    fn test_parse_view() {
        let yaml = r#"
cubes:
  - name: orders
    sql: "SELECT * FROM orders"
    dimensions:
      - name: id
        sql: id
        type: number
    measures:
      - name: count
        type: count
views:
  - name: orders_view
    cubes:
      - join_path: orders
        includes:
          - id
          - count
"#;

        let schema = parse_schema_yaml(yaml).unwrap();
        assert!(schema.get_cube("orders_view").is_some());

        let view_cube = schema.get_cube("orders_view").unwrap();
        assert!(view_cube.definition.static_data().is_view == Some(true));

        assert!(schema.get_dimension("orders_view", "id").is_some());
        assert!(schema.get_measure("orders_view", "count").is_some());
    }

    #[test]
    fn test_parse_sub_query_dimension() {
        let yaml = r#"
cubes:
  - name: orders
    sql: "SELECT * FROM orders"
    dimensions:
      - name: amount
        sql: "{line_items.total}"
        type: number
        sub_query: true
"#;

        let schema = parse_schema_yaml(yaml).unwrap();
        let dim = schema.get_dimension("orders", "amount").unwrap();
        assert_eq!(dim.static_data().sub_query, Some(true));
    }

    #[test]
    fn test_parse_multi_stage_example() {
        let yaml = r#"
cubes:
  - name: orders
    sql: "SELECT * FROM orders"
    joins:
      - name: line_items
        sql: "{CUBE}.ID = {line_items}.order_id"
        relationship: many_to_one
    dimensions:
      - name: id
        sql: ID
        type: number
        primary_key: true
      - name: status
        sql: STATUS
        type: string
      - name: date
        sql: CREATED_AT
        type: time
      - name: amount
        sql: "{line_items.total_amount}"
        type: number
        sub_query: true
    measures:
      - name: count
        type: count
      - name: revenue
        sql: "CASE WHEN {CUBE}.status = 'completed' THEN {CUBE.amount} END"
        type: sum

  - name: line_items
    sql: "SELECT * FROM line_items"
    joins:
      - name: products
        sql: "{CUBE}.PRODUCT_ID = {products}.ID"
        relationship: many_to_one
    dimensions:
      - name: id
        sql: ID
        type: number
        primary_key: true
      - name: price
        sql: "{products.price}"
        type: number
    measures:
      - name: count
        type: count
      - name: total_amount
        sql: "{price}"
        type: sum

  - name: products
    sql: "SELECT * FROM products"
    dimensions:
      - name: id
        sql: ID
        type: number
        primary_key: true
      - name: price
        sql: PRICE
        type: number
    measures:
      - name: count
        type: count

views:
  - name: orders_view
    cubes:
      - join_path: orders
        includes:
          - date
          - revenue
      - join_path: line_items.products
        includes:
          - price
"#;

        let schema = parse_schema_yaml(yaml).unwrap();

        assert!(schema.get_cube("orders").is_some());
        assert!(schema.get_cube("line_items").is_some());
        assert!(schema.get_cube("products").is_some());
        assert!(schema.get_cube("orders_view").is_some());

        let orders_cube = schema.get_cube("orders").unwrap();
        assert_eq!(orders_cube.definition.joins().len(), 1);

        let line_items_cube = schema.get_cube("line_items").unwrap();
        assert_eq!(line_items_cube.definition.joins().len(), 1);

        let amount_dim = schema.get_dimension("orders", "amount").unwrap();
        assert_eq!(amount_dim.static_data().sub_query, Some(true));

        let view_date = schema.get_dimension("orders_view", "date").unwrap();
        let date_sql = view_date.sql().unwrap().unwrap();
        assert_eq!(date_sql.args_names(), &vec!["orders"]);

        let view_revenue = schema.get_measure("orders_view", "revenue").unwrap();
        let revenue_sql = view_revenue.sql().unwrap().unwrap();
        assert_eq!(revenue_sql.args_names(), &vec!["orders"]);

        let view_price = schema.get_dimension("orders_view", "price").unwrap();
        let price_sql = view_price.sql().unwrap().unwrap();
        assert_eq!(price_sql.args_names(), &vec!["line_items"]);
    }
}
