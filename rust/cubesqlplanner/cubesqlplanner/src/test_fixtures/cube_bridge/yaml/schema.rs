use crate::test_fixtures::cube_bridge::yaml::{
    YamlDimensionDefinition, YamlMeasureDefinition, YamlSegmentDefinition,
};
use crate::test_fixtures::cube_bridge::{
    MockCubeDefinition, MockJoinItemDefinition, MockSchema, MockSchemaBuilder,
};
use cubenativeutils::CubeError;
use serde::Deserialize;
use std::rc::Rc;

#[derive(Debug, Deserialize)]
pub struct YamlSchema {
    cubes: Vec<YamlCube>,
    #[serde(default)]
    views: Vec<YamlView>,
}

#[derive(Debug, Deserialize)]
struct YamlCube {
    name: String,
    sql: String,
    #[serde(default)]
    joins: Vec<YamlJoin>,
    #[serde(default)]
    dimensions: Vec<YamlDimensionEntry>,
    #[serde(default)]
    measures: Vec<YamlMeasureEntry>,
    #[serde(default)]
    segments: Vec<YamlSegmentEntry>,
}

#[derive(Debug, Deserialize)]
struct YamlJoin {
    name: String,
    sql: String,
    relationship: String,
}

#[derive(Debug, Deserialize)]
struct YamlDimensionEntry {
    name: String,
    #[serde(flatten)]
    definition: YamlDimensionDefinition,
}

#[derive(Debug, Deserialize)]
struct YamlMeasureEntry {
    name: String,
    #[serde(flatten)]
    definition: YamlMeasureDefinition,
}

#[derive(Debug, Deserialize)]
struct YamlSegmentEntry {
    name: String,
    #[serde(flatten)]
    definition: YamlSegmentDefinition,
}

#[derive(Debug, Deserialize)]
struct YamlView {
    name: String,
    cubes: Vec<YamlViewCube>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum YamlIncludes {
    All(String),
    List(Vec<String>),
}

#[derive(Debug, Deserialize)]
struct YamlViewCube {
    join_path: String,
    #[serde(default)]
    includes: Option<YamlIncludes>,
}

impl YamlSchema {
    pub fn build(self) -> Result<MockSchema, CubeError> {
        let mut builder = MockSchemaBuilder::new();

        for cube in self.cubes {
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

            for dim_entry in cube.dimensions {
                let dim_rc = dim_entry.definition.build();
                let dim_def = Rc::try_unwrap(dim_rc)
                    .ok()
                    .expect("Rc should have single owner");
                cube_builder = cube_builder.add_dimension(dim_entry.name, dim_def);
            }

            for meas_entry in cube.measures {
                let meas_rc = meas_entry.definition.build();
                let meas_def = Rc::try_unwrap(meas_rc)
                    .ok()
                    .expect("Rc should have single owner");
                cube_builder = cube_builder.add_measure(meas_entry.name, meas_def);
            }

            for seg_entry in cube.segments {
                let seg_rc = seg_entry.definition.build();
                let seg_def = Rc::try_unwrap(seg_rc)
                    .ok()
                    .expect("Rc should have single owner");
                cube_builder = cube_builder.add_segment(seg_entry.name, seg_def);
            }

            builder = cube_builder.finish_cube();
        }

        for view in self.views {
            let mut view_builder = builder.add_view(view.name);

            for view_cube in view.cubes {
                let includes = match view_cube.includes {
                    Some(YamlIncludes::All(ref s)) if s == "*" => vec![],
                    Some(YamlIncludes::List(list)) => list,
                    _ => vec![],
                };
                view_builder = view_builder.include_cube(view_cube.join_path, includes);
            }

            builder = view_builder.finish_view();
        }

        Ok(builder.build())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cube_bridge::dimension_definition::DimensionDefinition;
    use crate::cube_bridge::measure_definition::MeasureDefinition;
    use indoc::indoc;

    #[test]
    fn test_parse_basic_cube() {
        let yaml = indoc! {r#"
            cubes:
              - name: orders
                sql: "SELECT * FROM orders"
                dimensions:
                  - name: id
                    type: number
                    sql: id
                    primary_key: true
                  - name: status
                    type: string
                    sql: status
                measures:
                  - name: count
                    type: count
        "#};

        let yaml_schema: YamlSchema = serde_yaml::from_str(yaml).unwrap();
        let schema = yaml_schema.build().unwrap();

        assert!(schema.get_cube("orders").is_some());

        let id_dim = schema.get_dimension("orders", "id").unwrap();
        assert_eq!(id_dim.static_data().dimension_type, "number");
        assert_eq!(id_dim.static_data().primary_key, Some(true));

        let count_measure = schema.get_measure("orders", "count").unwrap();
        assert_eq!(count_measure.static_data().measure_type, "count");
    }

    #[test]
    fn test_parse_cube_with_joins() {
        let yaml = indoc! {r#"
            cubes:
              - name: orders
                sql: "SELECT * FROM orders"
                joins:
                  - name: users
                    sql: "{CUBE}.user_id = {users.id}"
                    relationship: many_to_one
                dimensions:
                  - name: id
                    type: number
                    sql: id
        "#};

        let yaml_schema: YamlSchema = serde_yaml::from_str(yaml).unwrap();
        let schema = yaml_schema.build().unwrap();

        let cube = schema.get_cube("orders").unwrap();
        assert_eq!(cube.definition.joins().len(), 1);
        assert!(cube.definition.get_join("users").is_some());
    }

    #[test]
    fn test_parse_view() {
        let yaml = indoc! {r#"
            cubes:
              - name: orders
                sql: "SELECT * FROM orders"
                dimensions:
                  - name: id
                    type: number
                    sql: id
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
        "#};

        let yaml_schema: YamlSchema = serde_yaml::from_str(yaml).unwrap();
        let schema = yaml_schema.build().unwrap();

        assert!(schema.get_cube("orders_view").is_some());

        let view_cube = schema.get_cube("orders_view").unwrap();
        assert!(view_cube.definition.static_data().is_view == Some(true));

        assert!(schema.get_dimension("orders_view", "id").is_some());
        assert!(schema.get_measure("orders_view", "count").is_some());
    }

    #[test]
    fn test_parse_view_with_wildcard_includes() {
        let yaml = indoc! {r#"
            cubes:
              - name: orders
                sql: "SELECT * FROM orders"
                dimensions:
                  - name: id
                    type: number
                    sql: id
                  - name: status
                    type: string
                    sql: status
                measures:
                  - name: count
                    type: count
                  - name: total
                    type: sum
                    sql: amount
            views:
              - name: orders_view
                cubes:
                  - join_path: orders
                    includes: "*"
        "#};

        let yaml_schema: YamlSchema = serde_yaml::from_str(yaml).unwrap();
        let schema = yaml_schema.build().unwrap();

        assert!(schema.get_cube("orders_view").is_some());

        let view_cube = schema.get_cube("orders_view").unwrap();
        assert!(view_cube.definition.static_data().is_view == Some(true));

        assert!(schema.get_dimension("orders_view", "id").is_some());
        assert!(schema.get_dimension("orders_view", "status").is_some());
        assert!(schema.get_measure("orders_view", "count").is_some());
        assert!(schema.get_measure("orders_view", "total").is_some());
    }

    #[test]
    fn test_parse_sub_query_dimension() {
        let yaml = indoc! {r#"
            cubes:
              - name: orders
                sql: "SELECT * FROM orders"
                dimensions:
                  - name: amount
                    type: number
                    sql: "{line_items.total}"
                    sub_query: true
        "#};

        let yaml_schema: YamlSchema = serde_yaml::from_str(yaml).unwrap();
        let schema = yaml_schema.build().unwrap();

        let dim = schema.get_dimension("orders", "amount").unwrap();
        assert_eq!(dim.static_data().sub_query, Some(true));
    }

    #[test]
    fn test_parse_multi_stage_example() {
        let yaml = indoc! {r#"
            cubes:
              - name: orders
                sql: "SELECT * FROM orders"
                joins:
                  - name: line_items
                    sql: "{CUBE}.ID = {line_items}.order_id"
                    relationship: many_to_one
                dimensions:
                  - name: id
                    type: number
                    sql: ID
                    primary_key: true
                  - name: status
                    type: string
                    sql: STATUS
                  - name: date
                    type: time
                    sql: CREATED_AT
                  - name: amount
                    type: number
                    sql: "{line_items.total_amount}"
                    sub_query: true
                measures:
                  - name: count
                    type: count
                  - name: revenue
                    type: sum
                    sql: "CASE WHEN {CUBE}.status = 'completed' THEN {CUBE.amount} END"

              - name: line_items
                sql: "SELECT * FROM line_items"
                joins:
                  - name: products
                    sql: "{CUBE}.PRODUCT_ID = {products}.ID"
                    relationship: many_to_one
                dimensions:
                  - name: id
                    type: number
                    sql: ID
                    primary_key: true
                  - name: price
                    type: number
                    sql: "{products.price}"
                measures:
                  - name: count
                    type: count
                  - name: total_amount
                    type: sum
                    sql: "{price}"

              - name: products
                sql: "SELECT * FROM products"
                dimensions:
                  - name: id
                    type: number
                    sql: ID
                    primary_key: true
                  - name: price
                    type: number
                    sql: PRICE
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
        "#};

        let yaml_schema: YamlSchema = serde_yaml::from_str(yaml).unwrap();
        let schema = yaml_schema.build().unwrap();

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
