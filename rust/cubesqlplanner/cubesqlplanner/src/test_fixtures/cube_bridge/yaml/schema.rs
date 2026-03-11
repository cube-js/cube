use crate::test_fixtures::cube_bridge::yaml::{
    YamlDimensionDefinition, YamlMeasureDefinition, YamlPreAggregationDefinition,
    YamlSegmentDefinition,
};
use crate::test_fixtures::cube_bridge::{
    MockCubeDefinition, MockJoinItemDefinition, MockSchema, MockSchemaBuilder,
};
use cubenativeutils::CubeError;
use serde::Deserialize;
use std::collections::HashMap;
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
    #[serde(default)]
    pre_aggregations: Vec<YamlPreAggregationEntry>,
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
struct YamlPreAggregationEntry {
    name: String,
    #[serde(flatten)]
    definition: YamlPreAggregationDefinition,
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
            let mut joins = HashMap::new();
            for join in cube.joins {
                let join_def = MockJoinItemDefinition::builder()
                    .relationship(join.relationship)
                    .sql(join.sql)
                    .build();
                joins.insert(join.name.clone(), join_def);
            }

            let cube_def = MockCubeDefinition::builder()
                .name(cube.name.clone())
                .sql(cube.sql.clone())
                .joins(joins)
                .build();

            let mut cube_builder = builder.add_cube(cube.name).cube_def(cube_def);

            for dim_entry in cube.dimensions {
                let result = dim_entry.definition.build();
                cube_builder =
                    cube_builder.add_dimension(dim_entry.name.clone(), result.definition);
                for (gran_name, gran_def) in result.granularities {
                    cube_builder =
                        cube_builder.add_granularity(&dim_entry.name, &gran_name, gran_def);
                }
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

            for pre_agg_entry in cube.pre_aggregations {
                let pre_agg_rc = pre_agg_entry.definition.build(pre_agg_entry.name.clone());
                let pre_agg_def = Rc::try_unwrap(pre_agg_rc)
                    .ok()
                    .expect("Rc should have single owner");
                cube_builder = cube_builder.add_pre_aggregation(pre_agg_entry.name, pre_agg_def);
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
    use crate::cube_bridge::evaluator::CubeEvaluator;
    use crate::cube_bridge::measure_definition::MeasureDefinition;
    use crate::cube_bridge::member_sql::SqlTemplate;
    use crate::cube_bridge::pre_aggregation_description::PreAggregationDescription;
    use crate::test_fixtures::cube_bridge::{MockBaseTools, MockSecurityContext};
    use indoc::indoc;
    use std::rc::Rc;

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
    fn test_parse_cube_with_pre_aggregations() {
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
                  - name: created_at
                    type: time
                    sql: created_at
                measures:
                  - name: count
                    type: count
                  - name: total_amount
                    type: sum
                    sql: amount
                pre_aggregations:
                  - name: main
                    type: rollup
                    measures:
                      - count
                      - total_amount
                    dimensions:
                      - status
                    time_dimension: created_at
                    granularity: day
                  - name: by_status
                    measures:
                      - count
                    dimensions:
                      - status
        "#};

        let yaml_schema: YamlSchema = serde_yaml::from_str(yaml).unwrap();
        let schema = yaml_schema.build().unwrap();

        assert!(schema.get_cube("orders").is_some());

        let main_pre_agg = schema.get_pre_aggregation("orders", "main").unwrap();
        assert_eq!(main_pre_agg.static_data().pre_aggregation_type, "rollup");
        assert_eq!(
            main_pre_agg.static_data().granularity,
            Some("day".to_string())
        );
        assert!(main_pre_agg.has_measure_references().unwrap());
        assert!(main_pre_agg.has_dimension_references().unwrap());
        assert!(main_pre_agg.has_time_dimension_reference().unwrap());

        let by_status_pre_agg = schema.get_pre_aggregation("orders", "by_status").unwrap();
        assert_eq!(
            by_status_pre_agg.static_data().pre_aggregation_type,
            "rollup"
        );
        assert!(by_status_pre_agg.has_measure_references().unwrap());
        assert!(by_status_pre_agg.has_dimension_references().unwrap());
        assert!(!by_status_pre_agg.has_time_dimension_reference().unwrap());
    }

    #[test]
    fn test_parse_pre_aggregation_with_all_options() {
        let yaml = indoc! {r#"
            cubes:
              - name: sales
                sql: "SELECT * FROM sales"
                dimensions:
                  - name: id
                    type: number
                    sql: id
                    primary_key: true
                  - name: region
                    type: string
                    sql: region
                  - name: created_at
                    type: time
                    sql: created_at
                measures:
                  - name: count
                    type: count
                  - name: revenue
                    type: sum
                    sql: amount
                pre_aggregations:
                  - name: sales_rollup
                    type: rollup
                    measures:
                      - count
                      - revenue
                    dimensions:
                      - region
                    time_dimension: created_at
                    granularity: month
                    external: true
                    allow_non_strict_date_range_match: false
                  - name: original_sql_pre_agg
                    type: original_sql
                    sql_alias: sales_original
        "#};

        let yaml_schema: YamlSchema = serde_yaml::from_str(yaml).unwrap();
        let schema = yaml_schema.build().unwrap();

        let sales_rollup = schema.get_pre_aggregation("sales", "sales_rollup").unwrap();
        assert_eq!(sales_rollup.static_data().pre_aggregation_type, "rollup");
        assert_eq!(
            sales_rollup.static_data().granularity,
            Some("month".to_string())
        );
        assert_eq!(sales_rollup.static_data().external, Some(true));
        assert_eq!(
            sales_rollup.static_data().allow_non_strict_date_range_match,
            Some(false)
        );

        let original_sql = schema
            .get_pre_aggregation("sales", "original_sql_pre_agg")
            .unwrap();
        assert_eq!(
            original_sql.static_data().pre_aggregation_type,
            "original_sql"
        );
        assert_eq!(
            original_sql.static_data().sql_alias,
            Some("sales_original".to_string())
        );
    }

    #[test]
    fn test_pre_aggregation_references_compile_correctly() {
        let yaml = indoc! {r#"
            cubes:
              - name: orders
                sql: "SELECT * FROM orders"
                joins:
                  - name: line_items
                    sql: "{CUBE}.id = {line_items}.order_id"
                    relationship: one_to_many
                dimensions:
                  - name: status
                    type: string
                    sql: status
                  - name: created_at
                    type: time
                    sql: created_at
                measures:
                  - name: count
                    type: count
                  - name: total_amount
                    type: sum
                    sql: amount
                pre_aggregations:
                  - name: main
                    dimensions:
                      - orders.status
                    measures:
                      - orders.count
                      - orders.total_amount
                    time_dimension: orders.created_at
                    granularity: day
              - name: line_items
                sql: "SELECT * FROM line_items"
                dimensions:
                  - name: product_id
                    type: number
                    sql: product_id
        "#};

        let yaml_schema: YamlSchema = serde_yaml::from_str(yaml).unwrap();
        let schema = yaml_schema.build().unwrap();

        let main_pre_agg = schema.get_pre_aggregation("orders", "main").unwrap();

        let measure_refs = main_pre_agg.measure_references().unwrap().unwrap();
        let dim_refs = main_pre_agg.dimension_references().unwrap().unwrap();
        let time_dim_ref = main_pre_agg.time_dimension_reference().unwrap().unwrap();

        assert_eq!(measure_refs.args_names(), &vec!["orders"]);
        assert_eq!(dim_refs.args_names(), &vec!["orders"]);
        assert_eq!(time_dim_ref.args_names(), &vec!["orders"]);

        let base_tools = Rc::new(MockBaseTools::default());
        let sec_ctx = Rc::new(MockSecurityContext);

        let (measure_template, measure_args) = measure_refs
            .compile_template_sql(base_tools.clone(), sec_ctx.clone())
            .unwrap();
        let (dim_template, dim_args) = dim_refs
            .compile_template_sql(base_tools.clone(), sec_ctx.clone())
            .unwrap();
        let (time_template, time_args) = time_dim_ref
            .compile_template_sql(base_tools.clone(), sec_ctx.clone())
            .unwrap();

        match measure_template {
            SqlTemplate::StringVec(vec) => {
                assert_eq!(vec.len(), 2);
                assert_eq!(vec[0], "{arg:0}");
                assert_eq!(vec[1], "{arg:1}");
            }
            _ => panic!("Expected StringVec for measures"),
        }
        assert_eq!(measure_args.symbol_paths.len(), 2);
        assert_eq!(measure_args.symbol_paths[0], vec!["orders", "count"]);
        assert_eq!(measure_args.symbol_paths[1], vec!["orders", "total_amount"]);

        match dim_template {
            SqlTemplate::StringVec(vec) => {
                assert_eq!(vec.len(), 1);
                assert_eq!(vec[0], "{arg:0}");
            }
            _ => panic!("Expected StringVec for dimensions"),
        }
        assert_eq!(dim_args.symbol_paths.len(), 1);
        assert_eq!(dim_args.symbol_paths[0], vec!["orders", "status"]);

        match time_template {
            SqlTemplate::String(s) => {
                assert_eq!(s, "{arg:0}");
            }
            _ => panic!("Expected String for time dimension"),
        }
        assert_eq!(time_args.symbol_paths.len(), 1);
        assert_eq!(time_args.symbol_paths[0], vec!["orders", "created_at"]);
    }

    #[test]
    fn test_pre_aggregation_with_multiple_cubes() {
        let yaml = indoc! {r#"
            cubes:
              - name: orders
                sql: "SELECT * FROM orders"
                joins:
                  - name: line_items
                    sql: "{CUBE}.id = {line_items}.order_id"
                    relationship: one_to_many
                dimensions:
                  - name: status
                    type: string
                    sql: status
                  - name: created_at
                    type: time
                    sql: created_at
                measures:
                  - name: count
                    type: count
                  - name: total_qty
                    type: sum
                    sql: amount
                pre_aggregations:
                  - name: pre_agg_with_multiplied_measures
                    dimensions:
                      - orders.status
                      - line_items.product_id
                    measures:
                      - orders.count
                      - orders.total_qty
                    time_dimension: orders.created_at
                    granularity: month
              - name: line_items
                sql: "SELECT * FROM line_items"
                dimensions:
                  - name: product_id
                    type: number
                    sql: product_id
        "#};

        let yaml_schema: YamlSchema = serde_yaml::from_str(yaml).unwrap();
        let schema = yaml_schema.build().unwrap();

        let pre_agg = schema
            .get_pre_aggregation("orders", "pre_agg_with_multiplied_measures")
            .unwrap();

        let measure_refs = pre_agg.measure_references().unwrap().unwrap();
        let dim_refs = pre_agg.dimension_references().unwrap().unwrap();
        let time_dim_ref = pre_agg.time_dimension_reference().unwrap().unwrap();

        assert_eq!(measure_refs.args_names(), &vec!["orders"]);
        assert_eq!(dim_refs.args_names(), &vec!["orders", "line_items"]);
        assert_eq!(time_dim_ref.args_names(), &vec!["orders"]);

        let base_tools = Rc::new(MockBaseTools::default());
        let sec_ctx = Rc::new(MockSecurityContext);

        let (dim_template, dim_args) = dim_refs
            .compile_template_sql(base_tools.clone(), sec_ctx.clone())
            .unwrap();

        match dim_template {
            SqlTemplate::StringVec(vec) => {
                assert_eq!(vec.len(), 2);
                assert_eq!(vec[0], "{arg:0}");
                assert_eq!(vec[1], "{arg:1}");
            }
            _ => panic!("Expected StringVec for dimensions"),
        }
        assert_eq!(dim_args.symbol_paths.len(), 2);
        assert_eq!(dim_args.symbol_paths[0], vec!["orders", "status"]);
        assert_eq!(dim_args.symbol_paths[1], vec!["line_items", "product_id"]);
    }

    #[test]
    fn test_multi_stage_example() {
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

    #[test]
    fn test_pre_aggregations_preserve_order() {
        let yaml = indoc! {r#"
            cubes:
              - name: orders
                sql: "SELECT * FROM orders"
                dimensions:
                  - name: status
                    type: string
                    sql: status
                  - name: created_at
                    type: time
                    sql: created_at
                measures:
                  - name: count
                    type: count
                pre_aggregations:
                  - name: first_rollup
                    type: rollup
                    measures:
                      - count
                    dimensions:
                      - status
                    time_dimension: created_at
                    granularity: day
                  - name: second_rollup
                    type: rollup
                    measures:
                      - count
                    time_dimension: created_at
                    granularity: month
                  - name: third_rollup
                    type: rollup
                    measures:
                      - count
                    time_dimension: created_at
                    granularity: year
        "#};

        let schema = MockSchema::from_yaml(yaml).unwrap();
        let evaluator = schema.create_evaluator();

        // Check that pre_aggregations_for_cube_as_array returns pre-aggregations in the same order
        // as they are defined in YAML
        let pre_aggs = evaluator
            .pre_aggregations_for_cube_as_array("orders".to_string())
            .unwrap();

        assert_eq!(pre_aggs.len(), 3);
        assert_eq!(pre_aggs[0].static_data().name, "first_rollup");
        assert_eq!(
            pre_aggs[0].static_data().granularity,
            Some("day".to_string())
        );
        assert_eq!(pre_aggs[1].static_data().name, "second_rollup");
        assert_eq!(
            pre_aggs[1].static_data().granularity,
            Some("month".to_string())
        );
        assert_eq!(pre_aggs[2].static_data().name, "third_rollup");
        assert_eq!(
            pre_aggs[2].static_data().granularity,
            Some("year".to_string())
        );
    }
}
