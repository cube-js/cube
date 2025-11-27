use crate::test_fixtures::cube_bridge::MockSchema;
use indoc::indoc;

/// Creates a simple schema with orders and customers cubes
///
/// This schema demonstrates:
/// - Two cubes with basic dimensions and measures
/// - Single many-to-one join from orders to customers
/// - Standard measure types (count, max, min)
pub fn create_simple_schema() -> MockSchema {
    let yaml = indoc! {r#"
        cubes:
          - name: customers
            sql: "SELECT * FROM customers"
            dimensions:
              - name: id
                type: number
                sql: id
                primary_key: true
              - name: name
                type: string
                sql: name
              - name: city
                type: string
                sql: city
              - name: created_at
                type: time
                sql: created_at
            measures:
              - name: count
                type: count
                sql: "COUNT(*)"
              - name: max_age
                type: max
                sql: age
              - name: min_age
                type: min
                sql: age
              - name: payments
                type: sum
                sql: payments
              - name: payments_per_order
                type: sum
                sql: "{payments} / {orders.count}"

          - name: orders
            sql: "SELECT * FROM orders"
            joins:
              - name: customers
                relationship: many_to_one
                sql: "{orders}.customer_id = {customers.id}"
            dimensions:
              - name: id
                type: number
                sql: id
                primary_key: true
              - name: status
                type: string
                sql: status
              - name: priority
                type: string
                sql: priority
              - name: created_at
                type: time
                sql: created_at
            measures:
              - name: count
                type: count
                sql: "COUNT(*)"
              - name: max_amount
                type: max
                sql: amount
              - name: min_amount
                type: min
                sql: amount

        views:
          - name: orders_with_customer
            cubes:
              - join_path: orders
                includes:
                  - id
                  - status
                  - count
              - join_path: orders.customers
                includes:
                  - name
    "#};

    MockSchema::from_yaml(yaml).expect("Failed to parse simple schema")
}
