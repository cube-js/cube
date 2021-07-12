use crate::SqlClient;
use async_compression::tokio::write::GzipEncoder;
use cubestore::queryplanner::pretty_printers::{pp_phys_plan, pp_phys_plan_ext, PPOptions};
use cubestore::queryplanner::MIN_TOPK_STREAM_ROWS;
use cubestore::sql::timestamp_from_string;
use cubestore::store::DataFrame;
use cubestore::table::{Row, TableValue, TimestampValue};
use cubestore::util::decimal::Decimal;
use itertools::Itertools;
use pretty_assertions::assert_eq;
use std::env;
use std::fs::File;
use std::future::Future;
use std::io::Write;
use std::panic::RefUnwindSafe;
use std::path::Path;
use std::pin::Pin;
use tokio::io::{AsyncWriteExt, BufWriter};

pub type TestFn = Box<
    dyn Fn(Box<dyn SqlClient>) -> Pin<Box<dyn Future<Output = ()> + Send>>
        + Send
        + Sync
        + RefUnwindSafe,
>;
pub fn sql_tests() -> Vec<(&'static str, TestFn)> {
    return vec![
        t("insert", insert),
        t("select_test", select_test),
        t("negative_numbers", negative_numbers),
        t("custom_types", custom_types),
        t("group_by_boolean", group_by_boolean),
        t("group_by_decimal", group_by_decimal),
        t("float_decimal_scale", float_decimal_scale),
        t("float_merge", float_merge),
        t("join", join),
        t("three_tables_join", three_tables_join),
        t(
            "three_tables_join_with_filter",
            three_tables_join_with_filter,
        ),
        t("three_tables_join_with_union", three_tables_join_with_union),
        t("in_list", in_list),
        t("numeric_cast", numeric_cast),
        t("union", union),
        t("timestamp_select", timestamp_select),
        t("column_escaping", column_escaping),
        t("information_schema", information_schema),
        t("case_column_escaping", case_column_escaping),
        t("inner_column_escaping", inner_column_escaping),
        t("convert_tz", convert_tz),
        t("coalesce", coalesce),
        t("count_distinct_crash", count_distinct_crash),
        t("create_schema_if_not_exists", create_schema_if_not_exists),
        t(
            "create_index_before_ingestion",
            create_index_before_ingestion,
        ),
        t("ambiguous_join_sort", ambiguous_join_sort),
        t("join_with_aliases", join_with_aliases),
        t("group_by_without_aggregates", group_by_without_aggregates),
        t("create_table_with_location", create_table_with_location),
        t("create_table_with_url", create_table_with_url),
        t("create_table_fail_and_retry", create_table_fail_and_retry),
        t("empty_crash", empty_crash),
        t("bytes", bytes),
        t("hyperloglog", hyperloglog),
        t("hyperloglog_empty_inputs", hyperloglog_empty_inputs),
        t("hyperloglog_empty_group_by", hyperloglog_empty_group_by),
        t("hyperloglog_inserts", hyperloglog_inserts),
        t("hyperloglog_inplace_group_by", hyperloglog_inplace_group_by),
        t("hyperloglog_snowflake", hyperloglog_snowflake),
        t("planning_inplace_aggregate", planning_inplace_aggregate),
        t("planning_hints", planning_hints),
        t("planning_inplace_aggregate2", planning_inplace_aggregate2),
        t("topk_large_inputs", topk_large_inputs),
        t("planning_simple", planning_simple),
        t("planning_joins", planning_joins),
        t("planning_3_table_joins", planning_3_table_joins),
        t("topk_query", topk_query),
        t("topk_decimals", topk_decimals),
        t("offset", offset),
        t("having", having),
        t("rolling_window_join", rolling_window_join),
        t("decimal_index", decimal_index),
        t("float_index", float_index),
        t("date_add", date_add),
        t("now", now),
        t("dump", dump),
    ];

    fn t<F>(name: &'static str, f: fn(Box<dyn SqlClient>) -> F) -> (&'static str, TestFn)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        (name, Box::new(move |c| Box::pin(f(c))))
    }
}

async fn insert(service: Box<dyn SqlClient>) {
    let _ = service.exec_query("CREATE SCHEMA Foo").await.unwrap();
    let _ = service
        .exec_query(
            "CREATE TABLE Foo.Persons (
                            PersonID int,
                            LastName varchar(255),
                            FirstName varchar(255),
                            Address varchar(255),
                            City varchar(255)
                          )",
        )
        .await
        .unwrap();

    service.exec_query(
            "INSERT INTO Foo.Persons
        (
            PersonID,
            LastName,
            FirstName,
            Address,
            City
        )

        VALUES
        (23, 'LastName 1', 'FirstName 1', 'Address 1', 'City 1'), (38, 'LastName 21', 'FirstName 2', 'Address 2', 'City 2'),
        (24, 'LastName 3', 'FirstName 1', 'Address 1', 'City 1'), (37, 'LastName 22', 'FirstName 2', 'Address 2', 'City 2'),
        (25, 'LastName 4', 'FirstName 1', 'Address 1', 'City 1'), (36, 'LastName 23', 'FirstName 2', 'Address 2', 'City 2'),
        (26, 'LastName 5', 'FirstName 1', 'Address 1', 'City 1'), (35, 'LastName 24', 'FirstName 2', 'Address 2', 'City 2'),
        (27, 'LastName 6', 'FirstName 1', 'Address 1', 'City 1'), (34, 'LastName 25', 'FirstName 2', 'Address 2', 'City 2'),
        (28, 'LastName 7', 'FirstName 1', 'Address 1', 'City 1'), (33, 'LastName 26', 'FirstName 2', 'Address 2', 'City 2'),
        (29, 'LastName 8', 'FirstName 1', 'Address 1', 'City 1'), (32, 'LastName 27', 'FirstName 2', 'Address 2', 'City 2'),
        (30, 'LastName 9', 'FirstName 1', 'Address 1', 'City 1'), (31, 'LastName 28', 'FirstName 2', 'Address 2', 'City 2')"
        ).await.unwrap();

    service.exec_query("INSERT INTO Foo.Persons
        (LastName, PersonID, FirstName, Address, City)
        VALUES
        ('LastName 1', 23, 'FirstName 1', 'Address 1', 'City 1'), ('LastName 2', 22, 'FirstName 2', 'Address 2', 'City 2');").await.unwrap();
}

async fn select_test(service: Box<dyn SqlClient>) {
    let _ = service.exec_query("CREATE SCHEMA Foo").await.unwrap();

    let _ = service
        .exec_query(
            "CREATE TABLE Foo.Persons (
                            PersonID int,
                            LastName varchar(255),
                            FirstName varchar(255),
                            Address varchar(255),
                            City varchar(255)
                          );",
        )
        .await
        .unwrap();

    service
        .exec_query(
            "INSERT INTO Foo.Persons
            (LastName, PersonID, FirstName, Address, City)
            VALUES
            ('LastName 1', 23, 'FirstName 1', 'Address 1', 'City 1'),
            ('LastName 2', 22, 'FirstName 2', 'Address 2', 'City 2');",
        )
        .await
        .unwrap();

    let result = service
        .exec_query("SELECT PersonID person_id from Foo.Persons")
        .await
        .unwrap();

    assert_eq!(result.get_rows()[0], Row::new(vec![TableValue::Int(22)]));
    assert_eq!(result.get_rows()[1], Row::new(vec![TableValue::Int(23)]));
}

async fn negative_numbers(service: Box<dyn SqlClient>) {
    let _ = service.exec_query("CREATE SCHEMA foo").await.unwrap();

    let _ = service
        .exec_query("CREATE TABLE foo.values (int_value int)")
        .await
        .unwrap();

    service
        .exec_query("INSERT INTO foo.values (int_value) VALUES (-153)")
        .await
        .unwrap();

    let result = service
        .exec_query("SELECT * from foo.values")
        .await
        .unwrap();

    assert_eq!(result.get_rows()[0], Row::new(vec![TableValue::Int(-153)]));
}

async fn custom_types(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA foo").await.unwrap();

    service
        .exec_query("CREATE TABLE foo.values (int_value mediumint, b1 bytes, b2 varbinary)")
        .await
        .unwrap();

    service
        .exec_query("INSERT INTO foo.values (int_value, b1, b2) VALUES (-153, X'0a', X'0b')")
        .await
        .unwrap();
}

async fn group_by_boolean(service: Box<dyn SqlClient>) {
    let _ = service.exec_query("CREATE SCHEMA foo").await.unwrap();

    let _ = service
        .exec_query("CREATE TABLE foo.bool_group (bool_value boolean)")
        .await
        .unwrap();

    service.exec_query(
            "INSERT INTO foo.bool_group (bool_value) VALUES (true), (false), (true), (false), (false)"
        ).await.unwrap();

    // TODO compaction fails the test in between?
    // service.exec_query(
    //     "INSERT INTO foo.bool_group (bool_value) VALUES (true), (false), (true), (false), (false)"
    // ).await.unwrap();

    let result = service
        .exec_query("SELECT count(*) from foo.bool_group")
        .await
        .unwrap();
    assert_eq!(result.get_rows()[0], Row::new(vec![TableValue::Int(5)]));

    let result = service
        .exec_query("SELECT count(*) from foo.bool_group where bool_value = true")
        .await
        .unwrap();
    assert_eq!(result.get_rows()[0], Row::new(vec![TableValue::Int(2)]));

    let result = service
        .exec_query("SELECT count(*) from foo.bool_group where bool_value = 'true'")
        .await
        .unwrap();
    assert_eq!(result.get_rows()[0], Row::new(vec![TableValue::Int(2)]));

    let result = service
        .exec_query(
            "SELECT g.bool_value, count(*) from foo.bool_group g GROUP BY 1 ORDER BY 2 DESC",
        )
        .await
        .unwrap();

    assert_eq!(
        result.get_rows()[0],
        Row::new(vec![TableValue::Boolean(false), TableValue::Int(3)])
    );
    assert_eq!(
        result.get_rows()[1],
        Row::new(vec![TableValue::Boolean(true), TableValue::Int(2)])
    );
}

async fn group_by_decimal(service: Box<dyn SqlClient>) {
    let _ = service.exec_query("CREATE SCHEMA foo").await.unwrap();

    let _ = service
        .exec_query("CREATE TABLE foo.decimal_group (id INT, decimal_value DECIMAL)")
        .await
        .unwrap();

    service.exec_query(
            "INSERT INTO foo.decimal_group (id, decimal_value) VALUES (1, 100), (2, 200), (3, 100), (4, 100), (5, 200)"
        ).await.unwrap();

    let result = service
        .exec_query("SELECT count(*) from foo.decimal_group")
        .await
        .unwrap();
    assert_eq!(result.get_rows()[0], Row::new(vec![TableValue::Int(5)]));

    let result = service
        .exec_query("SELECT count(*) from foo.decimal_group where decimal_value = 200")
        .await
        .unwrap();
    assert_eq!(result.get_rows()[0], Row::new(vec![TableValue::Int(2)]));

    let result = service
        .exec_query(
            "SELECT g.decimal_value, count(*) from foo.decimal_group g GROUP BY 1 ORDER BY 2 DESC",
        )
        .await
        .unwrap();

    assert_eq!(
        result.get_rows(),
        &vec![
            Row::new(vec![
                TableValue::Decimal(Decimal::new(100 * 100_000)),
                TableValue::Int(3)
            ]),
            Row::new(vec![
                TableValue::Decimal(Decimal::new(200 * 100_000)),
                TableValue::Int(2)
            ])
        ]
    );
}

async fn float_decimal_scale(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA foo").await.unwrap();
    service
        .exec_query("CREATE TABLE foo.decimal_group (id INT, decimal_value FLOAT)")
        .await
        .unwrap();

    service.exec_query(
            "INSERT INTO foo.decimal_group (id, decimal_value) VALUES (1, 677863988852), (2, 677863988852.123e-10), (3, 6778639882.123e+3)"
        ).await.unwrap();

    let result = service
        .exec_query("SELECT SUM(decimal_value) FROM foo.decimal_group")
        .await
        .unwrap();

    assert_eq!(
        result.get_rows(),
        &vec![Row::new(vec![TableValue::Float(7456503871042.786.into())])]
    );
}

async fn float_merge(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA s").await.unwrap();
    service
        .exec_query("CREATE TABLE s.f1 (n float)")
        .await
        .unwrap();
    service
        .exec_query("INSERT INTO s.f1 (n) VALUES (1.0), (2.0)")
        .await
        .unwrap();
    service
        .exec_query("CREATE TABLE s.f2 (n float)")
        .await
        .unwrap();
    service
        .exec_query("INSERT INTO s.f2 (n) VALUES (1.0), (3.0)")
        .await
        .unwrap();
    let r = service
        .exec_query(
            "SELECT n \
             FROM (SELECT * from s.f1 UNION ALL SELECT * FROM s.f2) \
             GROUP BY 1 \
             ORDER BY 1",
        )
        .await
        .unwrap();

    assert_eq!(
        to_rows(&r),
        vec![
            vec![TableValue::Float(1.0.into())],
            vec![TableValue::Float(2.0.into())],
            vec![TableValue::Float(3.0.into())],
        ]
    );
}

async fn join(service: Box<dyn SqlClient>) {
    let _ = service.exec_query("CREATE SCHEMA foo").await.unwrap();

    let _ = service
        .exec_query("CREATE TABLE foo.orders (customer_id text, amount int)")
        .await
        .unwrap();
    let _ = service
        .exec_query("CREATE TABLE foo.customers (id text, city text, state text)")
        .await
        .unwrap();

    service
        .exec_query(
            "INSERT INTO foo.orders (customer_id, amount) VALUES ('a', 10), ('b', 2), ('b', 3)",
        )
        .await
        .unwrap();

    service.exec_query(
            "INSERT INTO foo.customers (id, city, state) VALUES ('a', 'San Francisco', 'CA'), ('b', 'New York', 'NY')"
        ).await.unwrap();

    let result = service.exec_query("SELECT c.city, sum(o.amount) from foo.orders o JOIN foo.customers c ON o.customer_id = c.id GROUP BY 1 ORDER BY 2 DESC").await.unwrap();

    assert_eq!(
        result.get_rows()[0],
        Row::new(vec![
            TableValue::String("San Francisco".to_string()),
            TableValue::Int(10)
        ])
    );
    assert_eq!(
        result.get_rows()[1],
        Row::new(vec![
            TableValue::String("New York".to_string()),
            TableValue::Int(5)
        ])
    );
}

async fn three_tables_join(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA foo").await.unwrap();

    service
        .exec_query(
            "CREATE TABLE foo.orders (orders_customer_id text, orders_product_id int, amount int)",
        )
        .await
        .unwrap();
    service
        .exec_query("CREATE INDEX orders_by_product ON foo.orders (orders_product_id)")
        .await
        .unwrap();
    service
        .exec_query("CREATE TABLE foo.customers (customer_id text, city text, state text)")
        .await
        .unwrap();
    service
        .exec_query("CREATE TABLE foo.products (product_id int, name text)")
        .await
        .unwrap();

    service.exec_query(
            "INSERT INTO foo.orders (orders_customer_id, orders_product_id, amount) VALUES ('a', 1, 10), ('b', 2, 2), ('b', 2, 3)"
        ).await.unwrap();

    service.exec_query(
            "INSERT INTO foo.orders (orders_customer_id, orders_product_id, amount) VALUES ('b', 1, 10), ('c', 2, 2), ('c', 2, 3)"
        ).await.unwrap();

    service.exec_query(
            "INSERT INTO foo.orders (orders_customer_id, orders_product_id, amount) VALUES ('c', 1, 10), ('d', 2, 2), ('d', 2, 3)"
        ).await.unwrap();

    service.exec_query(
            "INSERT INTO foo.customers (customer_id, city, state) VALUES ('a', 'San Francisco', 'CA'), ('b', 'New York', 'NY')"
        ).await.unwrap();

    service.exec_query(
            "INSERT INTO foo.customers (customer_id, city, state) VALUES ('c', 'San Francisco', 'CA'), ('d', 'New York', 'NY')"
        ).await.unwrap();

    service
        .exec_query(
            "INSERT INTO foo.products (product_id, name) VALUES (1, 'Potato'), (2, 'Tomato')",
        )
        .await
        .unwrap();

    let result = service
        .exec_query(
            "SELECT city, name, sum(amount) FROM foo.orders o \
            LEFT JOIN foo.customers c ON orders_customer_id = customer_id \
            LEFT JOIN foo.products p ON orders_product_id = product_id \
            GROUP BY 1, 2 ORDER BY 3 DESC, 1 ASC, 2 ASC",
        )
        .await
        .unwrap();

    let expected = vec![
        Row::new(vec![
            TableValue::String("San Francisco".to_string()),
            TableValue::String("Potato".to_string()),
            TableValue::Int(20),
        ]),
        Row::new(vec![
            TableValue::String("New York".to_string()),
            TableValue::String("Potato".to_string()),
            TableValue::Int(10),
        ]),
        Row::new(vec![
            TableValue::String("New York".to_string()),
            TableValue::String("Tomato".to_string()),
            TableValue::Int(10),
        ]),
        Row::new(vec![
            TableValue::String("San Francisco".to_string()),
            TableValue::String("Tomato".to_string()),
            TableValue::Int(5),
        ]),
    ];

    assert_eq!(result.get_rows(), &expected);

    let result = service
        .exec_query(
            "SELECT city, name, sum(amount) FROM foo.orders o \
            LEFT JOIN foo.customers c ON orders_customer_id = customer_id \
            LEFT JOIN foo.products p ON orders_product_id = product_id \
            WHERE customer_id = 'b' AND product_id IN ('2')
            GROUP BY 1, 2 ORDER BY 3 DESC, 1 ASC, 2 ASC",
        )
        .await
        .unwrap();

    let expected = vec![Row::new(vec![
        TableValue::String("New York".to_string()),
        TableValue::String("Tomato".to_string()),
        TableValue::Int(5),
    ])];

    assert_eq!(result.get_rows(), &expected);
}

async fn three_tables_join_with_filter(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA foo").await.unwrap();

    service
        .exec_query(
            "CREATE TABLE foo.orders (orders_customer_id text, orders_product_id int, amount int)",
        )
        .await
        .unwrap();
    service
        .exec_query("CREATE INDEX orders_by_product ON foo.orders (orders_product_id)")
        .await
        .unwrap();
    service
        .exec_query("CREATE TABLE foo.customers (customer_id text, city text, state text)")
        .await
        .unwrap();
    service
        .exec_query("CREATE TABLE foo.products (product_id int, name text)")
        .await
        .unwrap();

    service.exec_query(
            "INSERT INTO foo.orders (orders_customer_id, orders_product_id, amount) VALUES ('a', 1, 10), ('b', 2, 2), ('b', 2, 3)"
        ).await.unwrap();

    service.exec_query(
            "INSERT INTO foo.orders (orders_customer_id, orders_product_id, amount) VALUES ('b', 1, 10), ('c', 2, 2), ('c', 2, 3)"
        ).await.unwrap();

    service.exec_query(
            "INSERT INTO foo.orders (orders_customer_id, orders_product_id, amount) VALUES ('c', 1, 10), ('d', 2, 2), ('d', 2, 3)"
        ).await.unwrap();

    service.exec_query(
            "INSERT INTO foo.customers (customer_id, city, state) VALUES ('a', 'San Francisco', 'CA'), ('b', 'New York', 'NY')"
        ).await.unwrap();

    service.exec_query(
            "INSERT INTO foo.customers (customer_id, city, state) VALUES ('c', 'San Francisco', 'CA'), ('d', 'New York', 'NY')"
        ).await.unwrap();

    service
        .exec_query(
            "INSERT INTO foo.products (product_id, name) VALUES (1, 'Potato'), (2, 'Tomato')",
        )
        .await
        .unwrap();

    let result = service
        .exec_query(
            "SELECT city, name, sum(amount) FROM foo.orders o \
            LEFT JOIN foo.products p ON orders_product_id = product_id \
            LEFT JOIN foo.customers c ON orders_customer_id = customer_id \
            WHERE customer_id = 'a' \
            GROUP BY 1, 2 ORDER BY 3 DESC, 1 ASC, 2 ASC",
        )
        .await
        .unwrap();

    let expected = vec![Row::new(vec![
        TableValue::String("San Francisco".to_string()),
        TableValue::String("Potato".to_string()),
        TableValue::Int(10),
    ])];

    assert_eq!(result.get_rows(), &expected);
}

async fn three_tables_join_with_union(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA foo").await.unwrap();

    service.exec_query("CREATE TABLE foo.orders_1 (orders_customer_id text, orders_product_id int, amount int)").await.unwrap();
    service.exec_query("CREATE TABLE foo.orders_2 (orders_customer_id text, orders_product_id int, amount int)").await.unwrap();
    service
        .exec_query("CREATE INDEX orders_by_product_1 ON foo.orders_1 (orders_product_id)")
        .await
        .unwrap();
    service
        .exec_query("CREATE INDEX orders_by_product_2 ON foo.orders_2 (orders_product_id)")
        .await
        .unwrap();
    service
        .exec_query("CREATE TABLE foo.customers (customer_id text, city text, state text)")
        .await
        .unwrap();
    service
        .exec_query("CREATE TABLE foo.products (product_id int, name text)")
        .await
        .unwrap();

    service.exec_query(
            "INSERT INTO foo.orders_1 (orders_customer_id, orders_product_id, amount) VALUES ('a', 1, 10), ('b', 2, 2), ('b', 2, 3)"
        ).await.unwrap();

    service.exec_query(
            "INSERT INTO foo.orders_1 (orders_customer_id, orders_product_id, amount) VALUES ('b', 1, 10), ('c', 2, 2), ('c', 2, 3)"
        ).await.unwrap();

    service.exec_query(
            "INSERT INTO foo.orders_2 (orders_customer_id, orders_product_id, amount) VALUES ('c', 1, 10), ('d', 2, 2), ('d', 2, 3)"
        ).await.unwrap();

    service.exec_query(
            "INSERT INTO foo.customers (customer_id, city, state) VALUES ('a', 'San Francisco', 'CA'), ('b', 'New York', 'NY')"
        ).await.unwrap();

    service.exec_query(
            "INSERT INTO foo.customers (customer_id, city, state) VALUES ('c', 'San Francisco', 'CA'), ('d', 'New York', 'NY')"
        ).await.unwrap();

    service
        .exec_query(
            "INSERT INTO foo.products (product_id, name) VALUES (1, 'Potato'), (2, 'Tomato')",
        )
        .await
        .unwrap();

    let result = service.exec_query(
            "SELECT city, name, sum(amount) FROM (SELECT * FROM foo.orders_1 UNION ALL SELECT * FROM foo.orders_2) o \
            LEFT JOIN foo.customers c ON orders_customer_id = customer_id \
            LEFT JOIN foo.products p ON orders_product_id = product_id \
            WHERE customer_id = 'a' \
            GROUP BY 1, 2 ORDER BY 3 DESC, 1 ASC, 2 ASC"
        ).await.unwrap();

    let expected = vec![Row::new(vec![
        TableValue::String("San Francisco".to_string()),
        TableValue::String("Potato".to_string()),
        TableValue::Int(10),
    ])];

    assert_eq!(result.get_rows(), &expected);
}

async fn in_list(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA foo").await.unwrap();

    service
        .exec_query("CREATE TABLE foo.customers (id text, city text, state text)")
        .await
        .unwrap();

    service.exec_query(
            "INSERT INTO foo.customers (id, city, state) VALUES ('a', 'San Francisco', 'CA'), ('b', 'New York', 'NY'), ('c', 'San Diego', 'CA'), ('d', 'Austin', 'TX')"
        ).await.unwrap();

    let result = service
        .exec_query("SELECT count(*) from foo.customers WHERE state in ('CA', 'TX')")
        .await
        .unwrap();

    assert_eq!(result.get_rows()[0], Row::new(vec![TableValue::Int(3)]));
}

async fn numeric_cast(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA foo").await.unwrap();

    service
        .exec_query("CREATE TABLE foo.managers (id text, department_id int)")
        .await
        .unwrap();

    service.exec_query(
            "INSERT INTO foo.managers (id, department_id) VALUES ('a', 1), ('b', 3), ('c', 3), ('d', 5)"
        ).await.unwrap();

    let result = service
        .exec_query("SELECT count(*) from foo.managers WHERE department_id in ('3', '5')")
        .await
        .unwrap();

    assert_eq!(result.get_rows()[0], Row::new(vec![TableValue::Int(3)]));
}

async fn union(service: Box<dyn SqlClient>) {
    let _ = service.exec_query("CREATE SCHEMA foo").await.unwrap();

    let _ = service
        .exec_query("CREATE TABLE foo.orders1 (customer_id text, amount int)")
        .await
        .unwrap();
    let _ = service
        .exec_query("CREATE TABLE foo.orders2 (customer_id text, amount int)")
        .await
        .unwrap();

    service
        .exec_query(
            "INSERT INTO foo.orders1 (customer_id, amount) VALUES ('a', 10), ('b', 2), ('b', 3)",
        )
        .await
        .unwrap();

    service
        .exec_query(
            "INSERT INTO foo.orders2 (customer_id, amount) VALUES ('b', 20), ('c', 20), ('b', 30)",
        )
        .await
        .unwrap();

    let result = service
        .exec_query(
            "SELECT `u`.customer_id, sum(`u`.amount) FROM \
            (select * from foo.orders1 union all select * from foo.orders2) `u` \
            WHERE `u`.customer_id like '%' GROUP BY 1 ORDER BY 2 DESC",
        )
        .await
        .unwrap();

    assert_eq!(
        result.get_rows()[0],
        Row::new(vec![
            TableValue::String("b".to_string()),
            TableValue::Int(55)
        ])
    );
    assert_eq!(
        result.get_rows()[1],
        Row::new(vec![
            TableValue::String("c".to_string()),
            TableValue::Int(20)
        ])
    );
    assert_eq!(
        result.get_rows()[2],
        Row::new(vec![
            TableValue::String("a".to_string()),
            TableValue::Int(10)
        ])
    );
}

async fn timestamp_select(service: Box<dyn SqlClient>) {
    let _ = service.exec_query("CREATE SCHEMA foo").await.unwrap();

    let _ = service
        .exec_query("CREATE TABLE foo.timestamps (t timestamp)")
        .await
        .unwrap();

    service.exec_query(
            "INSERT INTO foo.timestamps (t) VALUES ('2020-01-01T00:00:00.000Z'), ('2020-01-02T00:00:00.000Z'), ('2020-01-03T00:00:00.000Z')"
        ).await.unwrap();

    service.exec_query(
            "INSERT INTO foo.timestamps (t) VALUES ('2020-01-01T00:00:00.000Z'), ('2020-01-02T00:00:00.000Z'), ('2020-01-03T00:00:00.000Z')"
        ).await.unwrap();

    let result = service.exec_query("SELECT count(*) from foo.timestamps WHERE t >= to_timestamp('2020-01-02T00:00:00.000Z')").await.unwrap();

    assert_eq!(result.get_rows()[0], Row::new(vec![TableValue::Int(4)]));
}

async fn column_escaping(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA foo").await.unwrap();

    service
        .exec_query("CREATE TABLE foo.timestamps (t timestamp, amount int)")
        .await
        .unwrap();

    service
        .exec_query(
            "INSERT INTO foo.timestamps (t, amount) VALUES \
            ('2020-01-01T00:00:00.000Z', 1), \
            ('2020-01-01T00:01:00.000Z', 2), \
            ('2020-01-02T00:10:00.000Z', 3)",
        )
        .await
        .unwrap();

    let result = service
        .exec_query(
            "SELECT date_trunc('day', `timestamp`.t) `day`, sum(`timestamp`.amount) \
            FROM foo.timestamps `timestamp` \
            WHERE `timestamp`.t >= to_timestamp('2020-01-02T00:00:00.000Z') GROUP BY 1",
        )
        .await
        .unwrap();

    assert_eq!(
        result.get_rows()[0],
        Row::new(vec![
            TableValue::Timestamp(TimestampValue::new(1577923200000000000)),
            TableValue::Int(3)
        ])
    );
}

async fn information_schema(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA foo").await.unwrap();

    service
        .exec_query("CREATE TABLE foo.timestamps (t timestamp, amount int)")
        .await
        .unwrap();

    let result = service
        .exec_query("SELECT schema_name FROM information_schema.schemata")
        .await
        .unwrap();

    assert_eq!(
        result.get_rows(),
        &vec![Row::new(vec![TableValue::String("foo".to_string())])]
    );

    let result = service
        .exec_query("SELECT table_name FROM information_schema.tables")
        .await
        .unwrap();

    assert_eq!(
        result.get_rows(),
        &vec![Row::new(vec![TableValue::String("timestamps".to_string())])]
    );
}

async fn case_column_escaping(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA foo").await.unwrap();

    service
        .exec_query("CREATE TABLE foo.timestamps (t timestamp, amount int)")
        .await
        .unwrap();

    service
        .exec_query(
            "INSERT INTO foo.timestamps (t, amount) VALUES \
            ('2020-01-01T00:00:00.000Z', 1), \
            ('2020-01-01T00:01:00.000Z', 2), \
            ('2020-01-02T00:10:00.000Z', 3)",
        )
        .await
        .unwrap();

    let result = service.exec_query(
            "SELECT date_trunc('day', `timestamp`.t) `day`, sum(CASE WHEN `timestamp`.t > to_timestamp('2020-01-02T00:01:00.000Z') THEN `timestamp`.amount END) \
            FROM foo.timestamps `timestamp` \
            WHERE `timestamp`.t >= to_timestamp('2020-01-02T00:00:00.000Z') GROUP BY 1"
        ).await.unwrap();

    assert_eq!(
        result.get_rows()[0],
        Row::new(vec![
            TableValue::Timestamp(TimestampValue::new(1577923200000000000)),
            TableValue::Int(3)
        ])
    );
}

async fn inner_column_escaping(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA foo").await.unwrap();

    service
        .exec_query("CREATE TABLE foo.timestamps (t timestamp, amount int)")
        .await
        .unwrap();

    service
        .exec_query(
            "INSERT INTO foo.timestamps (t, amount) VALUES \
            ('2020-01-01T00:00:00.000Z', 1), \
            ('2020-01-01T00:01:00.000Z', 2), \
            ('2020-01-02T00:10:00.000Z', 3)",
        )
        .await
        .unwrap();

    let result = service
        .exec_query(
            "SELECT date_trunc('day', `t`) `day`, sum(`amount`) \
            FROM foo.timestamps `timestamp` \
            WHERE `t` >= to_timestamp('2020-01-02T00:00:00.000Z') GROUP BY 1",
        )
        .await
        .unwrap();

    assert_eq!(
        result.get_rows()[0],
        Row::new(vec![
            TableValue::Timestamp(TimestampValue::new(1577923200000000000)),
            TableValue::Int(3)
        ])
    );
}

async fn convert_tz(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA foo").await.unwrap();

    service
        .exec_query("CREATE TABLE foo.timestamps (t timestamp, amount int)")
        .await
        .unwrap();

    service
        .exec_query(
            "INSERT INTO foo.timestamps (t, amount) VALUES \
            ('2020-01-01T00:00:00.000Z', 1), \
            ('2020-01-01T00:01:00.000Z', 2), \
            ('2020-01-02T00:10:00.000Z', 3)",
        )
        .await
        .unwrap();

    let result = service
        .exec_query(
            "SELECT date_trunc('day', `t`) `day`, sum(`amount`) \
            FROM foo.timestamps `timestamp` \
            WHERE `t` >= convert_tz(to_timestamp('2020-01-02T08:00:00.000Z'), '-08:00') GROUP BY 1",
        )
        .await
        .unwrap();

    assert_eq!(
        result.get_rows(),
        &vec![Row::new(vec![
            TableValue::Timestamp(TimestampValue::new(1577923200000000000)),
            TableValue::Int(3)
        ])]
    );
}

async fn coalesce(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA s").await.unwrap();

    service
        .exec_query("CREATE TABLE s.Data (n int, v int, s text)")
        .await
        .unwrap();

    service
        .exec_query(
            "INSERT INTO s.Data (n, v, s) VALUES \
            (1, 2, 'foo'),\
            (null, 3, 'bar'),\
            (null, null, 'baz'),\
            (null, null, null)",
        )
        .await
        .unwrap();

    let r = service
        .exec_query("SELECT coalesce(1, 2, 3)")
        .await
        .unwrap();
    assert_eq!(to_rows(&r), vec![vec![TableValue::Int(1)]]);
    // TODO: the type should be 'int' here. Hopefully not a problem in practice.
    let r = service
        .exec_query("SELECT coalesce(NULL, 2, 3)")
        .await
        .unwrap();
    assert_eq!(to_rows(&r), vec![vec![TableValue::String("2".to_string())]]);
    let r = service
        .exec_query("SELECT coalesce(NULL, NULL, NULL)")
        .await
        .unwrap();
    assert_eq!(to_rows(&r), vec![vec![TableValue::Null]]);
    let r = service
        .exec_query("SELECT coalesce(n, v) FROM s.Data ORDER BY 1")
        .await
        .unwrap();
    assert_eq!(
        to_rows(&r),
        vec![
            vec![TableValue::Null],
            vec![TableValue::Null],
            vec![TableValue::Int(1)],
            vec![TableValue::Int(3)]
        ]
    );
    // Coerces all args to text.
    let r = service
        .exec_query("SELECT coalesce(n, v, s) FROM s.Data ORDER BY 1")
        .await
        .unwrap();
    assert_eq!(
        to_rows(&r),
        vec![
            vec![TableValue::Null],
            vec![TableValue::String("1".to_string())],
            vec![TableValue::String("3".to_string())],
            vec![TableValue::String("baz".to_string())]
        ]
    );

    let r = service
        .exec_query("SELECT coalesce(n+1,v+1,0) FROM s.Data ORDER BY 1")
        .await
        .unwrap();
    assert_eq!(
        to_rows(&r),
        vec![
            vec![TableValue::Int(0)],
            vec![TableValue::Int(0)],
            vec![TableValue::Int(2)],
            vec![TableValue::Int(4)],
        ]
    );

    service
        .exec_query("SELECT n, coalesce() FROM s.Data ORDER BY 1")
        .await
        .unwrap_err();
}

async fn count_distinct_crash(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA s").await.unwrap();
    service
        .exec_query("CREATE TABLE s.Data (n int)")
        .await
        .unwrap();

    let r = service
        .exec_query("SELECT COUNT(DISTINCT n) FROM s.Data")
        .await
        .unwrap();
    assert_eq!(to_rows(&r), vec![vec![TableValue::Int(0)]]);

    service
        .exec_query("INSERT INTO s.Data(n) VALUES (1), (2), (3), (3), (4), (4), (4)")
        .await
        .unwrap();

    let r = service
        .exec_query("SELECT COUNT(DISTINCT n) FROM s.Data WHERE n > 4")
        .await
        .unwrap();

    assert_eq!(to_rows(&r), vec![vec![TableValue::Int(0)]]);
    let r = service
        .exec_query("SELECT COUNT(DISTINCT CASE WHEN n > 4 THEN n END) FROM s.Data")
        .await
        .unwrap();
    assert_eq!(to_rows(&r), vec![vec![TableValue::Int(0)]]);
}

async fn create_schema_if_not_exists(service: Box<dyn SqlClient>) {
    let _ = service
        .exec_query("CREATE SCHEMA IF NOT EXISTS Foo")
        .await
        .unwrap();
    let _ = service
        .exec_query("CREATE SCHEMA IF NOT EXISTS Foo")
        .await
        .unwrap();
}

async fn create_index_before_ingestion(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA foo").await.unwrap();

    service
        .exec_query("CREATE TABLE foo.timestamps (id int, t timestamp)")
        .await
        .unwrap();

    service
        .exec_query("CREATE INDEX by_timestamp ON foo.timestamps (`t`)")
        .await
        .unwrap();

    service.exec_query(
            "INSERT INTO foo.timestamps (id, t) VALUES (1, '2020-01-01T00:00:00.000Z'), (2, '2020-01-02T00:00:00.000Z'), (3, '2020-01-03T00:00:00.000Z')"
        ).await.unwrap();

    let result = service.exec_query("SELECT count(*) from foo.timestamps WHERE t >= to_timestamp('2020-01-02T00:00:00.000Z')").await.unwrap();

    assert_eq!(result.get_rows()[0], Row::new(vec![TableValue::Int(2)]));
}

async fn ambiguous_join_sort(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA foo").await.unwrap();

    service
        .exec_query("CREATE TABLE foo.sessions (t timestamp, id int)")
        .await
        .unwrap();
    service
        .exec_query("CREATE TABLE foo.page_views (session_id int, page_view_count int)")
        .await
        .unwrap();

    service
        .exec_query("CREATE INDEX by_id ON foo.sessions (id)")
        .await
        .unwrap();

    service.exec_query(
            "INSERT INTO foo.sessions (t, id) VALUES ('2020-01-01T00:00:00.000Z', 1), ('2020-01-02T00:00:00.000Z', 2), ('2020-01-03T00:00:00.000Z', 3)"
        ).await.unwrap();

    service.exec_query(
            "INSERT INTO foo.page_views (session_id, page_view_count) VALUES (1, 10), (2, 20), (3, 30)"
        ).await.unwrap();

    let result = service.exec_query("SELECT sum(p.page_view_count) from foo.sessions s JOIN foo.page_views p ON s.id = p.session_id WHERE s.t >= to_timestamp('2020-01-02T00:00:00.000Z')").await.unwrap();

    assert_eq!(result.get_rows()[0], Row::new(vec![TableValue::Int(50)]));
}

async fn join_with_aliases(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA foo").await.unwrap();

    service
        .exec_query("CREATE TABLE foo.sessions (t timestamp, id int)")
        .await
        .unwrap();
    service
        .exec_query("CREATE TABLE foo.page_views (session_id int, page_view_count int)")
        .await
        .unwrap();

    service
        .exec_query("CREATE INDEX by_id ON foo.sessions (id)")
        .await
        .unwrap();

    service.exec_query(
            "INSERT INTO foo.sessions (t, id) VALUES ('2020-01-01T00:00:00.000Z', 1), ('2020-01-02T00:00:00.000Z', 2), ('2020-01-03T00:00:00.000Z', 3)"
        ).await.unwrap();

    service.exec_query(
            "INSERT INTO foo.page_views (session_id, page_view_count) VALUES (1, 10), (2, 20), (3, 30)"
        ).await.unwrap();

    let result = service.exec_query("SELECT sum(`page_view_count`) from foo.sessions `sessions` JOIN foo.page_views `page_views` ON `id` = `session_id` WHERE `t` >= to_timestamp('2020-01-02T00:00:00.000Z')").await.unwrap();

    assert_eq!(result.get_rows()[0], Row::new(vec![TableValue::Int(50)]));
}

async fn group_by_without_aggregates(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA foo").await.unwrap();

    service
        .exec_query(
            "CREATE TABLE foo.sessions (id int, company_id int, location_id int, t timestamp)",
        )
        .await
        .unwrap();

    service
        .exec_query("CREATE INDEX by_company ON foo.sessions (company_id, location_id, id)")
        .await
        .unwrap();

    service.exec_query(
            "INSERT INTO foo.sessions (company_id, location_id, t, id) VALUES (1, 1, '2020-01-01T00:00:00.000Z', 1), (1, 2, '2020-01-02T00:00:00.000Z', 2), (2, 1, '2020-01-03T00:00:00.000Z', 3)"
        ).await.unwrap();

    let result = service.exec_query("SELECT `sessions`.location_id, `sessions`.id FROM foo.sessions `sessions` GROUP BY 1, 2 ORDER BY 2").await.unwrap();

    assert_eq!(
        result.get_rows(),
        &vec![
            Row::new(vec![TableValue::Int(1), TableValue::Int(1)]),
            Row::new(vec![TableValue::Int(2), TableValue::Int(2)]),
            Row::new(vec![TableValue::Int(1), TableValue::Int(3)]),
        ]
    );
}

async fn create_table_with_location(service: Box<dyn SqlClient>) {
    let paths = {
        let dir = env::temp_dir();

        let path_1 = dir.clone().join("foo-1.csv");
        let path_2 = dir.clone().join("foo-2.csv.gz");
        let mut file = File::create(path_1.clone()).unwrap();

        file.write_all("id,city,arr,t\n".as_bytes()).unwrap();
        file.write_all("1,San Francisco,\"[\"\"Foo\n\n\"\",\"\"Bar\"\",\"\"FooBar\"\"]\",\"2021-01-24 12:12:23 UTC\"\n".as_bytes()).unwrap();
        file.write_all("2,\"New York\",\"[\"\"\"\"]\",2021-01-24 19:12:23.123 UTC\n".as_bytes())
            .unwrap();
        file.write_all("3,New York,\"de Comunicación\",2021-01-25 19:12:23 UTC\n".as_bytes())
            .unwrap();

        let mut file = GzipEncoder::new(BufWriter::new(
            tokio::fs::File::create(path_2.clone()).await.unwrap(),
        ));

        file.write_all("id,city,arr,t\n".as_bytes()).await.unwrap();
        file.write_all("1,San Francisco,\"[\"\"Foo\"\",\"\"Bar\"\",\"\"FooBar\"\"]\",\"2021-01-24 12:12:23 UTC\"\n".as_bytes()).await.unwrap();
        file.write_all("2,\"New York\",\"[\"\"\"\"]\",2021-01-24 19:12:23 UTC\n".as_bytes())
            .await
            .unwrap();
        file.write_all("3,New York,,2021-01-25 19:12:23 UTC\n".as_bytes())
            .await
            .unwrap();
        file.write_all("4,New York,\"\",2021-01-25 19:12:23 UTC\n".as_bytes())
            .await
            .unwrap();
        file.write_all("5,New York,\"\",2021-01-25 19:12:23 UTC\n".as_bytes())
            .await
            .unwrap();

        file.shutdown().await.unwrap();

        vec![path_1, path_2]
    };

    let _ = service
        .exec_query("CREATE SCHEMA IF NOT EXISTS Foo")
        .await
        .unwrap();
    let _ = service.exec_query(
            &format!(
                "CREATE TABLE Foo.Persons (id int, city text, t timestamp, arr text) INDEX persons_city (`city`, `id`) LOCATION {}",
                paths.into_iter().map(|p| format!("'{}'", p.to_string_lossy())).join(",")
            )
        ).await.unwrap();
    let res = service
        .exec_query("CREATE INDEX by_city ON Foo.Persons (city)")
        .await;
    let error = format!("{:?}", res);
    assert!(error.contains("has data"));

    let result = service
        .exec_query("SELECT count(*) as cnt from Foo.Persons")
        .await
        .unwrap();
    assert_eq!(result.get_rows(), &vec![Row::new(vec![TableValue::Int(8)])]);

    let result = service.exec_query("SELECT count(*) as cnt from Foo.Persons WHERE arr = '[\"Foo\",\"Bar\",\"FooBar\"]' or arr = '[\"\"]' or arr is null").await.unwrap();
    assert_eq!(result.get_rows(), &vec![Row::new(vec![TableValue::Int(6)])]);
}

async fn create_table_with_url(service: Box<dyn SqlClient>) {
    let url = "https://data.wprdc.org/dataset/0b584c84-7e35-4f4d-a5a2-b01697470c0f/resource/e95dd941-8e47-4460-9bd8-1e51c194370b/download/bikepghpublic.csv";

    service
        .exec_query("CREATE SCHEMA IF NOT EXISTS foo")
        .await
        .unwrap();
    let create_table_sql = format!("CREATE TABLE foo.bikes (`Response ID` int, `Start Date` text, `End Date` text) LOCATION '{}'", url);
    let (_, query_result) = tokio::join!(
        service.exec_query(&create_table_sql),
        service.exec_query("SELECT count(*) from foo.bikes")
    );
    assert!(
        query_result.is_err(),
        "Table shouldn't be ready but querying returns {:?}",
        query_result
    );

    let result = service
        .exec_query("SELECT count(*) from foo.bikes")
        .await
        .unwrap();
    assert_eq!(
        result.get_rows(),
        &vec![Row::new(vec![TableValue::Int(813)])]
    );
}

async fn create_table_fail_and_retry(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA s").await.unwrap();
    service
        .exec_query(
            "CREATE TABLE s.Data(n int, v int) INDEX reverse (v,n) LOCATION 'non-existing-file'",
        )
        .await
        .unwrap_err();
    service
        .exec_query("CREATE TABLE s.Data(n int, v int) INDEX reverse (v,n)")
        .await
        .unwrap();
    service
        .exec_query("INSERT INTO s.Data(n, v) VALUES (1, -1), (2, -2)")
        .await
        .unwrap();
    let rows = service
        .exec_query("SELECT n FROM s.Data ORDER BY n")
        .await
        .unwrap();
    assert_eq!(
        to_rows(&rows),
        vec![vec![TableValue::Int(1)], vec![TableValue::Int(2)]]
    );
}

async fn empty_crash(service: Box<dyn SqlClient>) {
    let _ = service
        .exec_query("CREATE SCHEMA IF NOT EXISTS s")
        .await
        .unwrap();
    let _ = service
        .exec_query("CREATE TABLE s.Table (id int, s int)")
        .await
        .unwrap();
    let _ = service
        .exec_query("INSERT INTO s.Table(id, s) VALUES (1, 10);")
        .await
        .unwrap();

    let r = service
        .exec_query("SELECT * from s.Table WHERE id = 1 AND s = 15")
        .await
        .unwrap();
    assert_eq!(r.get_rows(), &vec![]);

    let r = service
        .exec_query("SELECT id, sum(s) from s.Table WHERE id = 1 AND s = 15 GROUP BY 1")
        .await
        .unwrap();
    assert_eq!(r.get_rows(), &vec![]);
}

async fn bytes(service: Box<dyn SqlClient>) {
    let _ = service
        .exec_query("CREATE SCHEMA IF NOT EXISTS s")
        .await
        .unwrap();
    let _ = service
        .exec_query("CREATE TABLE s.Bytes (id int, data bytea)")
        .await
        .unwrap();
    let _ = service
        .exec_query(
            "INSERT INTO s.Bytes(id, data) VALUES (1, '01 ff 1a'), (2, X'deADbeef'), (3, 456)",
        )
        .await
        .unwrap();

    let result = service.exec_query("SELECT * from s.Bytes").await.unwrap();
    let r = result.get_rows();
    assert_eq!(r.len(), 3);
    assert_eq!(r[0].values()[1], TableValue::Bytes(vec![0x01, 0xff, 0x1a]));
    assert_eq!(
        r[1].values()[1],
        TableValue::Bytes(vec![0xde, 0xad, 0xbe, 0xef])
    );
    assert_eq!(
        r[2].values()[1],
        TableValue::Bytes("456".as_bytes().to_vec())
    );
}

async fn hyperloglog(service: Box<dyn SqlClient>) {
    let _ = service
        .exec_query("CREATE SCHEMA IF NOT EXISTS hll")
        .await
        .unwrap();
    let _ = service
        .exec_query("CREATE TABLE hll.sketches (id int, hll varbinary)")
        .await
        .unwrap();

    let sparse = "X'020C0200C02FF58941D5F0C6'";
    let dense = "X'030C004020000001000000000000000000000000000000000000050020000001030100000410000000004102100000000000000051000020000020003220000003102000000000001200042000000001000200000002000000100000030040000000010040003010000000000100002000000000000000000031000020000000000000000000100000200302000000000000000000001002000000000002204000000001000001000200400000000000001000020031100000000080000000002003000000100000000100110000000000000000000010000000000000000000000020000001320205000100000612000000000004100020100000000000000000001000000002200000100000001000001020000000000020000000000000001000010300060000010000000000070100003000000000000020000000000001000010000104000000000000000000101000100000001401000000000000000000000000000100010000000000000000000000000400020000000002002300010000000000040000041000200005100000000000001000000000100000203010000000000000000000000000001006000100000000000000300100001000100254200000000000101100040000000020000010000050000000501000000000101020000000010000000003000000000200000102100000000204007000000200010000033000000000061000000000000000000000000000000000100001000001000000013000000003000000000002000000000000010001000000000000000000020010000020000000100001000000000000001000103000000000000000000020020000001000000000100001000000000000000020220200200000001001000010100000000200000000000001000002000000011000000000101200000000000000000000000000000000000000100130000000000000000000100000120000300040000000002000000000000000000000100000000070000100000000301000000401200002020000000000601030001510000000000000110100000000000000000050000000010000100000000000000000100022000100000101054010001000000000000001000001000000002000000000100000000000021000001000002000000000100000000000000000000951000000100000000000000000000000000102000200000000000000010000010000000000100002000000000000000000010000000000000010000000010000000102010000000010520100000021010100000030000000000000000100000001000000022000330051000000100000000000040003020000010000020000100000013000000102020000000050000000020010000000000000000101200C000100000001200400000000010000001000000000100010000000001000001000000100000000010000000004000000002000013102000100000000000000000000000600000010000000000000020000000000001000000000030000000000000020000000001000001000000000010000003002000003000200070001001003030010000000003000000000000020000006000000000000000011000000010000200000000000500000000000000020500000000003000000000000000004000030000100000000103000001000000000000200002004200000020000000030000000000000000000000002000100000000000000002000000000000000010020101000000005250000010000000000023010000001000000000000500002001000123100030011000020001310600000000000021000023000003000000000000000001000000000000220200000000004040000020201000000010201000000000020000400010000050000000000000000000000010000020000000000000000000000000000000000102000010000000000000000000000002010000200200000000000000000000000000100000000000000000200400000000010000000000000000000000000000000010000200300000000000100110000000000000000000000000010000030000001000000000010000010200013000000000000200000001000001200010000000010000000000001000000000000100000000410000040000001000100010000100000002001010000000000000000001000000000000010000000000000000000000002000000000001100001000000001010000000000000002200000000004000000000000100010000000000600000000100300000000000000000000010000003000000000000000000310000010100006000010001000000000000001010101000100000000000000000000000000000201000000000000000700010000030000000000000021000000000000000001020000000030000100001000000000000000000000004010100000000000000000000004000000040100000040100100001000000000300000100000000010010000300000200000000000001302000000000000000000100100000400030000001001000100100002300000004030000002010000220100000000000002000000010010000000003010500000000300000000005020102000200000000000000020100000000000000000000000011000000023000000000010000101000000000000010020040200040000020000004000020000000001000000000100000200000010000000000030100010001000000100000000000600400000000002000000000000132000000900010000000030021400000000004100006000304000000000000010000106000001300020000'";

    service
        .exec_query(&format!(
            "INSERT INTO hll.sketches (id, hll) VALUES (1, {s}), (2, {d}), (3, {s}), (4, {d})",
            s = sparse,
            d = dense
        ))
        .await
        .unwrap();

    //  Check cardinality.
    let result = service
        .exec_query("SELECT id, cardinality(hll) as cnt from hll.sketches WHERE id < 3 ORDER BY 1")
        .await
        .unwrap();
    assert_eq!(
        to_rows(&result),
        vec![
            vec![TableValue::Int(1), TableValue::Int(2)],
            vec![TableValue::Int(2), TableValue::Int(655)]
        ]
    );
    // Check merge and cardinality.
    let result = service
        .exec_query("SELECT cardinality(merge(hll)) from hll.sketches WHERE id < 3")
        .await
        .unwrap();
    assert_eq!(to_rows(&result), vec![vec![TableValue::Int(657)]]);

    // Now merge all 4 HLLs, results should stay the same.
    let result = service
        .exec_query("SELECT cardinality(merge(hll)) from hll.sketches")
        .await
        .unwrap();
    assert_eq!(to_rows(&result), vec![vec![TableValue::Int(657)]]);

    // TODO: add format checks on insert and test invalid inputs.
}

async fn hyperloglog_empty_inputs(service: Box<dyn SqlClient>) {
    let _ = service
        .exec_query("CREATE SCHEMA IF NOT EXISTS hll")
        .await
        .unwrap();
    let _ = service
        .exec_query("CREATE TABLE hll.sketches (id int, hll varbinary)")
        .await
        .unwrap();

    let result = service
        .exec_query("SELECT cardinality(merge(hll)) from hll.sketches")
        .await
        .unwrap();
    assert_eq!(to_rows(&result), vec![vec![TableValue::Int(0)]]);

    let result = service
        .exec_query("SELECT merge(hll) from hll.sketches")
        .await
        .unwrap();
    assert_eq!(to_rows(&result), vec![vec![TableValue::Bytes(vec![])]]);
}

async fn hyperloglog_empty_group_by(service: Box<dyn SqlClient>) {
    let _ = service
        .exec_query("CREATE SCHEMA IF NOT EXISTS hll")
        .await
        .unwrap();
    let _ = service
        .exec_query("CREATE TABLE hll.sketches (id int, key int, hll varbinary)")
        .await
        .unwrap();

    let result = service
        .exec_query("SELECT key, cardinality(merge(hll)) from hll.sketches group by key")
        .await
        .unwrap();
    assert_eq!(to_rows(&result), Vec::<Vec<TableValue>>::new());
}

async fn hyperloglog_inserts(service: Box<dyn SqlClient>) {
    let _ = service
        .exec_query("CREATE SCHEMA IF NOT EXISTS hll")
        .await
        .unwrap();
    let _ = service
        .exec_query("CREATE TABLE hll.sketches (id int, hll hyperloglog)")
        .await
        .unwrap();

    service
        .exec_query("INSERT INTO hll.sketches(id, hll) VALUES (0, X'')")
        .await
        .expect_err("should not allow invalid HLL");
    service
        .exec_query("INSERT INTO hll.sketches(id, hll) VALUES (0, X'020C0200C02FF58941D5F0C6')")
        .await
        .expect("should allow valid HLL");
    service
        .exec_query("INSERT INTO hll.sketches(id, hll) VALUES (0, X'020C0200C02FF58941D5F0C6123')")
        .await
        .expect_err("should not allow invalid HLL (with extra bytes)");
}

async fn hyperloglog_inplace_group_by(service: Box<dyn SqlClient>) {
    let _ = service
        .exec_query("CREATE SCHEMA IF NOT EXISTS hll")
        .await
        .unwrap();
    let _ = service
        .exec_query("CREATE TABLE hll.sketches1(id int, hll hyperloglog)")
        .await
        .unwrap();
    let _ = service
        .exec_query("CREATE TABLE hll.sketches2(id int, hll hyperloglog)")
        .await
        .unwrap();

    service
        .exec_query(
            "INSERT INTO hll.sketches1(id, hll) \
                     VALUES (0, X'020C0200C02FF58941D5F0C6'), \
                            (1, X'020C0200C02FF58941D5F0C6')",
        )
        .await
        .unwrap();
    service
        .exec_query(
            "INSERT INTO hll.sketches2(id, hll) \
                     VALUES (1, X'020C0200C02FF58941D5F0C6'), \
                            (2, X'020C0200C02FF58941D5F0C6')",
        )
        .await
        .unwrap();

    // Case expression should handle binary results.
    service
        .exec_query(
            "SELECT id, CASE WHEN id = 0 THEN merge(hll) ELSE merge(hll) END \
             FROM hll.sketches1 \
             GROUP BY 1",
        )
        .await
        .unwrap();
    // Without the ELSE branch.
    service
        .exec_query(
            "SELECT id, CASE WHEN id = 0 THEN merge(hll) END \
             FROM hll.sketches1 \
             GROUP BY 1",
        )
        .await
        .unwrap();
    // Binary type in condition. For completeness, probably not very useful in practice.
    // TODO: this fails for unrelated reasons, binary support is ad-hoc at this point.
    //       uncomment when fixed.
    // service.exec_query(
    //     "SELECT id, CASE hll WHEN '' THEN NULL else hll END \
    //      FROM hll.sketches1",
    // ).await.unwrap();

    // MergeSortExec uses the same code as case expression internally.
    let rows = service
        .exec_query(
            "SELECT id, cardinality(merge(hll)) \
             FROM
               (SELECT * FROM hll.sketches1
                UNION ALL
                SELECT * FROM hll.sketches2) \
             GROUP BY 1 ORDER BY 1",
        )
        .await
        .unwrap();
    assert_eq!(
        to_rows(&rows),
        vec![
            vec![TableValue::Int(0), TableValue::Int(2)],
            vec![TableValue::Int(1), TableValue::Int(2)],
            vec![TableValue::Int(2), TableValue::Int(2)],
        ]
    )
}

async fn hyperloglog_snowflake(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA s").await.unwrap();
    service
        .exec_query("CREATE TABLE s.Data(id int, hll HLL_SNOWFLAKE) ")
        .await
        .unwrap();
    service.exec_query(r#"INSERT INTO s.Data(id, hll) VALUES (1, '{"precision": 12,
                          "sparse": {
                            "indices": [223,736,976,1041,1256,1563,1811,2227,2327,2434,2525,2656,2946,2974,3256,3745,3771,4066],
                            "maxLzCounts": [1,2,1,4,2,2,3,1,1,2,4,2,1,1,2,3,2,1]
                          },
                          "version": 4
                        }')"#).await.unwrap();

    let r = service
        .exec_query("SELECT id, cardinality(hll) FROM s.Data")
        .await
        .unwrap();
    assert_eq!(
        to_rows(&r),
        vec![vec![TableValue::Int(1), TableValue::Int(18)]]
    );

    // Does not allow to import HLL in AirLift format.
    service
        .exec_query("INSERT INTO s.Data(id, hll) VALUES(2, X'020C0200C02FF58941D5F0C6')")
        .await
        .unwrap_err();
}

async fn planning_inplace_aggregate(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA s").await.unwrap();
    service
        .exec_query("CREATE TABLE s.Data(url text, day int, hits int)")
        .await
        .unwrap();

    let p = service
        .plan_query("SELECT url, SUM(hits) FROM s.Data GROUP BY 1")
        .await
        .unwrap();
    assert_eq!(
        pp_phys_plan(p.router.as_ref()),
        "FinalInplaceAggregate\
          \n  ClusterSend, partitions: [[1]]"
    );
    assert_eq!(
        pp_phys_plan(p.worker.as_ref()),
        "FinalInplaceAggregate\
           \n  Worker\
           \n    PartialInplaceAggregate\
           \n      MergeSort\
           \n        Scan, index: default:1:[1]:sort_on[url], fields: [url, hits]\
           \n          Empty"
    );

    // When there is no index, we fallback to inplace aggregates.
    let p = service
        .plan_query("SELECT day, SUM(hits) FROM s.Data GROUP BY 1")
        .await
        .unwrap();
    assert_eq!(
        pp_phys_plan(p.router.as_ref()),
        "FinalHashAggregate\
          \n  ClusterSend, partitions: [[1]]"
    );
    assert_eq!(
        pp_phys_plan(p.worker.as_ref()),
        "FinalHashAggregate\
           \n  Worker\
           \n    PartialHashAggregate\
           \n      Merge\
           \n        Scan, index: default:1:[1], fields: [day, hits]\
           \n          Empty"
    );
}

async fn planning_hints(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA s").await.unwrap();
    service
        .exec_query("CREATE TABLE s.Data(id1 int, id2 int, id3 int)")
        .await
        .unwrap();

    let mut show_hints = PPOptions::default();
    show_hints.show_output_hints = true;

    // Merge produces a sort order because there is only single partition.
    let p = service
        .plan_query("SELECT id1, id2 FROM s.Data")
        .await
        .unwrap();
    assert_eq!(
        pp_phys_plan_ext(p.worker.as_ref(), &show_hints),
        "Worker, sort_order: [0, 1]\
          \n  Projection, [id1, id2], sort_order: [0, 1]\
          \n    Merge, sort_order: [0, 1]\
          \n      Scan, index: default:1:[1], fields: [id1, id2], sort_order: [0, 1]\
          \n        Empty"
    );

    let p = service
        .plan_query("SELECT id2, id1 FROM s.Data")
        .await
        .unwrap();
    assert_eq!(
        pp_phys_plan_ext(p.worker.as_ref(), &show_hints),
        "Worker, sort_order: [1, 0]\
            \n  Projection, [id2, id1], sort_order: [1, 0]\
            \n    Merge, sort_order: [0, 1]\
            \n      Scan, index: default:1:[1], fields: [id1, id2], sort_order: [0, 1]\
            \n        Empty"
    );

    // Unsorted when skips columns from sort prefix.
    let p = service
        .plan_query("SELECT id2, id3 FROM s.Data")
        .await
        .unwrap();
    assert_eq!(
        pp_phys_plan_ext(p.worker.as_ref(), &show_hints),
        "Worker\
          \n  Projection, [id2, id3]\
          \n    Merge\
          \n      Scan, index: default:1:[1], fields: [id2, id3]\
          \n        Empty"
    );

    // The prefix columns are still sorted.
    let p = service
        .plan_query("SELECT id1, id3 FROM s.Data")
        .await
        .unwrap();
    assert_eq!(
        pp_phys_plan_ext(p.worker.as_ref(), &show_hints),
        "Worker, sort_order: [0]\
           \n  Projection, [id1, id3], sort_order: [0]\
           \n    Merge, sort_order: [0]\
           \n      Scan, index: default:1:[1], fields: [id1, id3], sort_order: [0]\
           \n        Empty"
    );

    // Single value hints.
    let p = service
        .plan_query("SELECT id3, id2 FROM s.Data WHERE id2 = 234")
        .await
        .unwrap();
    assert_eq!(
        pp_phys_plan_ext(p.worker.as_ref(), &show_hints),
        "Worker, single_vals: [1]\
           \n  Projection, [id3, id2], single_vals: [1]\
           \n    Filter, single_vals: [0]\
           \n      Merge\
           \n        Scan, index: default:1:[1], fields: [id2, id3]\
           \n          Empty"
    );

    // Removing single value columns should keep the sort order of the rest.
    let p = service
        .plan_query("SELECT id3 FROM s.Data WHERE id1 = 123 AND id2 = 234")
        .await
        .unwrap();
    assert_eq!(
        pp_phys_plan_ext(p.worker.as_ref(), &show_hints),
        "Worker, sort_order: [0]\
           \n  Projection, [id3], sort_order: [0]\
           \n    Filter, single_vals: [0, 1], sort_order: [0, 1, 2]\
           \n      Merge, sort_order: [0, 1, 2]\
           \n        Scan, index: default:1:[1], fields: *, sort_order: [0, 1, 2]\
           \n          Empty"
    );
    let p = service
        .plan_query("SELECT id1, id3 FROM s.Data WHERE id2 = 234")
        .await
        .unwrap();
    assert_eq!(
        pp_phys_plan_ext(p.worker.as_ref(), &show_hints),
        "Worker, sort_order: [0, 1]\
           \n  Projection, [id1, id3], sort_order: [0, 1]\
           \n    Filter, single_vals: [1], sort_order: [0, 1, 2]\
           \n      Merge, sort_order: [0, 1, 2]\
           \n        Scan, index: default:1:[1], fields: *, sort_order: [0, 1, 2]\
           \n          Empty"
    );
}

async fn planning_inplace_aggregate2(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA s").await.unwrap();
    service
        .exec_query(
            "CREATE TABLE s.Data1(allowed boolean, site_id int, url text, day timestamp, hits int)",
        )
        .await
        .unwrap();
    service
        .exec_query(
            "CREATE TABLE s.Data2(allowed boolean, site_id int, url text, day timestamp, hits int)",
        )
        .await
        .unwrap();

    let p = service
        .plan_query(
            "SELECT `url` `url`, SUM(`hits`) `hits` \
                         FROM (SELECT * FROM s.Data1 \
                               UNION ALL \
                               SELECT * FROM s.Data2) AS `Data` \
                         WHERE (`allowed` = 'true') AND (`site_id` = '1') \
                               AND (`day` >= to_timestamp('2021-01-01T00:00:00.000') \
                                AND `day` <= to_timestamp('2021-01-02T23:59:59.999')) \
                         GROUP BY 1 \
                         ORDER BY 2 DESC \
                         LIMIT 10",
        )
        .await
        .unwrap();

    let mut verbose = PPOptions::default();
    verbose.show_output_hints = true;
    verbose.show_sort_by = true;
    assert_eq!(
        pp_phys_plan_ext(p.router.as_ref(), &verbose),
        "Projection, [url, SUM(hits):hits]\
           \n  AggregateTopK, limit: 10, sortBy: [2 desc]\
           \n    ClusterSend, partitions: [[1, 2]]"
    );
    assert_eq!(
            pp_phys_plan_ext(p.worker.as_ref(), &verbose),
            "Projection, [url, SUM(hits):hits]\
           \n  AggregateTopK, limit: 10, sortBy: [2 desc]\
           \n    Worker\
           \n      Sort, by: [SUM(hits) desc]\
           \n        FullInplaceAggregate, sort_order: [0]\
           \n          Alias, single_vals: [0, 1], sort_order: [0, 1, 2, 3, 4]\
           \n            MergeSort, single_vals: [0, 1], sort_order: [0, 1, 2, 3, 4]\
           \n              Union, single_vals: [0, 1], sort_order: [0, 1, 2, 3, 4]\
           \n                Projection, [allowed, site_id, url, day, hits], single_vals: [0, 1], sort_order: [0, 1, 2, 3, 4]\
           \n                  Filter, single_vals: [0, 1], sort_order: [0, 1, 2, 3, 4]\
           \n                    MergeSort, sort_order: [0, 1, 2, 3, 4]\
           \n                      Scan, index: default:1:[1], fields: *, sort_order: [0, 1, 2, 3, 4]\
           \n                        Empty\
           \n                Projection, [allowed, site_id, url, day, hits], single_vals: [0, 1], sort_order: [0, 1, 2, 3, 4]\
           \n                  Filter, single_vals: [0, 1], sort_order: [0, 1, 2, 3, 4]\
           \n                    MergeSort, sort_order: [0, 1, 2, 3, 4]\
           \n                      Scan, index: default:2:[2], fields: *, sort_order: [0, 1, 2, 3, 4]\
           \n                        Empty"
        );
}

async fn topk_large_inputs(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA s").await.unwrap();
    service
        .exec_query("CREATE TABLE s.Data1(url text, hits int)")
        .await
        .unwrap();
    service
        .exec_query("CREATE TABLE s.Data2(url text, hits int)")
        .await
        .unwrap();

    const NUM_ROWS: i64 = 5 + MIN_TOPK_STREAM_ROWS as i64;

    let insert_data = |table, compute_hits: fn(i64) -> i64| {
        let service = &service;
        return async move {
            let mut values = String::new();
            for i in 0..NUM_ROWS {
                if !values.is_empty() {
                    values += ", "
                }
                values += &format!("('url{}', {})", i, compute_hits(i as i64));
            }
            service
                .exec_query(&format!(
                    "INSERT INTO s.{}(url, hits) VALUES {}",
                    table, values
                ))
                .await
                .unwrap();
        };
    };

    // Arrange so that top-k fully downloads both tables.
    insert_data("Data1", |i| i).await;
    insert_data("Data2", |i| NUM_ROWS - 2 * i).await;

    let query = "SELECT `url` `url`, SUM(`hits`) `hits` \
                     FROM (SELECT * FROM s.Data1 \
                           UNION ALL \
                           SELECT * FROM s.Data2) AS `Data` \
                     GROUP BY 1 \
                     ORDER BY 2 DESC \
                     LIMIT 10";

    let rows = service.exec_query(query).await.unwrap().get_rows().clone();
    assert_eq!(rows.len(), 10);
    for i in 0..10 {
        match &rows[i].values()[0] {
            TableValue::String(s) => assert_eq!(s, &format!("url{}", i)),
            v => panic!("invalid value in row {}: {:?}", i, v),
        }
        assert_eq!(
            rows[i].values()[1],
            TableValue::Int(NUM_ROWS - i as i64),
            "row {}",
            i
        );
    }
}

async fn planning_simple(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA s").await.unwrap();
    service
        .exec_query("CREATE TABLE s.Orders(id int, customer_id int, city text, amount int)")
        .await
        .unwrap();

    let p = service
        .plan_query("SELECT id, amount FROM s.Orders")
        .await
        .unwrap();
    assert_eq!(
        pp_phys_plan(p.router.as_ref()),
        "ClusterSend, partitions: [[1]]"
    );
    assert_eq!(
        pp_phys_plan(p.worker.as_ref()),
        "Worker\
           \n  Projection, [id, amount]\
           \n    Merge\
           \n      Scan, index: default:1:[1], fields: [id, amount]\
           \n        Empty"
    );

    let p = service
        .plan_query("SELECT id, amount FROM s.Orders WHERE id > 10")
        .await
        .unwrap();
    assert_eq!(
        pp_phys_plan(p.router.as_ref()),
        "ClusterSend, partitions: [[1]]"
    );
    assert_eq!(
        pp_phys_plan(p.worker.as_ref()),
        "Worker\
           \n  Projection, [id, amount]\
           \n    Filter\
           \n      Merge\
           \n        Scan, index: default:1:[1], fields: [id, amount]\
           \n          Empty"
    );

    let p = service
        .plan_query(
            "SELECT id, amount \
                 FROM s.Orders \
                 WHERE id > 10\
                 ORDER BY 2",
        )
        .await
        .unwrap();
    assert_eq!(
        pp_phys_plan(p.router.as_ref()),
        "Sort\
           \n  ClusterSend, partitions: [[1]]"
    );
    assert_eq!(
        pp_phys_plan(p.worker.as_ref()),
        "Sort\
           \n  Worker\
           \n    Projection, [id, amount]\
           \n      Filter\
           \n        Merge\
           \n          Scan, index: default:1:[1], fields: [id, amount]\
           \n            Empty"
    );

    let p = service
        .plan_query(
            "SELECT id, amount \
                 FROM s.Orders \
                 WHERE id > 10 \
                 LIMIT 10",
        )
        .await
        .unwrap();
    assert_eq!(
        pp_phys_plan(p.router.as_ref()),
        "GlobalLimit, n: 10\
           \n  ClusterSend, partitions: [[1]]"
    );
    assert_eq!(
        pp_phys_plan(p.worker.as_ref()),
        "GlobalLimit, n: 10\
           \n  Worker\
           \n    Projection, [id, amount]\
           \n      Filter\
           \n        Merge\
           \n          Scan, index: default:1:[1], fields: [id, amount]\
           \n            Empty"
    );

    let p = service
        .plan_query(
            "SELECT id, SUM(amount) \
                                    FROM s.Orders \
                                    GROUP BY 1",
        )
        .await
        .unwrap();
    assert_eq!(
        pp_phys_plan(p.router.as_ref()),
        "FinalInplaceAggregate\
           \n  ClusterSend, partitions: [[1]]"
    );
    assert_eq!(
        pp_phys_plan(p.worker.as_ref()),
        "FinalInplaceAggregate\
           \n  Worker\
           \n    PartialInplaceAggregate\
           \n      MergeSort\
           \n        Scan, index: default:1:[1]:sort_on[id], fields: [id, amount]\
           \n          Empty"
    );

    let p = service
        .plan_query(
            "SELECT id, SUM(amount) \
                 FROM (SELECT * FROM s.Orders \
                       UNION ALL \
                       SELECT * FROM s.Orders)\
                 GROUP BY 1",
        )
        .await
        .unwrap();
    // TODO: test MergeSort node is present if ClusterSend has multiple partitions.
    assert_eq!(
        pp_phys_plan(p.router.as_ref()),
        "FinalInplaceAggregate\
           \n  ClusterSend, partitions: [[1, 1]]"
    );
    assert_eq!(
        pp_phys_plan(p.worker.as_ref()),
        "FinalInplaceAggregate\
           \n  Worker\
           \n    PartialInplaceAggregate\
           \n      MergeSort\
           \n        Union\
           \n          Projection, [id, amount]\
           \n            MergeSort\
           \n              Scan, index: default:1:[1]:sort_on[id], fields: [id, amount]\
           \n                Empty\
           \n          Projection, [id, amount]\
           \n            MergeSort\
           \n              Scan, index: default:1:[1]:sort_on[id], fields: [id, amount]\
           \n                Empty"
    );
}

async fn planning_joins(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA s").await.unwrap();
    service
        .exec_query("CREATE TABLE s.Orders(order_id int, customer_id int, amount int)")
        .await
        .unwrap();
    service
        .exec_query("CREATE INDEX by_customer ON s.Orders(customer_id)")
        .await
        .unwrap();
    service
        .exec_query("CREATE TABLE s.Customers(customer_id int, customer_name text)")
        .await
        .unwrap();

    let p = service
        .plan_query(
            "SELECT order_id, customer_name \
                 FROM s.Orders `o`\
                 JOIN s.Customers `c` ON o.customer_id = c.customer_id",
        )
        .await
        .unwrap();
    assert_eq!(
        pp_phys_plan(p.router.as_ref()),
        "ClusterSend, partitions: [[2, 3]]"
    );
    assert_eq!(
            pp_phys_plan(p.worker.as_ref()),
            "Worker\
                  \n  Projection, [order_id, customer_name]\
                  \n    MergeJoin, on: [o.customer_id = c.customer_id]\
                  \n      Alias\
                  \n        MergeSort\
                  \n          Scan, index: by_customer:2:[2]:sort_on[customer_id], fields: [customer_id, order_id]\
                  \n            Empty\
                  \n      Alias\
                  \n        MergeSort\
                  \n          Scan, index: default:3:[3]:sort_on[customer_id], fields: *\
                  \n            Empty"
        );

    let p = service
        .plan_query(
            "SELECT order_id, customer_name, SUM(amount) \
                                    FROM s.Orders `o` \
                                    JOIN s.Customers `c` ON o.customer_id = c.customer_id \
                                    GROUP BY 1, 2 \
                                    ORDER BY 3 DESC",
        )
        .await
        .unwrap();
    assert_eq!(
        pp_phys_plan(p.router.as_ref()),
        "Sort\
                  \n  FinalHashAggregate\
                  \n    ClusterSend, partitions: [[2, 3]]"
    );
    assert_eq!(
        pp_phys_plan(p.worker.as_ref()),
        "Sort\
                  \n  FinalHashAggregate\
                  \n    Worker\
                  \n      PartialHashAggregate\
                  \n        MergeJoin, on: [o.customer_id = c.customer_id]\
                  \n          Alias\
                  \n            MergeSort\
                  \n              Scan, index: by_customer:2:[2]:sort_on[customer_id], fields: *\
                  \n                Empty\
                  \n          Alias\
                  \n            MergeSort\
                  \n              Scan, index: default:3:[3]:sort_on[customer_id], fields: *\
                  \n                Empty"
    );
}

async fn planning_3_table_joins(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA s").await.unwrap();
    service
        .exec_query(
            "CREATE TABLE s.Orders(order_id int, customer_id int, product_id int, amount int)",
        )
        .await
        .unwrap();
    service
        .exec_query("CREATE INDEX by_customer ON s.Orders(customer_id)")
        .await
        .unwrap();
    service
        .exec_query("CREATE TABLE s.Customers(customer_id int, customer_name text)")
        .await
        .unwrap();
    service
        .exec_query("CREATE TABLE s.Products(product_id int, product_name text)")
        .await
        .unwrap();

    let p = service
        .plan_query(
            "SELECT order_id, customer_name, product_name \
                 FROM s.Orders `o`\
                 JOIN s.Customers `c` ON o.customer_id = c.customer_id \
                 JOIN s.Products `p` ON o.product_id = p.product_id",
        )
        .await
        .unwrap();
    assert_eq!(
        pp_phys_plan(p.router.as_ref()),
        "ClusterSend, partitions: [[2, 3, 4]]"
    );
    assert_eq!(
            pp_phys_plan(p.worker.as_ref()),
            "Worker\
           \n  Projection, [order_id, customer_name, product_name]\
           \n    MergeJoin, on: [o.product_id = p.product_id]\
           \n      MergeResort\
           \n        MergeJoin, on: [o.customer_id = c.customer_id]\
           \n          Alias\
           \n            MergeSort\
           \n              Scan, index: by_customer:2:[2]:sort_on[customer_id], fields: [customer_id, order_id, product_id]\
           \n                Empty\
           \n          Alias\
           \n            MergeSort\
           \n              Scan, index: default:3:[3]:sort_on[customer_id], fields: *\
           \n                Empty\
           \n      Alias\
           \n        MergeSort\
           \n          Scan, index: default:4:[4]:sort_on[product_id], fields: *\
           \n            Empty",
        );

    let p = service
        .plan_query(
            "SELECT order_id, customer_name, product_name \
                 FROM s.Orders `o`\
                 JOIN s.Customers `c` ON o.customer_id = c.customer_id \
                 JOIN s.Products `p` ON o.product_id = p.product_id \
                 WHERE p.product_id = 125",
        )
        .await
        .unwrap();

    // Check filter pushdown properly mirrors the filters on joins.
    let mut show_filters = PPOptions::default();
    show_filters.show_filters = true;
    assert_eq!(
            pp_phys_plan_ext(p.worker.as_ref(), &show_filters),
            "Worker\
           \n  Projection, [order_id, customer_name, product_name]\
           \n    MergeJoin, on: [o.product_id = p.product_id]\
           \n      MergeResort\
           \n        MergeJoin, on: [o.customer_id = c.customer_id]\
           \n          Filter, predicate: product_id = 125\
           \n            Alias\
           \n              MergeSort\
           \n                Scan, index: by_customer:2:[2]:sort_on[customer_id], fields: [customer_id, order_id, product_id], predicate: #product_id Eq Int64(125)\
           \n                  Empty\
           \n          Alias\
           \n            MergeSort\
           \n              Scan, index: default:3:[3]:sort_on[customer_id], fields: *\
           \n                Empty\
           \n      Filter, predicate: product_id = 125\
           \n        Alias\
           \n          MergeSort\
           \n            Scan, index: default:4:[4]:sort_on[product_id], fields: *, predicate: #product_id Eq Int64(125)\
           \n              Empty",
        );
}

async fn topk_query(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA s").await.unwrap();
    service
        .exec_query("CREATE TABLE s.Data1(url text, hits int)")
        .await
        .unwrap();
    service
            .exec_query("INSERT INTO s.Data1(url, hits) VALUES ('a', 1), ('b', 2), ('c', 3), ('d', 4), ('e', 5), ('z', 100)")
            .await
            .unwrap();
    service
        .exec_query("CREATE TABLE s.Data2(url text, hits int)")
        .await
        .unwrap();
    service
            .exec_query("INSERT INTO s.Data2(url, hits) VALUES ('b', 50), ('c', 45), ('d', 40), ('e', 35), ('y', 80)")
            .await
            .unwrap();

    // A typical top-k query.
    let r = service
        .exec_query(
            "SELECT `url` `url`, SUM(`hits`) `hits` \
                         FROM (SELECT * FROM s.Data1 \
                               UNION ALL \
                               SELECT * FROM s.Data2) AS `Data` \
                         GROUP BY 1 \
                         ORDER BY 2 DESC \
                         LIMIT 3",
        )
        .await
        .unwrap();
    assert_eq!(to_rows(&r), rows(&[("z", 100), ("y", 80), ("b", 52)]));

    // Same query, ascending order.
    let r = service
        .exec_query(
            "SELECT `url` `url`, SUM(`hits`) `hits` \
                         FROM (SELECT * FROM s.Data1 \
                               UNION ALL \
                               SELECT * FROM s.Data2) AS `Data` \
                         GROUP BY 1 \
                         ORDER BY 2 ASC \
                         LIMIT 3",
        )
        .await
        .unwrap();
    assert_eq!(to_rows(&r), rows(&[("a", 1), ("e", 40), ("d", 44)]));

    // Min, descending.
    let r = service
        .exec_query(
            "SELECT `url` `url`, MIN(`hits`) `hits` \
                         FROM (SELECT * FROM s.Data1 \
                               UNION ALL \
                               SELECT * FROM s.Data2) AS `Data` \
                         GROUP BY 1 \
                         ORDER BY 2 DESC \
                         LIMIT 3",
        )
        .await
        .unwrap();
    assert_eq!(to_rows(&r), rows(&[("z", 100), ("y", 80), ("e", 5)]));

    // Min, ascending.
    let r = service
        .exec_query(
            "SELECT `url` `url`, MIN(`hits`) `hits` \
                         FROM (SELECT * FROM s.Data1 \
                               UNION ALL \
                               SELECT * FROM s.Data2) AS `Data` \
                         GROUP BY 1 \
                         ORDER BY 2 ASC \
                         LIMIT 3",
        )
        .await
        .unwrap();
    assert_eq!(to_rows(&r), rows(&[("a", 1), ("b", 2), ("c", 3)]));

    // Max, descending.
    let r = service
        .exec_query(
            "SELECT `url` `url`, MAX(`hits`) `hits` \
                         FROM (SELECT * FROM s.Data1 \
                               UNION ALL \
                               SELECT * FROM s.Data2) AS `Data` \
                         GROUP BY 1 \
                         ORDER BY 2 DESC \
                         LIMIT 3",
        )
        .await
        .unwrap();
    assert_eq!(to_rows(&r), rows(&[("z", 100), ("y", 80), ("b", 50)]));

    // Max, ascending.
    let r = service
        .exec_query(
            "SELECT `url` `url`, MAX(`hits`) `hits` \
                         FROM (SELECT * FROM s.Data1 \
                               UNION ALL \
                               SELECT * FROM s.Data2) AS `Data` \
                         GROUP BY 1 \
                         ORDER BY 2 ASC \
                         LIMIT 3",
        )
        .await
        .unwrap();
    assert_eq!(to_rows(&r), rows(&[("a", 1), ("e", 35), ("d", 40)]));

    fn rows(a: &[(&str, i64)]) -> Vec<Vec<TableValue>> {
        a.iter()
            .map(|(s, i)| vec![TableValue::String(s.to_string()), TableValue::Int(*i)])
            .collect_vec()
    }
}

async fn topk_decimals(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA s").await.unwrap();
    service
        .exec_query("CREATE TABLE s.Data1(url text, hits decimal)")
        .await
        .unwrap();
    service
        .exec_query("INSERT INTO s.Data1(url, hits) VALUES ('a', NULL), ('b', 2), ('c', 3), ('d', 4), ('e', 5), ('z', 100)")
        .await
        .unwrap();
    service
        .exec_query("CREATE TABLE s.Data2(url text, hits decimal)")
        .await
        .unwrap();
    service
        .exec_query("INSERT INTO s.Data2(url, hits) VALUES ('b', 50), ('c', 45), ('d', 40), ('e', 35), ('y', 80), ('z', NULL)")
        .await
        .unwrap();

    // A typical top-k query.
    let r = service
        .exec_query(
            "SELECT `url` `url`, SUM(`hits`) `hits` \
                         FROM (SELECT * FROM s.Data1 \
                               UNION ALL \
                               SELECT * FROM s.Data2) AS `Data` \
                         GROUP BY 1 \
                         ORDER BY 2 DESC NULLS LAST \
                         LIMIT 3",
        )
        .await
        .unwrap();
    assert_eq!(to_rows(&r), rows(&[("z", 100), ("y", 80), ("b", 52)]));

    fn rows(a: &[(&str, i64)]) -> Vec<Vec<TableValue>> {
        a.iter()
            .map(|(s, i)| {
                vec![
                    TableValue::String(s.to_string()),
                    TableValue::Decimal(Decimal::new(*i * 100_000)),
                ]
            })
            .collect_vec()
    }
}

async fn offset(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA s").await.unwrap();
    service
        .exec_query("CREATE TABLE s.Data1(t text)")
        .await
        .unwrap();
    service
        .exec_query("INSERT INTO s.Data1(t) VALUES ('a'), ('b'), ('c'), ('z')")
        .await
        .unwrap();
    service
        .exec_query("CREATE TABLE s.Data2(t text)")
        .await
        .unwrap();
    service
        .exec_query("INSERT INTO s.Data2(t) VALUES ('f'), ('g'), ('h')")
        .await
        .unwrap();

    let r = service
        .exec_query(
            "SELECT t FROM (SELECT * FROM s.Data1 UNION ALL SELECT * FROM s.Data2)\
             ORDER BY 1",
        )
        .await
        .unwrap();
    assert_eq!(to_rows(&r), rows(&["a", "b", "c", "f", "g", "h", "z"]));
    let r = service
        .exec_query(
            "SELECT t FROM (SELECT * FROM s.Data1 UNION ALL SELECT * FROM s.Data2)\
             ORDER BY 1 \
             LIMIT 3 \
             OFFSET 2",
        )
        .await
        .unwrap();
    assert_eq!(to_rows(&r), rows(&["c", "f", "g"]));

    let r = service
        .exec_query(
            "SELECT t FROM (SELECT * FROM s.Data1 UNION ALL SELECT * FROM s.Data2)\
             ORDER BY 1 DESC \
             LIMIT 3 \
             OFFSET 1",
        )
        .await
        .unwrap();
    assert_eq!(to_rows(&r), rows(&["h", "g", "f"]));

    fn rows(a: &[&str]) -> Vec<Vec<TableValue>> {
        a.iter()
            .map(|s| vec![TableValue::String(s.to_string())])
            .collect_vec()
    }
}

async fn having(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA s").await.unwrap();
    service
        .exec_query("CREATE TABLE s.Data1(id text, n int)")
        .await
        .unwrap();
    service
        .exec_query("INSERT INTO s.Data1(id, n) VALUES ('a', 1), ('b', 2), ('c', 3)")
        .await
        .unwrap();
    service
        .exec_query("CREATE TABLE s.Data2(id text, n int)")
        .await
        .unwrap();
    service
        .exec_query("INSERT INTO s.Data2(id, n) VALUES ('a', 4), ('b', 5), ('c', 6)")
        .await
        .unwrap();

    let r = service
        .exec_query(
            "SELECT id, count(n) FROM s.Data1 \
             WHERE id != 'c' \
             GROUP BY 1 \
             HAVING 2 <= sum(n)",
        )
        .await
        .unwrap();
    assert_eq!(to_rows(&r), rows(&[("b", 1)]));

    let r = service
        .exec_query(
            "SELECT `data`.id, count(`data`.n) \
             FROM (SELECT * FROM s.Data1 UNION ALL SELECT * FROM s.Data2) `data` \
             WHERE n != 2 \
             GROUP BY 1 \
             HAVING sum(n) <= 5 \
             ORDER BY 1",
        )
        .await
        .unwrap();
    assert_eq!(to_rows(&r), rows(&[("a", 2), ("b", 1)]));

    // We diverge from datafusion here, which resolve `n` in the HAVING to `sum(n)` and fail.
    // At the moment CubeJS sends requests like this, though, so we choose to remove support for
    // filtering on aliases in the same query.
    let r = service
        .exec_query(
            "SELECT `data`.id, sum(n) AS n \
             FROM (SELECT * FROM s.Data1 UNION ALL SELECT * FROM s.Data2) `data` \
             GROUP BY 1 \
             HAVING sum(n) > 5 \
             ORDER BY 1",
        )
        .await
        .unwrap();
    assert_eq!(to_rows(&r), rows(&[("b", 7), ("c", 9)]));
    // Since we do not resolve aliases, this will fail.
    let err = service
        .exec_query(
            "SELECT `data`.id, sum(n) AS n \
             FROM (SELECT * FROM s.Data1 UNION ALL SELECT * FROM s.Data2) `data` \
             GROUP BY 1 \
             HAVING n = 2 \
             ORDER BY 1",
        )
        .await;
    assert!(err.is_err());

    fn rows(a: &[(&str, i64)]) -> Vec<Vec<TableValue>> {
        a.iter()
            .map(|(s, n)| vec![TableValue::String(s.to_string()), TableValue::Int(*n)])
            .collect_vec()
    }
}

async fn rolling_window_join(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA s").await.unwrap();
    service
        .exec_query("CREATE TABLE s.Data(day timestamp, name text, n int)")
        .await
        .unwrap();
    let raw_query = "SELECT Series.date_to, Table.name, sum(Table.n) as n FROM (\
               SELECT to_timestamp('2020-01-01T00:00:00.000') date_from, \
                      to_timestamp('2020-01-01T23:59:59.999') date_to \
               UNION ALL \
               SELECT to_timestamp('2020-01-02T00:00:00.000') date_from, \
                      to_timestamp('2020-01-02T23:59:59.999') date_to \
               UNION ALL \
               SELECT to_timestamp('2020-01-03T00:00:00.000') date_from, \
                      to_timestamp('2020-01-03T23:59:59.999') date_to \
               UNION ALL \
               SELECT to_timestamp('2020-01-04T00:00:00.000') date_from, \
                      to_timestamp('2020-01-04T23:59:59.999') date_to\
            ) AS `Series` \
            LEFT JOIN (\
               SELECT date_trunc('day', CONVERT_TZ(day,'+00:00')) `day`, name, sum(n) `n` \
               FROM s.Data \
               GROUP BY 1, 2 \
            ) AS `Table` ON `Table`.day <= `Series`.date_to \
            GROUP BY 1, 2";
    let query = raw_query.to_string() + " ORDER BY 1, 2, 3";
    let query_sort_subquery = format!(
        "SELECT q0.date_to, q0.name, q0.n FROM ({}) as q0 ORDER BY 1,2,3",
        raw_query
    );

    let plan = service.plan_query(&query).await.unwrap().worker;
    assert_eq!(
        pp_phys_plan(plan.as_ref()),
        "Sort\
      \n  Projection, [date_to, name, SUM(n):n]\
      \n    CrossJoinAgg, on: day <= date_to\
      \n      Alias\
      \n        Projection, [day, name, SUM(n):n]\
      \n          FinalHashAggregate\
      \n            Worker\
      \n              PartialHashAggregate\
      \n                Merge\
      \n                  Scan, index: default:1:[1], fields: *\
      \n                    Empty"
    );

    let plan = service
        .plan_query(&query_sort_subquery)
        .await
        .unwrap()
        .worker;
    assert_eq!(
        pp_phys_plan(plan.as_ref()),
        "Sort\
        \n  Projection, [date_to, name, n]\
        \n    Alias\
        \n      Projection, [date_to, name, SUM(n):n]\
        \n        CrossJoinAgg, on: day <= date_to\
        \n          Alias\
        \n            Projection, [day, name, SUM(n):n]\
        \n              FinalHashAggregate\
        \n                Worker\
        \n                  PartialHashAggregate\
        \n                    Merge\
        \n                      Scan, index: default:1:[1], fields: *\
        \n                        Empty"
    );

    service
        .exec_query("INSERT INTO s.Data(day, name, n) VALUES ('2020-01-01T01:00:00.000', 'john', 10), \
                                                             ('2020-01-01T01:00:00.000', 'sara', 7), \
                                                             ('2020-01-03T02:00:00.000', 'sara', 3), \
                                                             ('2020-01-03T03:00:00.000', 'john', 9), \
                                                             ('2020-01-03T03:00:00.000', 'john', 11), \
                                                             ('2020-01-04T05:00:00.000', 'timmy', 5)")
        .await
        .unwrap();

    let mut jan = (1..=4)
        .map(|d| timestamp_from_string(&format!("2020-01-{:02}T23:59:59.999", d)).unwrap())
        .collect_vec();
    jan.insert(0, jan[1]); // jan[i] will correspond to i-th day of the month.

    for q in &[query.as_str(), query_sort_subquery.as_str()] {
        log::info!("Testing query {}", q);
        let r = service.exec_query(q).await.unwrap();
        assert_eq!(
            to_rows(&r),
            rows(&[
                (jan[1], "john", 10),
                (jan[1], "sara", 7),
                (jan[2], "john", 10),
                (jan[2], "sara", 7),
                (jan[3], "john", 30),
                (jan[3], "sara", 10),
                (jan[4], "john", 30),
                (jan[4], "sara", 10),
                (jan[4], "timmy", 5)
            ])
        );
    }

    fn rows(a: &[(TimestampValue, &str, i64)]) -> Vec<Vec<TableValue>> {
        a.iter()
            .map(|(t, s, n)| {
                vec![
                    TableValue::Timestamp(*t),
                    TableValue::String(s.to_string()),
                    TableValue::Int(*n),
                ]
            })
            .collect_vec()
    }
}

async fn decimal_index(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA s").await.unwrap();
    service
        .exec_query("CREATE TABLE s.Data(x decimal, y decimal)")
        .await
        .unwrap();
    service
        .exec_query("CREATE INDEX reverse on s.Data(y, x)")
        .await
        .unwrap();
    service
        .exec_query("INSERT INTO s.Data(x,y) VALUES (1, 2), (2, 3), (3, 4)")
        .await
        .unwrap();

    let r = service
        .exec_query("SELECT * FROM s.Data ORDER BY x")
        .await
        .unwrap();
    assert_eq!(to_rows(&r), rows(&[(1, 2), (2, 3), (3, 4)]));

    let r = service
        .exec_query("SELECT * FROM s.Data ORDER BY y DESC")
        .await
        .unwrap();
    assert_eq!(to_rows(&r), rows(&[(3, 4), (2, 3), (1, 2)]));

    fn rows(a: &[(i64, i64)]) -> Vec<Vec<TableValue>> {
        a.iter()
            .map(|(x, y)| {
                vec![
                    TableValue::Decimal(Decimal::new(x * 100000)),
                    TableValue::Decimal(Decimal::new(y * 100000)),
                ]
            })
            .collect_vec()
    }
}

async fn float_index(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA s").await.unwrap();
    service
        .exec_query("CREATE TABLE s.Data(x float, y float)")
        .await
        .unwrap();
    service
        .exec_query("CREATE INDEX reverse on s.Data(y, x)")
        .await
        .unwrap();
    service
        .exec_query("INSERT INTO s.Data(x,y) VALUES (1, 2), (2, 3), (3, 4)")
        .await
        .unwrap();

    let r = service
        .exec_query("SELECT * FROM s.Data ORDER BY x")
        .await
        .unwrap();
    assert_eq!(to_rows(&r), rows(&[(1., 2.), (2., 3.), (3., 4.)]));

    let r = service
        .exec_query("SELECT * FROM s.Data ORDER BY y DESC")
        .await
        .unwrap();
    assert_eq!(to_rows(&r), rows(&[(3., 4.), (2., 3.), (1., 2.)]));

    fn rows(a: &[(f64, f64)]) -> Vec<Vec<TableValue>> {
        a.iter()
            .map(|(x, y)| {
                vec![
                    TableValue::Float((*x).into()),
                    TableValue::Float((*y).into()),
                ]
            })
            .collect_vec()
    }
}

async fn date_add(service: Box<dyn SqlClient>) {
    let r = service
        .exec_query(
            "SELECT
            DATE_ADD(CAST('2021-01-01T00:00:00Z' as TIMESTAMP), INTERVAL '1 second'),\
            DATE_ADD(CAST('2021-01-01T00:00:00Z' as TIMESTAMP), INTERVAL '1 minute'),\
            DATE_ADD(CAST('2021-01-01T00:00:00Z' as TIMESTAMP), INTERVAL '1 hour'),\
            DATE_ADD(CAST('2021-01-01T00:00:00Z' as TIMESTAMP), INTERVAL '1 day'),\
            DATE_ADD(CAST('2021-01-01T00:00:00Z' as TIMESTAMP), INTERVAL '1 day 1 hour 1 minute 1 second'),\
            DATE_ADD(CAST('2021-01-01T00:00:00Z' as TIMESTAMP), INTERVAL '1 month'),\
            DATE_ADD(CAST('2021-01-01T00:00:00Z' as TIMESTAMP), INTERVAL '1 year'),\
            DATE_ADD(CAST('2021-01-01T00:00:00Z' as TIMESTAMP), INTERVAL '13 month'),\
            DATE_ADD(CAST('2021-01-01T23:59:00Z' as TIMESTAMP), INTERVAL '1 minute'),\
            DATE_ADD(CAST('2021-12-01T00:00:00Z' as TIMESTAMP), INTERVAL '1 month'),\
            DATE_ADD(CAST('2021-12-31T00:00:00Z' as TIMESTAMP), INTERVAL '1 day'),\
            DATE_ADD(CAST('2020-02-29T00:00:00Z' as TIMESTAMP), INTERVAL '1 day'),\
            DATE_ADD(CAST('2020-02-28T00:00:00Z' as TIMESTAMP), INTERVAL '1 day'),\
            DATE_ADD(CAST('2021-02-28T00:00:00Z' as TIMESTAMP), INTERVAL '1 day'),\
            DATE_ADD(CAST('2020-02-29T00:00:00Z' as TIMESTAMP), INTERVAL '1 year'),\
            DATE_ADD(CAST('2021-01-30T00:00:00Z' as TIMESTAMP), INTERVAL '1 month'),\
            DATE_ADD(CAST('2020-01-29T00:00:00Z' as TIMESTAMP), INTERVAL '1 month'),\
            DATE_ADD(CAST('2021-01-29T00:00:00Z' as TIMESTAMP), INTERVAL '1 month')\
        ",
        )
        .await
        .unwrap();

    assert_eq!(
        to_rows(&r),
        vec![vec![
            // Simple tests for IntervalDayTime
            TableValue::Timestamp(timestamp_from_string("2021-01-01T00:00:01Z").unwrap()),
            TableValue::Timestamp(timestamp_from_string("2021-01-01T00:01:00Z").unwrap()),
            TableValue::Timestamp(timestamp_from_string("2021-01-01T01:00:00Z").unwrap()),
            TableValue::Timestamp(timestamp_from_string("2021-01-02T00:00:00Z").unwrap()),
            TableValue::Timestamp(timestamp_from_string("2021-01-02T01:01:01Z").unwrap()),
            // Simple tests for IntervalYearMonth
            TableValue::Timestamp(timestamp_from_string("2021-02-01T00:00:00Z").unwrap()),
            TableValue::Timestamp(timestamp_from_string("2022-01-01T00:00:00Z").unwrap()),
            TableValue::Timestamp(timestamp_from_string("2022-02-01T00:00:00Z").unwrap()),
            // Calculation logic
            TableValue::Timestamp(timestamp_from_string("2021-01-02T00:00:00Z").unwrap()),
            TableValue::Timestamp(timestamp_from_string("2022-01-01T00:00:00Z").unwrap()),
            TableValue::Timestamp(timestamp_from_string("2022-01-01T00:00:00Z").unwrap()),
            TableValue::Timestamp(timestamp_from_string("2020-03-01T00:00:00Z").unwrap()),
            TableValue::Timestamp(timestamp_from_string("2020-02-29T00:00:00Z").unwrap()),
            TableValue::Timestamp(timestamp_from_string("2021-03-01T00:00:00Z").unwrap()),
            TableValue::Timestamp(timestamp_from_string("2021-02-28T00:00:00Z").unwrap()),
            TableValue::Timestamp(timestamp_from_string("2021-02-28T00:00:00Z").unwrap()),
            TableValue::Timestamp(timestamp_from_string("2020-02-29T00:00:00Z").unwrap()),
            TableValue::Timestamp(timestamp_from_string("2021-02-28T00:00:00Z").unwrap()),
        ],]
    );
}

async fn now(service: Box<dyn SqlClient>) {
    let r = service.exec_query("SELECT now()").await.unwrap();
    assert_eq!(r.get_rows().len(), 1);
    assert_eq!(r.get_rows()[0].values().len(), 1);
    match &r.get_rows()[0].values()[0] {
        TableValue::Timestamp(_) => {} // all ok.
        v => panic!("not a timestamp: {:?}", v),
    }

    service.exec_query("CREATE SCHEMA s").await.unwrap();
    service
        .exec_query("CREATE TABLE s.Data(i int)")
        .await
        .unwrap();
    service
        .exec_query("INSERT INTO s.Data(i) VALUES (1), (2), (3)")
        .await
        .unwrap();

    let r = service
        .exec_query("SELECT i, now() FROM s.Data")
        .await
        .unwrap();
    assert_eq!(r.len(), 3);
    let mut seen = None;
    for r in r.get_rows() {
        match &r.values()[1] {
            TableValue::Timestamp(v) => match &seen {
                None => seen = Some(v),
                Some(seen) => assert_eq!(seen, &v),
            },
            v => panic!("not a timestamp: {:?}", v),
        }
    }

    let r = service
        .exec_query("SELECT i, now() FROM s.Data WHERE now() = now()")
        .await
        .unwrap();
    assert_eq!(r.len(), 3);

    let r = service
        .exec_query("SELECT now(), unix_timestamp()")
        .await
        .unwrap();
    match r.get_rows()[0].values().as_slice() {
        &[TableValue::Timestamp(v), TableValue::Int(t)] => {
            assert_eq!(v.get_time_stamp() / 1_000_000_000, t)
        }
        _ => panic!("unexpected values: {:?}", r.get_rows()[0]),
    }
}

async fn dump(service: Box<dyn SqlClient>) {
    let r = service.exec_query("DUMP SELECT 1").await.unwrap();
    let dump_dir = match &r.get_rows()[0].values()[0] {
        TableValue::String(d) => d,
        _ => panic!("invalid result"),
    };

    assert!(tokio::fs::metadata(dump_dir).await.unwrap().is_dir());
    assert!(
        tokio::fs::metadata(Path::new(dump_dir).join("metastore-backup"))
            .await
            .unwrap()
            .is_dir()
    );
}

fn to_rows(d: &DataFrame) -> Vec<Vec<TableValue>> {
    return d
        .get_rows()
        .iter()
        .map(|r| r.values().clone())
        .collect_vec();
}
