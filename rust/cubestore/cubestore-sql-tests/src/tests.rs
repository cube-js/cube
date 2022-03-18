use crate::files::write_tmp_file;
use crate::rows::{rows, NULL};
use crate::SqlClient;
use async_compression::tokio::write::GzipEncoder;
use cubestore::queryplanner::pretty_printers::{pp_phys_plan, pp_phys_plan_ext, PPOptions};
use cubestore::queryplanner::MIN_TOPK_STREAM_ROWS;
use cubestore::sql::timestamp_from_string;
use cubestore::store::DataFrame;
use cubestore::table::{Row, TableValue, TimestampValue};
use cubestore::util::decimal::Decimal;
use cubestore::CubeError;
use indoc::indoc;
use itertools::Itertools;
use pretty_assertions::assert_eq;
use std::env;
use std::fs::File;
use std::future::Future;
use std::io::Write;
use std::panic::RefUnwindSafe;
use std::path::Path;
use std::pin::Pin;
use std::time::Duration;
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
        t("negative_decimal", negative_decimal),
        t("custom_types", custom_types),
        t("group_by_boolean", group_by_boolean),
        t("group_by_decimal", group_by_decimal),
        t("group_by_nulls", group_by_nulls),
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
        t("in_list_with_union", in_list_with_union),
        t("numeric_cast", numeric_cast),
        t("numbers_to_bool", numbers_to_bool),
        t("union", union),
        t("timestamp_select", timestamp_select),
        t("timestamp_seconds_frac", timestamp_seconds_frac),
        t("column_escaping", column_escaping),
        t("information_schema", information_schema),
        t("case_column_escaping", case_column_escaping),
        t("inner_column_escaping", inner_column_escaping),
        t("convert_tz", convert_tz),
        t("date_trunc", date_trunc),
        t("coalesce", coalesce),
        t("ilike", ilike),
        t("count_distinct_crash", count_distinct_crash),
        t(
            "count_distinct_group_by_crash",
            count_distinct_group_by_crash,
        ),
        t("count_distinct_take_crash", count_distinct_take_crash),
        t("create_schema_if_not_exists", create_schema_if_not_exists),
        t(
            "create_index_before_ingestion",
            create_index_before_ingestion,
        ),
        t("ambiguous_join_sort", ambiguous_join_sort),
        t("join_with_aliases", join_with_aliases),
        t("group_by_without_aggregates", group_by_without_aggregates),
        t("create_table_with_location", create_table_with_location),
        t(
            "create_table_with_location_messed_order",
            create_table_with_location_messed_order,
        ),
        t(
            "create_table_with_location_invalid_digit",
            create_table_with_location_invalid_digit,
        ),
        t("create_table_with_csv", create_table_with_csv),
        t(
            "create_table_with_csv_and_index",
            create_table_with_csv_and_index,
        ),
        t(
            "create_table_with_csv_no_header",
            create_table_with_csv_no_header,
        ),
        t("create_table_with_url", create_table_with_url),
        t("create_table_fail_and_retry", create_table_fail_and_retry),
        t("empty_crash", empty_crash),
        t("bytes", bytes),
        t("hyperloglog", hyperloglog),
        t("hyperloglog_empty_inputs", hyperloglog_empty_inputs),
        t("hyperloglog_empty_group_by", hyperloglog_empty_group_by),
        t("hyperloglog_inserts", hyperloglog_inserts),
        t("hyperloglog_inplace_group_by", hyperloglog_inplace_group_by),
        t("hyperloglog_postgres", hyperloglog_postgres),
        t("hyperloglog_snowflake", hyperloglog_snowflake),
        t("planning_inplace_aggregate", planning_inplace_aggregate),
        t("planning_hints", planning_hints),
        t("planning_inplace_aggregate2", planning_inplace_aggregate2),
        t("topk_large_inputs", topk_large_inputs),
        t("partitioned_index", partitioned_index),
        t(
            "partitioned_index_if_not_exists",
            partitioned_index_if_not_exists,
        ),
        t("drop_partitioned_index", drop_partitioned_index),
        t("planning_simple", planning_simple),
        t("planning_joins", planning_joins),
        t("planning_3_table_joins", planning_3_table_joins),
        t(
            "planning_join_with_partitioned_index",
            planning_join_with_partitioned_index,
        ),
        t("topk_query", topk_query),
        t("topk_decimals", topk_decimals),
        t("offset", offset),
        t("having", having),
        t("rolling_window_join", rolling_window_join),
        t("rolling_window_query", rolling_window_query),
        t("rolling_window_exprs", rolling_window_exprs),
        t(
            "rolling_window_query_timestamps",
            rolling_window_query_timestamps,
        ),
        t(
            "rolling_window_extra_aggregate",
            rolling_window_extra_aggregate,
        ),
        t(
            "rolling_window_extra_aggregate_timestamps",
            rolling_window_extra_aggregate_timestamps,
        ),
        t(
            "rolling_window_one_week_interval",
            rolling_window_one_week_interval,
        ),
        t("rolling_window_offsets", rolling_window_offsets),
        t("decimal_index", decimal_index),
        t("decimal_order", decimal_order),
        t("float_index", float_index),
        t("float_order", float_order),
        t("date_add", date_add),
        t("now", now),
        t("dump", dump),
        t("unsorted_merge_assertion", unsorted_merge_assertion),
        t("unsorted_data_timestamps", unsorted_data_timestamps),
        // t("ksql_simple", ksql_simple),
        t(
            "dimension_only_queries_for_stream_table",
            dimension_only_queries_for_stream_table,
        ),
        t(
            "unique_key_and_multi_measures_for_stream_table",
            unique_key_and_multi_measures_for_stream_table,
        ),
        t("divide_by_zero", divide_by_zero),
        t("panic_worker", panic_worker),
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

async fn negative_decimal(service: Box<dyn SqlClient>) {
    let _ = service.exec_query("CREATE SCHEMA foo").await.unwrap();

    let _ = service
        .exec_query("CREATE TABLE foo.values (decimal_value decimal)")
        .await
        .unwrap();

    service
        .exec_query("INSERT INTO foo.values (decimal_value) VALUES (-0.12345)")
        .await
        .unwrap();

    let result = service
        .exec_query("SELECT * from foo.values")
        .await
        .unwrap();

    assert_eq!(
        match &result.get_rows()[0].values()[0] {
            TableValue::Decimal(d) => d.to_string(5),
            x => panic!("Expected decimal but found: {:?}", x),
        },
        "-0.12345"
    );
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

async fn group_by_nulls(service: Box<dyn SqlClient>) {
    let _ = service.exec_query("CREATE SCHEMA s").await.unwrap();

    let _ = service
        .exec_query("CREATE TABLE s.data (id int, n int)")
        .await
        .unwrap();

    service
        .exec_query(
            "INSERT INTO s.data (id, n) VALUES (NULL, 1), (NULL, 2), (NULL, 3), (1, 1), (2, 2)",
        )
        .await
        .unwrap();

    let result = service
        .exec_query("SELECT id, sum(n) from s.data group by 1 order by 1")
        .await
        .unwrap();
    assert_eq!(
        to_rows(&result),
        rows(&[(Some(1), 1), (Some(2), 2), (None, 6)])
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
        to_rows(&result),
        vec![
            vec![
                TableValue::String("San Francisco".to_string()),
                TableValue::Int(10)
            ],
            vec![
                TableValue::String("New York".to_string()),
                TableValue::Int(5)
            ]
        ]
    );

    // Same query, reverse comparison order.
    let result2 = service.exec_query("SELECT c.city, sum(o.amount) from foo.orders o JOIN foo.customers c ON c.id = o.customer_id GROUP BY 1 ORDER BY 2 DESC").await.unwrap();
    assert_eq!(result.get_rows(), result2.get_rows());

    // Join on non-existing field.
    assert!(service.exec_query("SELECT c.id, sum(o.amount) FROM foo.customers c JOIN foo.orders o ON c.id = o.not_found")
        .await.is_err());
    assert!(service.exec_query("SELECT c.id, sum(o.amount) FROM foo.customers c JOIN foo.orders o ON o.not_found = c.id")
        .await.is_err());

    // Join on ambiguous fields.
    let result = service
        .exec_query(
            "SELECT c.id, k.id FROM foo.customers c JOIN foo.customers k ON id = id ORDER BY 1",
        )
        .await
        .unwrap();
    assert_eq!(to_rows(&result), rows(&[("a", "a"), ("b", "b")]));
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

async fn in_list_with_union(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA foo").await.unwrap();

    service
        .exec_query("CREATE TABLE foo.customers_1 (id text, city text, state text)")
        .await
        .unwrap();

    service
        .exec_query("CREATE TABLE foo.customers_2 (id text, city text, state text)")
        .await
        .unwrap();

    service.exec_query(
        "INSERT INTO foo.customers_1 (id, city, state) VALUES ('a1', 'San Francisco', 'CA'), ('b1', 'New York', 'NY'), ('c1', 'San Diego', 'CA'), ('d1', 'Austin', 'TX')"
    ).await.unwrap();

    service.exec_query(
        "INSERT INTO foo.customers_2 (id, city, state) VALUES ('a2', 'San Francisco', 'CA'), ('b2', 'New York', 'NY'), ('c2', 'San Diego', 'CA'), ('d2', 'Austin', 'TX')"
    ).await.unwrap();

    let result = service
        .exec_query("SELECT count(*) from (SELECT * FROM foo.customers_1 UNION ALL SELECT * FROM foo.customers_2) AS `customers` WHERE state in ('CA', 'TX')")
        .await
        .unwrap();

    assert_eq!(result.get_rows()[0], Row::new(vec![TableValue::Int(6)]));
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

async fn numbers_to_bool(service: Box<dyn SqlClient>) {
    let r = service
        .exec_query("SELECT 1 = TRUE, FALSE = 0, -1 = TRUE, 123 = TRUE")
        .await
        .unwrap();
    assert_eq!(to_rows(&r), rows(&[(true, true, true, true)]));

    service.exec_query("CREATE SCHEMA s").await.unwrap();
    service
        .exec_query("CREATE TABLE s.bools (b boolean, i int)")
        .await
        .unwrap();
    service
        .exec_query(
            "INSERT INTO s.bools(b, i) VALUES (true, 0), (false, 0), (true, 123), (false, 123)",
        )
        .await
        .unwrap();

    // Compare array with constant.
    let r = service
        .exec_query("SELECT b, b = 1, b = 0, b = 123 FROM s.bools GROUP BY 1, 2, 3 ORDER BY 1")
        .await
        .unwrap();
    assert_eq!(
        to_rows(&r),
        rows(&[(false, false, true, false), (true, true, false, true)])
    );

    // Compare array with array.
    let r = service
        .exec_query("SELECT b, i, b = i FROM s.bools ORDER BY 1, 2")
        .await
        .unwrap();
    assert_eq!(
        to_rows(&r),
        rows(&[
            (false, 0, true),
            (false, 123, false),
            (true, 0, false),
            (true, 123, true)
        ])
    );

    // Other types work fine.
    let r = service
        .exec_query("SELECT 1 = 1, '1' = 1, 'foo' = 'foo'")
        .await
        .unwrap();
    assert_eq!(to_rows(&r), rows(&[(true, true, true)]))
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

async fn timestamp_seconds_frac(service: Box<dyn SqlClient>) {
    for s in &[
        "1970-01-01T00:00:00.123Z",
        "1970-01-01T00:00:00.123",
        "1970-01-01 00:00:00.123Z",
        "1970-01-01 00:00:00.123 UTC",
    ] {
        assert_eq!(
            timestamp_from_string(s).expect(s).get_time_stamp(),
            123000000,
            "input {}",
            s
        );
        if s.ends_with("UTC") {
            // Currently accepted only on ingestion.
            continue;
        }
        let r = service
            .exec_query(&format!("SELECT to_timestamp('{}')", s))
            .await
            .unwrap();
        assert_eq!(to_rows(&r), rows(&[TimestampValue::new(123000000)]));
    }
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

async fn date_trunc(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA foo").await.unwrap();

    service
        .exec_query("CREATE TABLE foo.timestamps (t timestamp)")
        .await
        .unwrap();

    service
        .exec_query(
            "INSERT INTO foo.timestamps (t) VALUES \
            ('2020-01-01T00:00:00.000Z'), \
            ('2020-03-01T00:00:00.000Z'), \
            ('2020-04-01T00:00:00.000Z'), \
            ('2020-07-01T00:00:00.000Z'), \
            ('2020-09-01T00:00:00.000Z')",
        )
        .await
        .unwrap();

    let result = service
        .exec_query(
            "SELECT date_trunc('quarter', `t`) `quarter` \
            FROM foo.timestamps `timestamp`",
        )
        .await
        .unwrap();

    assert_eq!(
        result.get_rows(),
        &vec![
            Row::new(vec![TableValue::Timestamp(TimestampValue::new(
                1577836800000000000
            )),]),
            Row::new(vec![TableValue::Timestamp(TimestampValue::new(
                1577836800000000000
            )),]),
            Row::new(vec![TableValue::Timestamp(TimestampValue::new(
                1585699200000000000
            )),]),
            Row::new(vec![TableValue::Timestamp(TimestampValue::new(
                1593561600000000000
            )),]),
            Row::new(vec![TableValue::Timestamp(TimestampValue::new(
                1593561600000000000
            )),])
        ]
    );
}

async fn ilike(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA s").await.unwrap();
    service
        .exec_query("CREATE TABLE s.strings(t text, pat text)")
        .await
        .unwrap();
    service
        .exec_query(
            "INSERT INTO s.strings(t, pat) \
             VALUES ('aba', '%ABA'), ('ABa', '%aba%'), ('CABA', 'aba%'), ('ZABA', '%a%b%a%'), ('ZZZ', 'zzz'), ('TTT', 'TTT'),\
             ('some_underscore', '%some\\\\_underscore%')",
        )
        .await
        .unwrap();
    let r = service
        .exec_query("SELECT t FROM s.strings WHERE t ILIKE '%aBA%' ORDER BY t")
        .await
        .unwrap();
    assert_eq!(to_rows(&r), rows(&["ABa", "CABA", "ZABA", "aba"]));

    let r = service
        .exec_query("SELECT t FROM s.strings WHERE t ILIKE 'aBA%' ORDER BY t")
        .await
        .unwrap();
    assert_eq!(to_rows(&r), rows(&["ABa", "aba"]));

    let r = service
        .exec_query("SELECT t FROM s.strings WHERE t ILIKE '%aBA' ORDER BY t")
        .await
        .unwrap();
    assert_eq!(to_rows(&r), rows(&["ABa", "CABA", "ZABA", "aba"]));

    let r = service
        .exec_query("SELECT t FROM s.strings WHERE t ILIKE 'aBA' ORDER BY t")
        .await
        .unwrap();
    assert_eq!(to_rows(&r), rows(&["ABa", "aba"]));

    let r = service
        .exec_query(
            "SELECT t FROM s.strings WHERE t ILIKE CONCAT('%', 'some\\\\_underscore', '%') ORDER BY t",
        )
        .await
        .unwrap();
    assert_eq!(to_rows(&r), rows(&["some_underscore"]));

    // Compare constant string with a bunch of patterns.
    // Inputs are: ('aba', '%ABA'), ('ABa', '%aba%'), ('CABA', 'aba%'), ('ZABA', '%a%b%a%'),
    //             ('ZZZ', 'zzz'), ('TTT', 'TTT').
    let r = service
        .exec_query("SELECT pat FROM s.strings WHERE 'aba' ILIKE pat ORDER BY pat")
        .await
        .unwrap();
    assert_eq!(to_rows(&r), rows(&["%ABA", "%a%b%a%", "%aba%", "aba%"]));

    // Compare array against array.
    let r = service
        .exec_query("SELECT t, pat FROM s.strings WHERE t ILIKE pat ORDER BY t")
        .await
        .unwrap();
    assert_eq!(
        to_rows(&r),
        rows(&[
            ("ABa", "%aba%"),
            ("TTT", "TTT"),
            ("ZABA", "%a%b%a%"),
            ("ZZZ", "zzz"),
            ("aba", "%ABA"),
            ("some_underscore", "%some\\_underscore%"),
        ])
    );

    // Check NOT ILIKE also works.
    let r = service
        .exec_query("SELECT t, pat FROM s.strings WHERE t NOT ILIKE pat ORDER BY t")
        .await
        .unwrap();
    assert_eq!(to_rows(&r), rows(&[("CABA", "aba%")]));
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
            vec![TableValue::Int(1)],
            vec![TableValue::Int(3)],
            vec![TableValue::Null],
            vec![TableValue::Null],
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
            vec![TableValue::String("1".to_string())],
            vec![TableValue::String("3".to_string())],
            vec![TableValue::String("baz".to_string())],
            vec![TableValue::Null],
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

async fn count_distinct_group_by_crash(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA s").await.unwrap();
    service
        .exec_query("CREATE TABLE s.Data (n string)")
        .await
        .unwrap();
    service
        .exec_query("INSERT INTO s.Data (n) VALUES ('a'), ('b'), ('c'), ('b'), ('c')")
        .await
        .unwrap();

    let r = service
        .exec_query(
            "SELECT n, COUNT(DISTINCT CASE WHEN n <> 'a' THEN n END), COUNT(*) \
             FROM s.Data \
             GROUP BY 1 \
             ORDER BY 1",
        )
        .await
        .unwrap();
    assert_eq!(
        to_rows(&r),
        vec![
            vec![
                TableValue::String("a".to_string()),
                TableValue::Int(0),
                TableValue::Int(1)
            ],
            vec![
                TableValue::String("b".to_string()),
                TableValue::Int(1),
                TableValue::Int(2)
            ],
            vec![
                TableValue::String("c".to_string()),
                TableValue::Int(1),
                TableValue::Int(2)
            ],
        ]
    );
}

async fn count_distinct_take_crash(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA s").await.unwrap();
    service
        .exec_query("CREATE TABLE s.data(id int, n int)")
        .await
        .unwrap();
    service
        .exec_query("INSERT INTO s.data(id, n) VALUES (1, 1)")
        .await
        .unwrap();
    // This used to crash because `take` on empty list returned null. The implementation can easily
    // change with time, though, so test is not robust.
    let r = service
        .exec_query("SELECT n, COUNT(DISTINCT CASE WHEN id = 2 THEN 2 END) FROM s.data GROUP BY n")
        .await
        .unwrap();
    assert_eq!(to_rows(&r), rows(&[(1, 0)]));
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
        file.write_all("6,New York,\"\",\"\\N\"\n".as_bytes())
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
    assert_eq!(result.get_rows(), &vec![Row::new(vec![TableValue::Int(9)])]);

    let result = service.exec_query("SELECT count(*) as cnt from Foo.Persons WHERE arr = '[\"Foo\",\"Bar\",\"FooBar\"]' or arr = '[\"\"]' or arr is null").await.unwrap();
    assert_eq!(result.get_rows(), &vec![Row::new(vec![TableValue::Int(7)])]);
}

async fn create_table_with_location_messed_order(service: Box<dyn SqlClient>) {
    let paths = {
        let dir = env::temp_dir();

        let path_1 = dir.clone().join("messed-order.csv");
        let mut file = File::create(path_1.clone()).unwrap();

        file.write_all("c6,c11,c10,c5,c9,c4,c2,c8,c1,c3,c7,c12\n".as_bytes())
            .unwrap();
        file.write_all(
            "123,0,0.5,193,0.5,2,2021-11-01,0.5,foo,42,0,2021-01-01 00:00:00\n".as_bytes(),
        )
        .unwrap();

        vec![path_1]
    };

    let _ = service
        .exec_query("CREATE SCHEMA IF NOT EXISTS test")
        .await
        .unwrap();
    let _ = service.exec_query(
        &format!(
            "CREATE TABLE test.main (`c1` varchar(255), `c2` date, `c3` bigint, `c4` bigint, `c5` bigint, `c6` bigint, `c7` double, `c8` double, `c9` double, `c10` double, `c11` double, `c12` timestamp)  LOCATION {}",
            paths.into_iter().map(|p| format!("'{}'", p.to_string_lossy())).join(",")
        )
    ).await.unwrap();

    let result = service
        .exec_query("SELECT count(*) as cnt from test.main")
        .await
        .unwrap();
    assert_eq!(result.get_rows(), &vec![Row::new(vec![TableValue::Int(1)])]);
}

async fn create_table_with_location_invalid_digit(service: Box<dyn SqlClient>) {
    let paths = {
        let dir = env::temp_dir();

        let path_1 = dir.clone().join("invalid_digit.csv");
        let mut file = File::create(path_1.clone()).unwrap();

        file.write_all("c1,c3\n".as_bytes()).unwrap();
        file.write_all("foo,1a23\n".as_bytes()).unwrap();

        vec![path_1]
    };

    let _ = service
        .exec_query("CREATE SCHEMA IF NOT EXISTS test")
        .await
        .unwrap();
    let res = service
        .exec_query(&format!(
            "CREATE TABLE test.main (`c1` text, `c3` decimal)  LOCATION {}",
            paths
                .into_iter()
                .map(|p| format!("'{}'", p.to_string_lossy()))
                .join(",")
        ))
        .await;

    println!("Res: {:?}", res);

    assert!(
        res.is_err(),
        "Expected invalid digit error but got {:?}",
        res
    );
}

async fn create_table_with_csv(service: Box<dyn SqlClient>) {
    let file = write_tmp_file(indoc! {"
        fruit,number
        apple,2
        banana,3
    "})
    .unwrap();
    let path = file.path().to_string_lossy();
    let _ = service
        .exec_query("CREATE SCHEMA IF NOT EXISTS test")
        .await
        .unwrap();
    let _ = service
        .exec_query(format!("CREATE TABLE test.table (`fruit` text, `number` int) WITH (input_format = 'csv') LOCATION '{}'", path).as_str())
        .await
        .unwrap();
    let result = service
        .exec_query("SELECT * FROM test.table")
        .await
        .unwrap();
    assert_eq!(
        to_rows(&result),
        vec![
            vec![TableValue::String("apple".to_string()), TableValue::Int(2)],
            vec![TableValue::String("banana".to_string()), TableValue::Int(3)]
        ]
    );
}

async fn create_table_with_csv_and_index(service: Box<dyn SqlClient>) {
    let file = write_tmp_file(indoc! {"
        fruit,number
        apple,2
        banana,3
    "})
    .unwrap();
    let path = file.path().to_string_lossy();
    let _ = service
        .exec_query("CREATE SCHEMA IF NOT EXISTS test")
        .await
        .unwrap();
    let _ = service
        .exec_query(format!("CREATE TABLE test.table (`fruit` text, `number` int) WITH (input_format = 'csv') INDEX by_number (`number`) LOCATION '{}'", path).as_str())
        .await
        .unwrap();
    let result = service
        .exec_query("SELECT * FROM test.table")
        .await
        .unwrap();
    assert_eq!(
        to_rows(&result),
        vec![
            vec![TableValue::String("apple".to_string()), TableValue::Int(2)],
            vec![TableValue::String("banana".to_string()), TableValue::Int(3)]
        ]
    );
}

async fn create_table_with_csv_no_header(service: Box<dyn SqlClient>) {
    let file = write_tmp_file(indoc! {"
        apple,2
        banana,3
    "})
    .unwrap();
    let path = file.path().to_string_lossy();
    let _ = service
        .exec_query("CREATE SCHEMA IF NOT EXISTS test")
        .await
        .unwrap();
    let _ = service
        .exec_query(format!("CREATE TABLE test.table (`fruit` text, `number` int) WITH (input_format = 'csv_no_header') LOCATION '{}'", path).as_str())
        .await
        .unwrap();
    let result = service
        .exec_query("SELECT * FROM test.table")
        .await
        .unwrap();
    assert_eq!(
        to_rows(&result),
        vec![
            vec![TableValue::String("apple".to_string()), TableValue::Int(2)],
            vec![TableValue::String("banana".to_string()), TableValue::Int(3)]
        ]
    );
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

async fn hyperloglog_postgres(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA s").await.unwrap();
    service
        .exec_query("CREATE TABLE s.hlls(id int, hll HLL_POSTGRES)")
        .await
        .unwrap();
    service.exec_query("INSERT INTO s.hlls(id, hll) VALUES \
        (1, X'118b7f'),\
        (2, X'128b7fee22c470691a8134'),\
        (3, X'138b7f04a10642078507c308e309230a420ac10c2510a2114511611363138116811848188218a119411a821ae11f0122e223a125a126632685276327a328e2296129e52b812fe23081320132c133e335a53641368236a23721374237e1382138e13a813c243e6140e341854304434148a24a034f8150c1520152e254e155a1564157e158e35ac25b265b615c615fc1620166a368226a416a626c016c816d677163728275817a637a817ac37b617c247c427d677f6180e18101826382e1846184e18541858287e1880189218a418b818bc38e018ea290a19244938295e4988198c299e29b239b419c419ce49da1a1e1a321a381a4c1aa61acc2ae01b0a1b101b142b161b443b801bd02bd61bf61c263c4a3c501c7a1caa1cb03cd03cf03cf42d123d4c3d662d744d901dd01df81e001e0a2e641e7e3edc1f0a2f1c1f203f484f5c4f763fc84fdc1fe02fea1'),\
        (4, X'148b7f21083288a4320a12086719c65108c1088422884511063388232904418c8520484184862886528c65198832106328c83114e6214831108518d03208851948511884188441908119083388661842818c43190c320ce4210a50948221083084a421c8328c632104221c4120d01284e20902318ca5214641942319101294641906228483184e128c43188e308882204a538c8328903288642102220c64094631086330c832106320c46118443886329062118a230c63108a320c23204a11852419c6528c85210a318c6308c41088842086308ce7110a418864190650884210ca631064108642a1022186518c8509862109020a0a4318671144150842400e5090631a0811848320c821888120c81114a220880290622906310d0220c83090a118c433106128c221902210cc23106029044114841104409862190c43188111063104c310c6728c8618c62290441102310c23214440882438ca2110a32908548c432110329462188a43946328842114640944320884190c928c442084228863318a2190a318c6618ca3114651886618c44190c5108e2110612144319062284641908428882314862106419883310421988619ca420cc511442104633888218c4428465288651910730c81118821088218c6418c45108452106519ce410d841904218863308622086211483198c710c83104a328c620906218864118623086418c8711423094632186420c4620c41104620a441108e40882628c6311c212046428c8319021104672888428ca320c431984418c4209043084451886510c641108310c4c20c66188472146310ca71084820c621946218c8228822190e2410861904411c27288621144328c6440c6311063190813086228ca710c2218c4718865188c2114850888608864404a3194e22882310ce53088619ca31904519503188e1118c4214cb2948110c6119c2818c843108520c43188c5204821186528c871908311086214c630c4218c8418cc3298a31888210c63110a121042198622886531082098c419c4210c6210c8338c25294610944518c442104610884104424206310c8311462288873102308c2440c451082228824310440982220c4240c622084310c642850118c641148430d0128c8228c2120c221884428863208c21a0a4190a4404c21186548865204633906308ca32086211c8319ce22146520c6120803318a518c840084519461208c21908538cc428c2110844384e40906320c44014a3204e62042408c8328c632146318c812004310c41318e3208a5308a511827104a4188c51048421446090a7088631102231484104473084318c41210860906919083190652906129c4628c45310652848221443114420084500865184a618c81198c32906418c63190e320c231882728484184671888309465188a320c83208632144318c6331c642988108c61218812144328d022844021022184a31908328c6218c2328c4528cc541428190641046418c84108443146230c6419483214232184411863290a210824318c220868194631106618c43188821048230c4128c6310c0330462094241106330c42188c321043118863046438823110a041464108e3190e4209a11902439c43188631104321008090441106218c6419064294a229463594622244320cc71184510902924421908218c62308641044328ca328882111012884120ca52882428c62184442086718c4221c8211082208a321023115270086218c4218c6528ce400482310a520c43104a520c44210811884118c4310864198263942331822')"
    ).await.unwrap();

    let r = service
        .exec_query("SELECT id, cardinality(hll) FROM s.hlls ORDER BY id")
        .await
        .unwrap();
    assert_eq!(to_rows(&r), rows(&[(1, 0), (2, 1), (3, 164), (4, 9722)]));
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
        "Projection, [url, SUM(s.Data.hits)@1:SUM(hits)]\
       \n  FinalInplaceAggregate\
       \n    ClusterSend, partitions: [[1]]"
    );
    assert_eq!(
        pp_phys_plan(p.worker.as_ref()),
        "Projection, [url, SUM(s.Data.hits)@1:SUM(hits)]\
      \n  FinalInplaceAggregate\
      \n    Worker\
      \n      PartialInplaceAggregate\
      \n        MergeSort\
      \n          Scan, index: default:1:[1]:sort_on[url], fields: [url, hits]\
      \n            Empty"
    );

    // When there is no index, we fallback to inplace aggregates.
    let p = service
        .plan_query("SELECT day, SUM(hits) FROM s.Data GROUP BY 1")
        .await
        .unwrap();
    assert_eq!(
        pp_phys_plan(p.router.as_ref()),
        "Projection, [day, SUM(s.Data.hits)@1:SUM(hits)]\
       \n  FinalHashAggregate\
       \n    ClusterSend, partitions: [[1]]"
    );
    assert_eq!(
        pp_phys_plan(p.worker.as_ref()),
        "Projection, [day, SUM(s.Data.hits)@1:SUM(hits)]\
       \n  FinalHashAggregate\
       \n    Worker\
       \n      PartialHashAggregate\
       \n        Merge\
       \n          Scan, index: default:1:[1], fields: [day, hits]\
       \n            Empty"
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
        "Projection, [url, SUM(Data.hits)@1:hits]\
           \n  AggregateTopK, limit: 10, sortBy: [2 desc null last]\
           \n    ClusterSend, partitions: [[1, 2]]"
    );
    assert_eq!(
        pp_phys_plan_ext(p.worker.as_ref(), &verbose),
        "Projection, [url, SUM(Data.hits)@1:hits]\
           \n  AggregateTopK, limit: 10, sortBy: [2 desc null last]\
           \n    Worker\
           \n      Sort, by: [SUM(hits)@1 desc nulls last]\
           \n        FullInplaceAggregate, sort_order: [0]\
           \n          MergeSort, single_vals: [0, 1], sort_order: [0, 1, 2, 3, 4]\
           \n            Union, single_vals: [0, 1], sort_order: [0, 1, 2, 3, 4]\
           \n              Filter, single_vals: [0, 1], sort_order: [0, 1, 2, 3, 4]\
           \n                MergeSort, sort_order: [0, 1, 2, 3, 4]\
           \n                  Scan, index: default:1:[1], fields: *, sort_order: [0, 1, 2, 3, 4]\
           \n                    Empty\
           \n              Filter, single_vals: [0, 1], sort_order: [0, 1, 2, 3, 4]\
           \n                MergeSort, sort_order: [0, 1, 2, 3, 4]\
           \n                  Scan, index: default:2:[2], fields: *, sort_order: [0, 1, 2, 3, 4]\
           \n                    Empty"
    );
}

async fn partitioned_index(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA s").await.unwrap();
    service
        .exec_query("CREATE PARTITIONED INDEX s.ind(id int, url text)")
        .await
        .unwrap();
    service
        .exec_query(
            "CREATE TABLE s.Data1(id int, url text, hits int) \
                     ADD TO PARTITIONED INDEX s.ind(id, url)",
        )
        .await
        .unwrap();
    service
        .exec_query(
            "CREATE TABLE s.Data2(id2 int, url2 text, location text) \
                     ADD TO PARTITIONED INDEX s.ind(id2, url2)",
        )
        .await
        .unwrap();

    service
        .exec_query(
            "INSERT INTO s.Data1(id, url, hits) VALUES (0, 'a', 10), (1, 'a', 20), (2, 'c', 30)",
        )
        .await
        .unwrap();
    service
        .exec_query("INSERT INTO s.Data2(id2, url2, location) VALUES (0, 'a', 'Mars'), (1, 'c', 'Earth'), (2, 'c', 'Moon')")
        .await
        .unwrap();

    let r = service
        .exec_query(
            "SELECT id, url, hits, location \
                     FROM s.Data1 `l` JOIN s.Data2 `r` ON l.id = r.id2 AND l.url = r.url2 \
                     ORDER BY 1, 2",
        )
        .await
        .unwrap();
    assert_eq!(
        to_rows(&r),
        rows(&[(0, "a", 10, "Mars"), (2, "c", 30, "Moon")])
    );
}

async fn partitioned_index_if_not_exists(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA s").await.unwrap();
    service
        .exec_query("CREATE PARTITIONED INDEX s.ind(id int, url text)")
        .await
        .unwrap();
    service
        .exec_query("CREATE PARTITIONED INDEX s.ind(id int, url text)")
        .await
        .unwrap_err();
    service
        .exec_query("CREATE PARTITIONED INDEX IF NOT EXISTS s.ind(id int, url text)")
        .await
        .unwrap();

    service
        .exec_query("CREATE PARTITIONED INDEX IF NOT EXISTS s.other_ind(id int, url text)")
        .await
        .unwrap();
}

async fn drop_partitioned_index(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA s").await.unwrap();
    service
        .exec_query("CREATE PARTITIONED INDEX s.ind(url text, some_column int)")
        .await
        .unwrap();
    // DROP without any data.
    service
        .exec_query("DROP PARTITIONED INDEX s.ind")
        .await
        .unwrap();
    // Another drop fails as index does not exist.
    service
        .exec_query("DROP PARTITIONED INDEX s.ind")
        .await
        .unwrap_err();
    // Note columns are different.
    service
        .exec_query("CREATE PARTITIONED INDEX s.ind(id int, url text)")
        .await
        .unwrap();
    service
        .exec_query(
            "CREATE TABLE s.Data1(id int, url text, hits int) \
                     ADD TO PARTITIONED INDEX s.ind(id, url)",
        )
        .await
        .unwrap();
    service
        .exec_query(
            "CREATE TABLE s.Data2(id2 int, url2 text, location text) \
                     ADD TO PARTITIONED INDEX s.ind(id2, url2)",
        )
        .await
        .unwrap();
    service
        .exec_query(
            "INSERT INTO s.Data1(id, url, hits) VALUES (0, 'a', 10), (1, 'a', 20), (2, 'c', 30)",
        )
        .await
        .unwrap();
    service
        .exec_query("INSERT INTO s.Data2(id2, url2, location) VALUES (0, 'a', 'Mars'), (1, 'c', 'Earth'), (2, 'c', 'Moon')")
        .await
        .unwrap();

    let r = service
        .exec_query(
            "SELECT id, url, hits, location \
                     FROM s.Data1 `l` JOIN s.Data2 `r` ON l.id = r.id2 AND l.url = r.url2 \
                     ORDER BY 1, 2",
        )
        .await
        .unwrap();
    assert_eq!(
        to_rows(&r),
        rows(&[(0, "a", 10, "Mars"), (2, "c", 30, "Moon")])
    );

    service
        .exec_query("DROP PARTITIONED INDEX s.ind")
        .await
        .unwrap();
    service
        .exec_query(
            "CREATE TABLE s.Data3(id3 int, url3 text, location text) \
                     ADD TO PARTITIONED INDEX s.ind(id3, url3)",
        )
        .await
        .unwrap_err(); // Fails as the index does not exist anymore.

    // Query can still run from the default table data.
    let r = service
        .exec_query(
            "SELECT id, url, hits, location \
                     FROM s.Data1 `l` JOIN s.Data2 `r` ON l.id = r.id2 AND l.url = r.url2 \
                     ORDER BY 1, 2",
        )
        .await
        .unwrap();
    assert_eq!(
        to_rows(&r),
        rows(&[(0, "a", 10, "Mars"), (2, "c", 30, "Moon")])
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
        "Projection, [id, SUM(s.Orders.amount)@1:SUM(amount)]\
       \n  FinalInplaceAggregate\
       \n    ClusterSend, partitions: [[1]]"
    );
    assert_eq!(
        pp_phys_plan(p.worker.as_ref()),
        "Projection, [id, SUM(s.Orders.amount)@1:SUM(amount)]\
       \n  FinalInplaceAggregate\
       \n    Worker\
       \n      PartialInplaceAggregate\
       \n        MergeSort\
       \n          Scan, index: default:1:[1]:sort_on[id], fields: [id, amount]\
       \n            Empty"
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
        "Projection, [id, SUM(amount)]\
       \n  FinalInplaceAggregate\
       \n    ClusterSend, partitions: [[1, 1]]"
    );
    assert_eq!(
        pp_phys_plan(p.worker.as_ref()),
        "Projection, [id, SUM(amount)]\
       \n  FinalInplaceAggregate\
       \n    Worker\
       \n      PartialInplaceAggregate\
       \n        MergeSort\
       \n          Union\
       \n            MergeSort\
       \n              Scan, index: default:1:[1]:sort_on[id], fields: [id, amount]\
       \n                Empty\
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
           \n    MergeJoin, on: [customer_id@1 = customer_id@0]\
           \n      MergeSort\
           \n        Scan, index: by_customer:2:[2]:sort_on[customer_id], fields: [order_id, customer_id]\
           \n          Empty\
           \n      MergeSort\
           \n        Scan, index: default:3:[3]:sort_on[customer_id], fields: *\
           \n          Empty"
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
       \n  Projection, [order_id, customer_name, SUM(o.amount)@2:SUM(amount)]\
       \n    FinalHashAggregate\
       \n      ClusterSend, partitions: [[2, 3]]"
    );
    assert_eq!(
        pp_phys_plan(p.worker.as_ref()),
        "Sort\
       \n  Projection, [order_id, customer_name, SUM(o.amount)@2:SUM(amount)]\
       \n    FinalHashAggregate\
       \n      Worker\
       \n        PartialHashAggregate\
       \n          MergeJoin, on: [customer_id@1 = customer_id@0]\
       \n            MergeSort\
       \n              Scan, index: by_customer:2:[2]:sort_on[customer_id], fields: *\
       \n                Empty\
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
           \n    MergeJoin, on: [product_id@2 = product_id@0]\
           \n      MergeResort\
           \n        MergeJoin, on: [customer_id@1 = customer_id@0]\
           \n          MergeSort\
           \n            Scan, index: by_customer:2:[2]:sort_on[customer_id], fields: [order_id, customer_id, product_id]\
           \n              Empty\
           \n          MergeSort\
           \n            Scan, index: default:3:[3]:sort_on[customer_id], fields: *\
           \n              Empty\
           \n      MergeSort\
           \n        Scan, index: default:4:[4]:sort_on[product_id], fields: *\
           \n          Empty",
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
           \n    MergeJoin, on: [product_id@2 = product_id@0]\
           \n      MergeResort\
           \n        MergeJoin, on: [customer_id@1 = customer_id@0]\
           \n          Filter, predicate: product_id@2 = 125\
           \n            MergeSort\
           \n              Scan, index: by_customer:2:[2]:sort_on[customer_id], fields: [order_id, customer_id, product_id], predicate: #product_id Eq Int64(125)\
           \n                Empty\
           \n          MergeSort\
           \n            Scan, index: default:3:[3]:sort_on[customer_id], fields: *\
           \n              Empty\
           \n      Filter, predicate: product_id@0 = 125\
           \n        MergeSort\
           \n          Scan, index: default:4:[4]:sort_on[product_id], fields: *, predicate: #product_id Eq Int64(125)\
           \n            Empty",
        );
}

async fn planning_join_with_partitioned_index(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA s").await.unwrap();
    service
        .exec_query("CREATE PARTITIONED INDEX s.by_customer(customer_id int)")
        .await
        .unwrap();

    service
        .exec_query(
            "CREATE TABLE s.Orders(order_id int, customer_id int, product_id int, amount int) \
             ADD TO PARTITIONED INDEX s.by_customer(customer_id)",
        )
        .await
        .unwrap();
    service
        .exec_query(
            "CREATE TABLE s.Customers(customer_id int, customer_name text) \
             ADD TO PARTITIONED INDEX s.by_customer(customer_id)",
        )
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
        "ClusterSend, partitions: [[1, 3]]"
    );
    assert_eq!(
        pp_phys_plan(p.worker.as_ref()),
        "Worker\
           \n  Projection, [order_id, customer_name]\
           \n    MergeJoin, on: [customer_id@1 = customer_id@0]\
           \n      MergeSort\
           \n        Scan, index: #mi0:1:[1]:sort_on[customer_id], fields: [order_id, customer_id]\
           \n          Empty\
           \n      MergeSort\
           \n        Scan, index: #mi0:3:[3]:sort_on[customer_id], fields: *\
           \n          Empty",
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
    assert_eq!(
        to_rows(&r),
        rows(&[("z", dec5(100)), ("y", dec5(80)), ("b", dec5(52))])
    );
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
      \n  Projection, [date_to, name, SUM(Table.n)@2:n]\
      \n    CrossJoinAgg, on: day@1 <= date_to@0\
      \n      Projection, [datetrunc(Utf8(\"day\"),converttz(s.Data.day,Utf8(\"+00:00\")))@0:day, name, SUM(s.Data.n)@2:n]\
      \n        FinalHashAggregate\
      \n          Worker\
      \n            PartialHashAggregate\
      \n              Merge\
      \n                Scan, index: default:1:[1], fields: *\
      \n                  Empty"
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
        \n    Projection, [date_to, name, SUM(Table.n)@2:n]\
        \n      CrossJoinAgg, on: day@1 <= date_to@0\
        \n        Projection, [datetrunc(Utf8(\"day\"),converttz(s.Data.day,Utf8(\"+00:00\")))@0:day, name, SUM(s.Data.n)@2:n]\
        \n          FinalHashAggregate\
        \n            Worker\
        \n              PartialHashAggregate\
        \n                Merge\
        \n                  Scan, index: default:1:[1], fields: *\
        \n                    Empty"
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
}

async fn rolling_window_query(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA s").await.unwrap();
    service
        .exec_query("CREATE TABLE s.Data(day int, name text, n int)")
        .await
        .unwrap();
    service
        .exec_query(
            "INSERT INTO s.Data(day, name, n) VALUES (1, 'john', 10), \
                                                     (1, 'sara', 7), \
                                                     (3, 'sara', 3), \
                                                     (3, 'john', 9), \
                                                     (3, 'john', 11), \
                                                     (5, 'timmy', 5)",
        )
        .await
        .unwrap();

    let r = service
        .exec_query(
            "SELECT day, ROLLING(SUM(n) RANGE 1 PRECEDING) \
             FROM (SELECT day, SUM(n) as n FROM s.Data GROUP BY 1) \
             ROLLING_WINDOW DIMENSION day \
             FROM 1 TO 5 EVERY 1 \
             ORDER BY 1",
        )
        .await
        .unwrap();
    assert_eq!(
        to_rows(&r),
        rows(&[(1, 17), (2, 17), (3, 23), (4, 23), (5, 5)])
    );

    // Same, without preceding, i.e. with missing nodes.
    let r = service
        .exec_query(
            "SELECT day, ROLLING(SUM(n) RANGE 0 PRECEDING) \
             FROM (SELECT day, SUM(n) as n FROM s.Data GROUP BY 1) \
             ROLLING_WINDOW DIMENSION day \
             FROM 1 TO 5 EVERY 1 \
             ORDER BY 1",
        )
        .await
        .unwrap();
    assert_eq!(
        to_rows(&r),
        rows(&[
            (1, Some(17)),
            (2, None),
            (3, Some(23)),
            (4, None),
            (5, Some(5))
        ])
    );

    // Unbounded windows.
    let r = service
        .exec_query(
            "SELECT day, ROLLING(SUM(n) RANGE UNBOUNDED PRECEDING) \
             FROM (SELECT day, SUM(n) as n FROM s.Data GROUP BY 1) \
             ROLLING_WINDOW DIMENSION day \
             FROM 1 TO 5 EVERY 1 \
             ORDER BY 1",
        )
        .await
        .unwrap();
    assert_eq!(
        to_rows(&r),
        rows(&[(1, 17), (2, 17), (3, 40), (4, 40), (5, 45)]),
    );
    let r = service
        .exec_query(
            "SELECT day, ROLLING(SUM(n) RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) \
             FROM (SELECT day, SUM(n) as n FROM s.Data GROUP BY 1) \
             ROLLING_WINDOW DIMENSION day \
             FROM 1 TO 5 EVERY 1 \
             ORDER BY 1",
        )
        .await
        .unwrap();
    assert_eq!(
        to_rows(&r),
        rows(&[(1, 45), (2, 28), (3, 28), (4, 5), (5, 5)])
    );
    let r = service
        .exec_query(
            "SELECT day, ROLLING(SUM(n) RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) \
             FROM (SELECT day, SUM(n) as n FROM s.Data GROUP BY 1) \
             ROLLING_WINDOW DIMENSION day \
             FROM 1 TO 5 EVERY 1 \
             ORDER BY 1",
        )
        .await
        .unwrap();
    assert_eq!(
        to_rows(&r),
        rows(&[(1, 45), (2, 45), (3, 45), (4, 45), (5, 45)])
    );
    // Combined windows.
    let r = service
        .exec_query(
            "SELECT day, ROLLING(SUM(n) RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) \
             FROM (SELECT day, SUM(n) as n FROM s.Data GROUP BY 1) \
             ROLLING_WINDOW DIMENSION day \
             FROM 1 TO 5 EVERY 1 \
             ORDER BY 1",
        )
        .await
        .unwrap();
    assert_eq!(
        to_rows(&r),
        rows(&[(1, 17), (2, 40), (3, 23), (4, 28), (5, 5)])
    );
    // Both bounds are either PRECEDING or FOLLOWING.
    let r = service
        .exec_query(
            "SELECT day, ROLLING(SUM(n) RANGE BETWEEN 1 FOLLOWING and 2 FOLLOWING) \
             FROM (SELECT day, SUM(n) as n FROM s.Data GROUP BY 1) \
             ROLLING_WINDOW DIMENSION day \
             FROM 1 TO 5 EVERY 1 \
             ORDER BY 1",
        )
        .await
        .unwrap();
    assert_eq!(
        to_rows(&r),
        rows(&[
            (1, Some(23)),
            (2, Some(23)),
            (3, Some(5)),
            (4, Some(5)),
            (5, None)
        ])
    );
    let r = service
        .exec_query(
            "SELECT day, ROLLING(SUM(n) RANGE BETWEEN 2 PRECEDING and 1 PRECEDING) \
             FROM (SELECT day, SUM(n) as n FROM s.Data GROUP BY 1) \
             ROLLING_WINDOW DIMENSION day \
             FROM 1 TO 5 EVERY 1 \
             ORDER BY 1",
        )
        .await
        .unwrap();
    assert_eq!(
        to_rows(&r),
        rows(&[
            (1, None),
            (2, Some(17)),
            (3, Some(17)),
            (4, Some(23)),
            (5, Some(23))
        ])
    );
    // Empty inputs.
    let r = service
        .exec_query(
            "SELECT day, ROLLING(SUM(n) RANGE 0 PRECEDING) \
             FROM (SELECT day, n FROM s.Data WHERE day = 123123123) \
             ROLLING_WINDOW DIMENSION day \
             FROM 1 TO 5 EVERY 1 \
             ORDER BY 1",
        )
        .await
        .unwrap();
    assert_eq!(to_rows(&r), vec![] as Vec<Vec<_>>);

    // Broader range step than input data.
    let r = service
        .exec_query(
            "SELECT day, ROLLING(SUM(n) RANGE BETWEEN 1 PRECEDING AND 2 FOLLOWING) \
             FROM (SELECT day, SUM(n) as n FROM s.Data GROUP BY 1) \
             ROLLING_WINDOW DIMENSION day \
             FROM 1 TO 5 EVERY 4 \
             ORDER BY 1",
        )
        .await
        .unwrap();
    assert_eq!(to_rows(&r), rows(&[(1, 40), (5, 5)]));

    // Dimension values not in the input data.
    let r = service
        .exec_query(
            "SELECT day, ROLLING(SUM(n) RANGE BETWEEN 1 PRECEDING AND 2 FOLLOWING) \
             FROM (SELECT day, SUM(n) as n FROM s.Data GROUP BY 1) \
             ROLLING_WINDOW DIMENSION day \
             FROM -10 TO 10 EVERY 5 \
             ORDER BY 1",
        )
        .await
        .unwrap();
    assert_eq!(
        to_rows(&r),
        rows(&[
            (-10, None),
            (-5, None),
            (0, Some(17)),
            (5, Some(5)),
            (10, None)
        ])
    );

    // Partition by clause.
    let r = service
        .exec_query(
            "SELECT day, name, ROLLING(SUM(n) RANGE 2 PRECEDING) \
             FROM (SELECT day, name, SUM(n) as n FROM s.Data GROUP BY 1, 2) \
             ROLLING_WINDOW DIMENSION day \
             PARTITION BY name \
             FROM 1 TO 5 EVERY 2 \
             ORDER BY 1, 2",
        )
        .await
        .unwrap();
    assert_eq!(
        to_rows(&r),
        rows(&[
            (1, "john", 10),
            (1, "sara", 7),
            (3, "john", 30),
            (3, "sara", 10),
            (5, "john", 20),
            (5, "sara", 3),
            (5, "timmy", 5)
        ])
    );

    let r = service
        .exec_query(
            "SELECT day, name, ROLLING(SUM(n) RANGE 1 PRECEDING) \
             FROM (SELECT day, name, SUM(n) as n FROM s.Data GROUP BY 1, 2) \
             ROLLING_WINDOW DIMENSION day \
             PARTITION BY name \
             FROM 1 TO 5 EVERY 2 \
             ORDER BY 1, 2",
        )
        .await
        .unwrap();
    assert_eq!(
        to_rows(&r),
        rows(&[
            (1, "john", 10),
            (1, "sara", 7),
            (3, "john", 20),
            (3, "sara", 3),
            (5, "timmy", 5)
        ])
    );

    // Missing dates must be filled.
    let r = service
        .exec_query(
            "SELECT day, name, ROLLING(SUM(n) RANGE CURRENT ROW) \
             FROM (SELECT day, name, SUM(n) as n FROM s.Data GROUP BY 1, 2) \
             ROLLING_WINDOW DIMENSION day \
             PARTITION BY name \
             FROM 1 TO 5 EVERY 1 \
             ORDER BY 1, 2",
        )
        .await
        .unwrap();
    assert_eq!(
        to_rows(&r),
        rows(&[
            (1, Some("john"), Some(10)),
            (1, Some("sara"), Some(7)),
            (2, None, None),
            (3, Some("john"), Some(20)),
            (3, Some("sara"), Some(3)),
            (4, None, None),
            (5, Some("timmy"), Some(5))
        ])
    );

    // Check for errors.
    // GROUP BY not allowed with ROLLING.
    service
        .exec_query("SELECT day, ROLLING(SUM(n) RANGE 2 PRECEDING) FROM s.Data GROUP BY 1 ROLLING_WINDOW DIMENSION day FROM 0 TO 10 EVERY 2")
        .await
        .unwrap_err();
    // Rolling aggregate without ROLLING_WINDOW.
    service
        .exec_query("SELECT day, ROLLING(SUM(n) RANGE 2 PRECEDING) FROM s.Data")
        .await
        .unwrap_err();
    // ROLLING_WINDOW without rolling aggregate.
    service
        .exec_query("SELECT day, n FROM s.Data ROLLING_WINDOW DIMENSION day FROM 0 to 10 EVERY 2")
        .await
        .unwrap_err();
    // No RANGE in rolling aggregate.
    service
        .exec_query("SELECT day, ROLLING(SUM(n)) FROM s.Data ROLLING_WINDOW DIMENSION day FROM 0 to 10 EVERY 2")
        .await
        .unwrap_err();
    // No DIMENSION.
    service
        .exec_query("SELECT day, ROLLING(SUM(n) RANGE 2 PRECEDING) FROM s.Data ROLLING_WINDOW FROM 0 to 10 EVERY 2")
        .await
        .unwrap_err();
    // Invalid DIMENSION.
    service
        .exec_query("SELECT day, ROLLING(SUM(n) RANGE 2 PRECEDING) FROM s.Data ROLLING_WINDOW DIMENSION unknown FROM 0 to 10 EVERY 2")
        .await
        .unwrap_err();
    // Invalid types in FROM, TO, EVERY.
    service
        .exec_query("SELECT day, ROLLING(SUM(n) RANGE 2 PRECEDING) FROM s.Data ROLLING_WINDOW DIMENSION day FROM 'a' to 10 EVERY 1")
        .await
        .unwrap_err();
    service
        .exec_query("SELECT day, ROLLING(SUM(n) RANGE 2 PRECEDING) FROM s.Data ROLLING_WINDOW DIMENSION day FROM 0 to 'a' EVERY 1")
        .await
        .unwrap_err();
    service
        .exec_query("SELECT day, ROLLING(SUM(n) RANGE 2 PRECEDING) FROM s.Data ROLLING_WINDOW DIMENSION day FROM 0 to 10 EVERY 'a'")
        .await
        .unwrap_err();
    // Invalid values for FROM, TO, EVERY
    service
        .exec_query("SELECT day, ROLLING(SUM(n) RANGE 2 PRECEDING) FROM s.Data ROLLING_WINDOW DIMENSION day FROM 0 to 10 EVERY 0")
        .await
        .unwrap_err();
    service
        .exec_query("SELECT day, ROLLING(SUM(n) RANGE 2 PRECEDING) FROM s.Data ROLLING_WINDOW DIMENSION day FROM 0 to 10 EVERY -10")
        .await
        .unwrap_err();
    service
        .exec_query("SELECT day, ROLLING(SUM(n) RANGE 2 PRECEDING) FROM s.Data ROLLING_WINDOW DIMENSION day FROM 10 to 0 EVERY 10")
        .await
        .unwrap_err();
}

async fn rolling_window_exprs(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA s").await.unwrap();
    service
        .exec_query("CREATE TABLE s.data(day int, n int)")
        .await
        .unwrap();
    service
        .exec_query("INSERT INTO s.data(day, n) VALUES(1, 10), (2, 20), (3, 30)")
        .await
        .unwrap();
    let r = service
        .exec_query(
            "SELECT ROLLING(SUM(n) RANGE 1 PRECEDING) / ROLLING(COUNT(n) RANGE 1 PRECEDING),\
                    ROLLING(AVG(n) RANGE 1 PRECEDING) \
             FROM (SELECT * FROM s.data) \
             ROLLING_WINDOW DIMENSION day FROM 1 to 3 EVERY 1",
        )
        .await
        .unwrap();
    assert_eq!(to_rows(&r), rows(&[(10, 10.), (15, 15.), (25, 25.)]))
}

async fn rolling_window_query_timestamps(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA s").await.unwrap();
    service
        .exec_query("CREATE TABLE s.data(day timestamp, name string, n int)")
        .await
        .unwrap();
    service
        .exec_query(
            "INSERT INTO s.data(day, name, n)\
                        VALUES \
                         ('2021-01-01T00:00:00Z', 'john', 10), \
                         ('2021-01-01T00:00:00Z', 'sara', 7), \
                         ('2021-01-03T00:00:00Z', 'sara', 3), \
                         ('2021-01-03T00:00:00Z', 'john', 9), \
                         ('2021-01-03T00:00:00Z', 'john', 11), \
                         ('2021-01-05T00:00:00Z', 'timmy', 5)",
        )
        .await
        .unwrap();

    let mut jan = (1..=5)
        .map(|d| timestamp_from_string(&format!("2021-01-{:02}T00:00:00.000Z", d)).unwrap())
        .collect_vec();
    jan.insert(0, jan[1]); // jan[i] will correspond to i-th day of the month.

    let r = service
        .exec_query(
            "SELECT day, ROLLING(SUM(n) RANGE INTERVAL '1 day' PRECEDING) \
             FROM (SELECT day, SUM(n) as n FROM s.data GROUP BY 1) \
             ROLLING_WINDOW DIMENSION day \
               FROM to_timestamp('2021-01-01T00:00:00Z') \
               TO to_timestamp('2021-01-05T00:00:00Z') \
               EVERY INTERVAL '1 day' \
             ORDER BY 1",
        )
        .await
        .unwrap();
    assert_eq!(
        to_rows(&r),
        rows(&[
            (jan[1], 17),
            (jan[2], 17),
            (jan[3], 23),
            (jan[4], 23),
            (jan[5], 5)
        ])
    );
}

async fn rolling_window_extra_aggregate(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA s").await.unwrap();
    service
        .exec_query("CREATE TABLE s.Data(day int, name text, n int)")
        .await
        .unwrap();
    service
        .exec_query(
            "INSERT INTO s.Data(day, name, n) VALUES (1, 'john', 10), \
                                                     (1, 'sara', 7), \
                                                     (3, 'sara', 3), \
                                                     (3, 'john', 9), \
                                                     (3, 'john', 11), \
                                                     (5, 'timmy', 5)",
        )
        .await
        .unwrap();

    let r = service
        .exec_query(
            "SELECT day, ROLLING(SUM(n) RANGE 1 PRECEDING), SUM(n) \
             FROM (SELECT day, SUM(n) as n FROM s.Data GROUP BY 1) \
             ROLLING_WINDOW DIMENSION day \
             GROUP BY DIMENSION day \
             FROM 1 TO 5 EVERY 1 \
             ORDER BY 1",
        )
        .await
        .unwrap();
    assert_eq!(
        to_rows(&r),
        rows(&[
            (1, 17, Some(17)),
            (2, 17, None),
            (3, 23, Some(23)),
            (4, 23, None),
            (5, 5, Some(5))
        ])
    );

    // We could also distribute differently.
    let r = service
        .exec_query(
            "SELECT day, ROLLING(SUM(n) RANGE 1 PRECEDING), SUM(n) \
             FROM (SELECT day, SUM(n) as n FROM s.Data GROUP BY 1) \
             ROLLING_WINDOW DIMENSION day \
             GROUP BY DIMENSION CASE WHEN day <= 3 THEN 1 ELSE 5 END \
             FROM 1 TO 5 EVERY 1 \
             ORDER BY 1",
        )
        .await
        .unwrap();
    assert_eq!(
        to_rows(&r),
        rows(&[
            (1, 17, Some(40)),
            (2, 17, None),
            (3, 23, None),
            (4, 23, None),
            (5, 5, Some(5))
        ])
    );

    // Putting everything into an out-of-range dimension.
    let r = service
        .exec_query(
            "SELECT day, ROLLING(SUM(n) RANGE 1 PRECEDING), SUM(n) \
             FROM (SELECT day, SUM(n) as n FROM s.Data GROUP BY 1) \
             ROLLING_WINDOW DIMENSION day \
             GROUP BY DIMENSION 6 \
             FROM 1 TO 5 EVERY 1 \
             ORDER BY 1",
        )
        .await
        .unwrap();
    assert_eq!(
        to_rows(&r),
        rows(&[
            (1, 17, NULL),
            (2, 17, NULL),
            (3, 23, NULL),
            (4, 23, NULL),
            (5, 5, NULL)
        ])
    );

    // Check errors.
    // Mismatched types.
    service
        .exec_query(
            "SELECT day, ROLLING(SUM(n) RANGE 1 PRECEDING), SUM(n) \
             FROM (SELECT day, SUM(n) as n FROM s.Data GROUP BY 1) \
             ROLLING_WINDOW DIMENSION day \
             GROUP BY DIMENSION 'aaa' \
             FROM 1 TO 5 EVERY 1 \
             ORDER BY 1",
        )
        .await
        .unwrap_err();
    // Aggregate without GROUP BY DIMENSION.
    service
        .exec_query(
            "SELECT day, ROLLING(SUM(n) RANGE 1 PRECEDING), SUM(n) \
             FROM (SELECT day, SUM(n) as n FROM s.Data GROUP BY 1) \
             ROLLING_WINDOW DIMENSION day \
             FROM 1 TO 5 EVERY 1 \
             ORDER BY 1",
        )
        .await
        .unwrap_err();
    // GROUP BY DIMENSION without aggregates.
    service
        .exec_query(
            "SELECT day, ROLLING(SUM(n) RANGE 1 PRECEDING) \
             FROM (SELECT day, SUM(n) as n FROM s.Data GROUP BY 1) \
             ROLLING_WINDOW DIMENSION day \
             GROUP BY DIMENSION 0 \
             FROM 1 TO 5 EVERY 1 \
             ORDER BY 1",
        )
        .await
        .unwrap_err();
}

async fn rolling_window_extra_aggregate_timestamps(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA s").await.unwrap();
    service
        .exec_query("CREATE TABLE s.data(day timestamp, name string, n int)")
        .await
        .unwrap();
    service
        .exec_query(
            "INSERT INTO s.data(day, name, n)\
                        VALUES \
                         ('2021-01-01T00:00:00Z', 'john', 10), \
                         ('2021-01-01T00:00:00Z', 'sara', 7), \
                         ('2021-01-03T00:00:00Z', 'sara', 3), \
                         ('2021-01-03T00:00:00Z', 'john', 9), \
                         ('2021-01-03T00:00:00Z', 'john', 11), \
                         ('2021-01-05T00:00:00Z', 'timmy', 5)",
        )
        .await
        .unwrap();

    let mut jan = (1..=5)
        .map(|d| timestamp_from_string(&format!("2021-01-{:02}T00:00:00.000Z", d)).unwrap())
        .collect_vec();
    jan.insert(0, jan[1]); // jan[i] will correspond to i-th day of the month.

    let r = service
        .exec_query(
            "SELECT day, ROLLING(SUM(n) RANGE INTERVAL '1 day' PRECEDING), SUM(n) \
             FROM (SELECT day, SUM(n) as n FROM s.data GROUP BY 1) \
             ROLLING_WINDOW DIMENSION day \
             GROUP BY DIMENSION day \
             FROM date_trunc('day', to_timestamp('2021-01-01T00:00:00Z')) \
             TO date_trunc('day', to_timestamp('2021-01-05T00:00:00Z')) \
             EVERY INTERVAL '1 day' \
             ORDER BY 1",
        )
        .await
        .unwrap();
    assert_eq!(
        to_rows(&r),
        rows(&[
            (jan[1], 17, Some(17)),
            (jan[2], 17, None),
            (jan[3], 23, Some(23)),
            (jan[4], 23, None),
            (jan[5], 5, Some(5))
        ])
    );
}

async fn rolling_window_one_week_interval(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA s").await.unwrap();
    service
        .exec_query("CREATE TABLE s.data(day timestamp, name string, n int)")
        .await
        .unwrap();
    service
        .exec_query(
            "INSERT INTO s.data(day, name, n)\
                        VALUES \
                         ('2021-01-01T00:00:00Z', 'john', 10), \
                         ('2021-01-01T00:00:00Z', 'sara', 7), \
                         ('2021-01-03T00:00:00Z', 'sara', 3), \
                         ('2021-01-03T00:00:00Z', 'john', 9), \
                         ('2021-01-03T00:00:00Z', 'john', 11), \
                         ('2021-01-05T00:00:00Z', 'timmy', 5)",
        )
        .await
        .unwrap();

    let mut jan = (1..=11)
        .map(|d| timestamp_from_string(&format!("2021-01-{:02}T00:00:00.000Z", d)).unwrap())
        .collect_vec();
    jan.insert(0, jan[1]); // jan[i] will correspond to i-th day of the month.

    let r = service
        .exec_query(
            "SELECT w, ROLLING(SUM(n) RANGE UNBOUNDED PRECEDING OFFSET START), SUM(CASE WHEN w >= to_timestamp('2021-01-04T00:00:00Z') AND w < to_timestamp('2021-01-11T00:00:00Z') THEN n END) \
             FROM (SELECT date_trunc('day', day) w, SUM(n) as n FROM s.data GROUP BY 1) \
             ROLLING_WINDOW DIMENSION w \
             GROUP BY DIMENSION date_trunc('week', w) \
             FROM date_trunc('week', to_timestamp('2021-01-04T00:00:00Z')) \
             TO date_trunc('week', to_timestamp('2021-01-11T00:00:00Z')) \
             EVERY INTERVAL '1 week' \
             ORDER BY 1",
        )
        .await
        .unwrap();
    assert_eq!(
        to_rows(&r),
        rows(&[(jan[4], 40, Some(5)), (jan[11], 45, None),])
    );
}

async fn rolling_window_offsets(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA s").await.unwrap();
    service
        .exec_query("CREATE TABLE s.data(day int, n int)")
        .await
        .unwrap();

    service
        .exec_query("INSERT INTO s.data(day, n) VALUES (1, 1), (2, 2), (3, 3), (5, 5), (9, 9)")
        .await
        .unwrap();
    let r = service
        .exec_query(
            "SELECT day, ROLLING(SUM(n) RANGE UNBOUNDED PRECEDING OFFSET END) \
             FROM s.data \
             ROLLING_WINDOW DIMENSION day FROM 0 TO 10 EVERY 2 \
             ORDER BY day",
        )
        .await
        .unwrap();
    assert_eq!(
        to_rows(&r),
        rows(&[(0, 1), (2, 6), (4, 11), (6, 11), (8, 20), (10, 20)])
    );
    let r = service
        .exec_query(
            "SELECT day, ROLLING(SUM(n) RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING OFFSET END) \
             FROM s.data \
             ROLLING_WINDOW DIMENSION day FROM 0 TO 10 EVERY 2 \
             ORDER BY day",
        )
        .await
        .unwrap();
    assert_eq!(
        to_rows(&r),
        rows(&[
            (0, Some(3)),
            (2, Some(5)),
            (4, Some(5)),
            (6, None),
            (8, Some(9)),
            (10, None)
        ])
    );
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

    assert_eq!(
        to_rows(&r),
        rows(&[(dec5(1), dec5(2)), (dec5(2), dec5(3)), (dec5(3), dec5(4))])
    );

    let r = service
        .exec_query("SELECT * FROM s.Data ORDER BY y DESC")
        .await
        .unwrap();
    assert_eq!(
        to_rows(&r),
        rows(&[(dec5(3), dec5(4)), (dec5(2), dec5(3)), (dec5(1), dec5(2))])
    );
}

async fn decimal_order(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA s").await.unwrap();
    service
        .exec_query("CREATE TABLE s.data(i decimal, j decimal)")
        .await
        .unwrap();
    service
        .exec_query(
            "INSERT INTO s.data(i, j) VALUES (1.0, -1.0), (2.0, 0.5), (0.5, 1.0), (100, -25.5)",
        )
        .await
        .unwrap();

    let r = service
        .exec_query("SELECT i FROM s.data ORDER BY 1 DESC")
        .await
        .unwrap();
    assert_eq!(
        to_rows(&r),
        rows(&[dec5(100), dec5(2), dec5(1), dec5f1(0, 5)])
    );

    // Two and more columns use a different code path, so test these too.
    let r = service
        .exec_query("SELECT i, j FROM s.data ORDER BY 2, 1")
        .await
        .unwrap();
    assert_eq!(
        to_rows(&r),
        rows(&[
            (dec5(100), dec5f1(-25, 5)),
            (dec5(1), dec5(-1)),
            (dec5(2), dec5f1(0, 5)),
            (dec5f1(0, 5), dec5(1))
        ])
    );
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
}

/// Ensure DataFusion code consistently uses IEEE754 total order for comparing floats.
async fn float_order(s: Box<dyn SqlClient>) {
    s.exec_query("CREATE SCHEMA s").await.unwrap();
    s.exec_query("CREATE TABLE s.data(f float, i int)")
        .await
        .unwrap();
    s.exec_query("INSERT INTO s.data(f, i) VALUES (0., -1), (-0., 1), (-0., 2), (0., -2)")
        .await
        .unwrap();

    // Sorting one and multiple columns use different code paths in DataFusion. Test both.
    let r = s
        .exec_query("SELECT f FROM s.data ORDER BY f")
        .await
        .unwrap();
    assert_eq!(to_rows(&r), rows(&[-0., -0., 0., 0.]));
    let r = s
        .exec_query("SELECT f, i FROM s.data ORDER BY f, i")
        .await
        .unwrap();
    assert_eq!(to_rows(&r), rows(&[(-0., 1), (-0., 2), (0., -2), (0., -1)]));

    // DataFusion compares grouping keys with a separate code path.
    let r = s
        .exec_query("SELECT f, min(i), max(i) FROM s.data GROUP BY f ORDER BY f")
        .await
        .unwrap();
    assert_eq!(to_rows(&r), rows(&[(-0., 1, 2), (0., -2, -1)]));
}

async fn date_add(service: Box<dyn SqlClient>) {
    let check_fun = |name, t, i, expected| {
        let expected = timestamp_from_string(expected).unwrap();
        let service = &service;
        async move {
            let actual = service
                .exec_query(&format!(
                    "SELECT {}(CAST('{}' as TIMESTAMP), INTERVAL '{}')",
                    name, t, i
                ))
                .await
                .unwrap();
            assert_eq!(to_rows(&actual), rows(&[expected]));
        }
    };
    let check_adds_to = |t, i, expected| check_fun("DATE_ADD", t, i, expected);
    let check_subs_to = |t, i, expected| check_fun("DATE_SUB", t, i, expected);

    check_adds_to("2021-01-01T00:00:00Z", "1 second", "2021-01-01T00:00:01Z").await;
    check_adds_to("2021-01-01T00:00:00Z", "1 minute", "2021-01-01T00:01:00Z").await;
    check_adds_to("2021-01-01T00:00:00Z", "1 hour", "2021-01-01T01:00:00Z").await;
    check_adds_to("2021-01-01T00:00:00Z", "1 day", "2021-01-02T00:00:00Z").await;

    check_adds_to(
        "2021-01-01T00:00:00Z",
        "1 day 1 hour 1 minute 1 second",
        "2021-01-02T01:01:01Z",
    )
    .await;
    check_subs_to(
        "2021-01-02T01:01:01Z",
        "1 day 1 hour 1 minute 1 second",
        "2021-01-01T00:00:00Z",
    )
    .await;

    check_adds_to("2021-01-01T00:00:00Z", "1 month", "2021-02-01T00:00:00Z").await;

    check_adds_to("2021-01-01T00:00:00Z", "1 year", "2022-01-01T00:00:00Z").await;
    check_subs_to("2022-01-01T00:00:00Z", "1 year", "2021-01-01T00:00:00Z").await;

    check_adds_to("2021-01-01T00:00:00Z", "13 month", "2022-02-01T00:00:00Z").await;
    check_subs_to("2022-02-01T00:00:00Z", "13 month", "2021-01-01T00:00:00Z").await;

    check_adds_to("2021-01-01T23:59:00Z", "1 minute", "2021-01-02T00:00:00Z").await;
    check_subs_to("2021-01-02T00:00:00Z", "1 minute", "2021-01-01T23:59:00Z").await;

    check_adds_to("2021-12-01T00:00:00Z", "1 month", "2022-01-01T00:00:00Z").await;
    check_subs_to("2022-01-01T00:00:00Z", "1 month", "2021-12-01T00:00:00Z").await;

    check_adds_to("2021-12-31T00:00:00Z", "1 day", "2022-01-01T00:00:00Z").await;
    check_subs_to("2022-01-01T00:00:00Z", "1 day", "2021-12-31T00:00:00Z").await;

    // Feb 29 on leap and non-leap years.
    check_adds_to("2020-02-29T00:00:00Z", "1 day", "2020-03-01T00:00:00Z").await;
    check_subs_to("2020-03-01T00:00:00Z", "1 day", "2020-02-29T00:00:00Z").await;

    check_adds_to("2020-02-28T00:00:00Z", "1 day", "2020-02-29T00:00:00Z").await;
    check_subs_to("2020-02-29T00:00:00Z", "1 day", "2020-02-28T00:00:00Z").await;

    check_adds_to("2021-02-28T00:00:00Z", "1 day", "2021-03-01T00:00:00Z").await;
    check_subs_to("2021-03-01T00:00:00Z", "1 day", "2021-02-28T00:00:00Z").await;

    check_adds_to("2020-02-29T00:00:00Z", "1 year", "2021-02-28T00:00:00Z").await;
    check_subs_to("2020-02-29T00:00:00Z", "1 year", "2019-02-28T00:00:00Z").await;

    check_adds_to("2020-01-30T00:00:00Z", "1 month", "2020-02-29T00:00:00Z").await;
    check_subs_to("2020-03-30T00:00:00Z", "1 month", "2020-02-29T00:00:00Z").await;

    check_adds_to("2020-01-29T00:00:00Z", "1 month", "2020-02-29T00:00:00Z").await;
    check_subs_to("2020-03-29T00:00:00Z", "1 month", "2020-02-29T00:00:00Z").await;

    check_adds_to("2021-01-29T00:00:00Z", "1 month", "2021-02-28T00:00:00Z").await;
    check_subs_to("2021-03-29T00:00:00Z", "1 month", "2021-02-28T00:00:00Z").await;

    // Nulls.
    let r = service
        .exec_query(
            "SELECT date_add(CAST(NULL as timestamp), INTERVAL '1 month'), \
                            date_sub(CAST(NULL as timestamp), INTERVAL '3 month')",
        )
        .await
        .unwrap();
    assert_eq!(to_rows(&r), rows(&[(NULL, NULL)]));

    // Invalid types passed to date_add.
    service
        .exec_query("SELECT date_add(1, 2)")
        .await
        .unwrap_err();
    service
        .exec_query("SELECT date_add(CAST('2021-01-01T00:00:00Z' as TIMESTAMP), 2)")
        .await
        .unwrap_err();
    service
        .exec_query("SELECT date_add(1, INTERVAL '1 second')")
        .await
        .unwrap_err();
    // Too many arguments.
    service
        .exec_query("SELECT date_add(1, 2, 3)")
        .await
        .unwrap_err();
    // Too few arguments
    service.exec_query("SELECT date_add(1)").await.unwrap_err();

    // Must work on columnar data.
    service.exec_query("CREATE SCHEMA s").await.unwrap();
    service
        .exec_query("CREATE TABLE s.data(t timestamp)")
        .await
        .unwrap();
    service
        .exec_query(
            "INSERT INTO s.data(t) VALUES ('2020-01-01T00:00:00Z'), ('2020-02-01T00:00:00Z'), (NULL)",
        )
        .await
        .unwrap();
    let r = service
        .exec_query("SELECT date_add(t, INTERVAL '1 year') FROM s.data ORDER BY 1")
        .await
        .unwrap();
    assert_eq!(
        to_rows(&r),
        rows(&[
            Some(timestamp_from_string("2021-01-01T00:00:00Z").unwrap()),
            Some(timestamp_from_string("2021-02-01T00:00:00Z").unwrap()),
            None,
        ]),
    );
    let r = service
        .exec_query("SELECT date_add(t, INTERVAL '1 hour') FROM s.data ORDER BY 1")
        .await
        .unwrap();
    assert_eq!(
        to_rows(&r),
        rows(&[
            Some(timestamp_from_string("2020-01-01T01:00:00Z").unwrap()),
            Some(timestamp_from_string("2020-02-01T01:00:00Z").unwrap()),
            None,
        ]),
    );
}

async fn unsorted_merge_assertion(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA s").await.unwrap();
    service
        .exec_query("CREATE TABLE s.Data1(x int, y int)")
        .await
        .unwrap();
    service
        .exec_query("INSERT INTO s.Data1(x,y) VALUES (1, 4), (2, 3), (3, 2)")
        .await
        .unwrap();

    service
        .exec_query("CREATE TABLE s.Data2(x int, y int)")
        .await
        .unwrap();
    service
        .exec_query("INSERT INTO s.Data2(x,y) VALUES (1, 4), (2, 3), (3, 2)")
        .await
        .unwrap();

    let r = service
        .exec_query(
            "SELECT x, y, count(x) \
             FROM (SELECT * FROM s.Data1 UNION ALL \
                   SELECT * FROM s.Data2)\
             GROUP BY y, x \
             ORDER BY y, x",
        )
        .await
        .unwrap();
    assert_eq!(to_rows(&r), rows(&[(3, 2, 2), (2, 3, 2), (1, 4, 2)]));
}

async fn unsorted_data_timestamps(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA s").await.unwrap();
    service
        .exec_query("CREATE TABLE s.data(t timestamp, n string)")
        .await
        .unwrap();
    service
        .exec_query(
            "INSERT INTO s.data(t, n) VALUES \
            ('2020-01-01T00:00:00.000000005Z', 'a'), \
            ('2020-01-01T00:00:00.000000001Z', 'b'), \
            ('2020-01-01T00:00:00.000000002Z', 'c')",
        )
        .await
        .unwrap();

    // CubeStore currently truncs timestamps to millisecond precision.
    // This checks we sort trunced precisions on inserts. We rely on implementation details of
    // CubeStore here.
    let r = service.exec_query("SELECT t, n FROM s.data").await.unwrap();

    let t = timestamp_from_string("2020-01-01T00:00:00Z").unwrap();
    assert_eq!(to_rows(&r), rows(&[(t, "a"), (t, "b"), (t, "c")]));

    // This ends up using MergeSortExec, make sure we see no assertions.
    let r = service
        .exec_query(
            "SELECT t, n FROM (SELECT * FROM s.data UNION ALL SELECT * FROM s.data) data \
        GROUP BY 1, 2 \
        ORDER BY 1, 2",
        )
        .await
        .unwrap();
    assert_eq!(to_rows(&r), rows(&[(t, "a"), (t, "b"), (t, "c")]));
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

#[allow(dead_code)]
async fn ksql_simple(service: Box<dyn SqlClient>) {
    let vars = env::var("TEST_KSQL_USER").and_then(|user| {
        env::var("TEST_KSQL_PASS")
            .and_then(|pass| env::var("TEST_KSQL_URL").and_then(|url| Ok((user, pass, url))))
    });
    if let Ok((user, pass, url)) = vars {
        service
            .exec_query(&format!("CREATE SOURCE OR UPDATE ksql AS 'ksql' VALUES (user = '{}', password = '{}', url = '{}')", user, pass, url))
            .await
            .unwrap();

        service.exec_query("CREATE SCHEMA test").await.unwrap();
        service.exec_query("CREATE TABLE test.events_by_type (`EVENT` text, `KSQL_COL_0` int) unique key (`EVENT`) location 'stream://ksql/EVENTS_BY_TYPE'").await.unwrap();
        for _ in 0..100 {
            let res = service
                .exec_query(
                    "SELECT * FROM test.events_by_type WHERE `EVENT` = 'load_request_success'",
                )
                .await
                .unwrap();
            if res.len() == 0 {
                futures_timer::Delay::new(Duration::from_millis(100)).await;
                continue;
            }
            if res.len() == 1 {
                return;
            }
        }
        panic!("Can't load data from ksql");
    }
}

async fn dimension_only_queries_for_stream_table(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA test").await.unwrap();
    service.exec_query("CREATE TABLE test.events_by_type (foo text, bar timestamp, bar_id text, measure1 int) unique key (foo, bar, bar_id)").await.unwrap();
    for i in 0..2 {
        for j in 0..2 {
            service
                .exec_query(&format!("INSERT INTO test.events_by_type (foo, bar, bar_id, measure1, __seq) VALUES ('a', '2021-01-01T00:00:00.000', '{}', {}, {})", i, j, i * 10 + j))
                .await
                .unwrap();
        }
    }
    let r = service
        .exec_query(
            "SELECT `bar_id` `bar_id` FROM test.events_by_type as `events` GROUP BY 1 ORDER BY 1 LIMIT 100",
        )
        .await
        .unwrap();

    assert_eq!(to_rows(&r), rows(&[("0"), ("1")]));
}

async fn unique_key_and_multi_measures_for_stream_table(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA test").await.unwrap();
    service.exec_query("CREATE TABLE test.events_by_type (foo text, bar timestamp, bar_id text, measure1 int, measure2 text) unique key (foo, bar, bar_id)").await.unwrap();
    for i in 0..2 {
        for j in 0..2 {
            service
                .exec_query(&format!("INSERT INTO test.events_by_type (foo, bar, bar_id, measure1, measure2, __seq) VALUES ('a', '2021-01-01T00:00:00.000', '{}', {}, '{}', {})", i, j, "text_value", i * 10 + j))
                .await
                .unwrap();
        }
    }
    let r = service
        .exec_query(
            "SELECT bar_id, measure1, measure2 FROM test.events_by_type as `events` LIMIT 100",
        )
        .await
        .unwrap();

    assert_eq!(
        to_rows(&r),
        rows(&[("0", 1, "text_value"), ("1", 1, "text_value")])
    );
}

async fn divide_by_zero(service: Box<dyn SqlClient>) {
    service.exec_query("CREATE SCHEMA s").await.unwrap();
    service
        .exec_query("CREATE TABLE s.t(i int, z int)")
        .await
        .unwrap();
    service
        .exec_query("INSERT INTO s.t(i, z) VALUES (1, 0), (2, 0), (3, 0)")
        .await
        .unwrap();
    let r = service
        .exec_query("SELECT i / z FROM s.t")
        .await
        .err()
        .unwrap();
    assert_eq!(
        r.elide_backtrace(),
        CubeError::internal("Execution error: Internal: Arrow error: External error: Arrow error: Divide by zero error".to_string())
    );
}

async fn panic_worker(service: Box<dyn SqlClient>) {
    let r = service.exec_query("SYS PANIC WORKER").await;
    assert_eq!(r, Err(CubeError::panic("worker panic".to_string())));
}

fn to_rows(d: &DataFrame) -> Vec<Vec<TableValue>> {
    return d
        .get_rows()
        .iter()
        .map(|r| r.values().clone())
        .collect_vec();
}

fn dec5(i: i64) -> Decimal {
    dec5f1(i, 0)
}

fn dec5f1(i: i64, f: u64) -> Decimal {
    assert!(f < 10);
    let f = if i < 0 { -(f as i64) } else { f as i64 };
    Decimal::new(i * 100_000 + 10_000 * f)
}
