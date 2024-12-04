use criterion::{criterion_group, criterion_main, Criterion};
use cubesql::compile::test::{
    get_test_tenant_ctx,
    rewrite_engine::{
        create_test_postgresql_cube_context, query_to_logical_plan, rewrite_rules, rewrite_runner,
    },
};
use itertools::Itertools;
use std::sync::Arc;

macro_rules! bench_func {
    ($NAME:expr, $QUERY:expr, $CRITERION:expr) => {{
        let context = Arc::new(
            futures::executor::block_on(create_test_postgresql_cube_context(get_test_tenant_ctx()))
                .unwrap(),
        );
        let plan = query_to_logical_plan($QUERY, &context);
        let rules = rewrite_rules(context.clone());

        $CRITERION.bench_function($NAME, |b| {
            b.iter(|| {
                let context = context.clone();
                let plan = plan.clone();
                let rules = rules.clone();

                let runner = rewrite_runner(plan, context);
                runner.run(&rules)
            })
        });
    }};
}

fn get_split_query() -> String {
    "
    SELECT
        cast(taxful_total_price as integer) a1,
        cast(taxful_total_price as text) a2,
        cast(taxful_total_price as decimal) a3,
        cast(taxful_total_price -1 as integer) a4,
        cast(taxful_total_price - 2 as integer) a5,
        date_trunc('month', order_date) a6,
        date_trunc('day', order_date) a7,
        date_trunc('year', order_date) a8,
        date_trunc('second', order_date) a9,
        date_trunc('quarter', order_date) a10,
        CAST(((((EXTRACT(YEAR FROM \"ta_1\".\"order_date\") * 100) + 1) * 100) + 10) AS varchar) a11,
        CAST(((((EXTRACT(DAY FROM \"ta_1\".\"order_date\") * 100) + 2) * 100) + 9) AS varchar) a12,
        CAST(((((EXTRACT(SECOND FROM \"ta_1\".\"order_date\") * 100) + 3) * 100) + 8) AS varchar) a13,
        CAST(((((EXTRACT(MINUTE FROM \"ta_1\".\"order_date\") * 100) + 4) * 100) + 7) AS varchar) a14,
        CAST(((((EXTRACT(YEAR FROM \"ta_1\".\"order_date\") * 100) + 5) * 100) + 6) AS varchar) a15,
        CAST(((((EXTRACT(DAY FROM \"ta_1\".\"order_date\") * 100) + 6) * 100) + 5) AS varchar) a16,
        CAST(((((EXTRACT(MONTH FROM \"ta_1\".\"order_date\") * 100) + 7) * 100) + 4) AS varchar) a17,
        CAST(((((EXTRACT(SECOND FROM \"ta_1\".\"order_date\") * 100) + 8) * 100) + 3) AS varchar) a18,
        CAST(((((EXTRACT(MONTH FROM \"ta_1\".\"order_date\") * 100) + 9) * 100) + 2) AS varchar) a19,
        CAST(((((EXTRACT(MINUTE FROM \"ta_1\".\"order_date\") * 100) + 10) * 100) + 1) AS varchar) a20,
        count(count) a21,
        count(count) a22,
        count(count) a23,
        count(count) a24,
        count(count) a25,
        max(maxPrice) a26,
        max(maxPrice) a27,
        max(maxPrice) a28,
        max(maxPrice) a29,
        max(maxPrice) a30,
        min(minPrice) a31,
        min(minPrice) a32,
        min(minPrice) a33,
        min(minPrice) a34,
        min(minPrice) a35,
        EXTRACT(MONTH FROM \"ta_1\".\"order_date\") a36,
        CAST(CAST(((((EXTRACT(YEAR FROM \"ta_1\".\"order_date\") * 100) + 1) * 100) + 1) AS varchar) AS date) a37,
        ((((EXTRACT(DAY FROM \"ta_1\".\"order_date\") * 100) + 1) * 100) + 1) a38,
        count(\"ta_1\".\"count\") a39,
        count(\"ta_1\".\"count\") a40
    FROM \"public\".\"KibanaSampleDataEcommerce\" \"ta_1\"
    GROUP BY
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 36, 37, 38".to_string()
}

pub fn split_query(c: &mut Criterion) {
    bench_func!("split_query", get_split_query(), c);
}

pub fn split_query_count_distinct(c: &mut Criterion) {
    let query = "
        SELECT
            cast(taxful_total_price as integer) a1,
            cast(taxful_total_price as text) a2,
            cast(taxful_total_price as decimal) a3,
            cast(taxful_total_price -1 as integer) a4,
            cast(taxful_total_price - 2 as integer) a5,
            date_trunc('month', order_date) a6,
            date_trunc('day', order_date) a7,
            date_trunc('year', order_date) a8,
            date_trunc('second', order_date) a9,
            date_trunc('quarter', order_date) a10,
            CAST(((((EXTRACT(YEAR FROM \"ta_1\".\"order_date\") * 100) + 1) * 100) + 10) AS varchar) a11,
            CAST(((((EXTRACT(DAY FROM \"ta_1\".\"order_date\") * 100) + 2) * 100) + 9) AS varchar) a12,
            CAST(((((EXTRACT(SECOND FROM \"ta_1\".\"order_date\") * 100) + 3) * 100) + 8) AS varchar) a13,
            CAST(((((EXTRACT(MINUTE FROM \"ta_1\".\"order_date\") * 100) + 4) * 100) + 7) AS varchar) a14,
            CAST(((((EXTRACT(YEAR FROM \"ta_1\".\"order_date\") * 100) + 5) * 100) + 6) AS varchar) a15,
            CAST(((((EXTRACT(DAY FROM \"ta_1\".\"order_date\") * 100) + 6) * 100) + 5) AS varchar) a16,
            CAST(((((EXTRACT(MONTH FROM \"ta_1\".\"order_date\") * 100) + 7) * 100) + 4) AS varchar) a17,
            CAST(((((EXTRACT(SECOND FROM \"ta_1\".\"order_date\") * 100) + 8) * 100) + 3) AS varchar) a18,
            CAST(((((EXTRACT(MONTH FROM \"ta_1\".\"order_date\") * 100) + 9) * 100) + 2) AS varchar) a19,
            CAST(((((EXTRACT(MINUTE FROM \"ta_1\".\"order_date\") * 100) + 10) * 100) + 1) AS varchar) a20,
            count(count) a21,
            count(count) a22,
            count(count) a23,
            count(count) a24,
            count(count) a25,
            max(maxPrice) a26,
            max(maxPrice) a27,
            max(maxPrice) a28,
            max(maxPrice) a29,
            max(maxPrice) a30,
            min(minPrice) a31,
            min(minPrice) a32,
            min(minPrice) a33,
            min(minPrice) a34,
            min(minPrice) a35,
            EXTRACT(MONTH FROM \"ta_1\".\"order_date\") a36,
            CAST(CAST(((((EXTRACT(YEAR FROM \"ta_1\".\"order_date\") * 100) + 1) * 100) + 1) AS varchar) AS date) a37,
            ((((EXTRACT(DAY FROM \"ta_1\".\"order_date\") * 100) + 1) * 100) + 1) a38,
            count(\"ta_1\".\"count\") a39,
            count(\"ta_1\".\"count\") a40
        FROM \"public\".\"KibanaSampleDataEcommerce\" \"ta_1\"
        GROUP BY
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 36, 37, 38".to_string();

    bench_func!("split_query_count_distinct", query, c);
}

fn get_wrapped_query() -> String {
    "
    SELECT * FROM
        (SELECT * FROM
            (SELECT
                cast(taxful_total_price as integer) a1,
                cast(taxful_total_price as text) a2,
                cast(taxful_total_price as decimal) a3,
                cast(taxful_total_price -1 as integer) a4,
                cast(taxful_total_price - 2 as integer) a5,
                date_trunc('month', order_date) a6,
                date_trunc('day', order_date) a7,
                date_trunc('year', order_date) a8,
                date_trunc('second', order_date) a9,
                date_trunc('quarter', order_date) a10,
                CAST(((((EXTRACT(YEAR FROM \"ta_1\".\"order_date\") * 100) + 1) * 100) + 10) AS varchar) a11,
                CAST(((((EXTRACT(DAY FROM \"ta_1\".\"order_date\") * 100) + 2) * 100) + 9) AS varchar) a12,
                CAST(((((EXTRACT(SECOND FROM \"ta_1\".\"order_date\") * 100) + 3) * 100) + 8) AS varchar) a13,
                CAST(((((EXTRACT(MINUTE FROM \"ta_1\".\"order_date\") * 100) + 4) * 100) + 7) AS varchar) a14,
                CAST(((((EXTRACT(YEAR FROM \"ta_1\".\"order_date\") * 100) + 5) * 100) + 6) AS varchar) a15,
                CAST(((((EXTRACT(DAY FROM \"ta_1\".\"order_date\") * 100) + 6) * 100) + 5) AS varchar) a16,
                CAST(((((EXTRACT(MONTH FROM \"ta_1\".\"order_date\") * 100) + 7) * 100) + 4) AS varchar) a17,
                CAST(((((EXTRACT(SECOND FROM \"ta_1\".\"order_date\") * 100) + 8) * 100) + 3) AS varchar) a18,
                CAST(((((EXTRACT(MONTH FROM \"ta_1\".\"order_date\") * 100) + 9) * 100) + 2) AS varchar) a19,
                CAST(((((EXTRACT(MINUTE FROM \"ta_1\".\"order_date\") * 100) + 10) * 100) + 1) AS varchar) a20,
                count(count) a21,
                count(count) a22,
                count(count) a23,
                count(count) a24,
                count(count) a25,
                max(maxPrice) a26,
                max(maxPrice) a27,
                max(maxPrice) a28,
                max(maxPrice) a29,
                max(maxPrice) a30,
                min(minPrice) a31,
                min(minPrice) a32,
                min(minPrice) a33,
                min(minPrice) a34,
                min(minPrice) a35,
                EXTRACT(MONTH FROM \"ta_1\".\"order_date\") a36,
                CAST(CAST(((((EXTRACT(YEAR FROM \"ta_1\".\"order_date\") * 100) + 1) * 100) + 1) AS varchar) AS date) a37,
                ((((EXTRACT(DAY FROM \"ta_1\".\"order_date\") * 100) + 1) * 100) + 1) a38,
                count(\"ta_1\".\"count\") a39,
                count(\"ta_1\".\"count\") a40
            FROM \"public\".\"KibanaSampleDataEcommerce\" \"ta_1\"
            GROUP BY
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 36, 37, 38) x
        WHERE a1 > 0 and a21 > 0) a
    GROUP BY
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40".to_string()
}

pub fn wrapped_query(c: &mut Criterion) {
    bench_func!("wrapped_query", get_wrapped_query(), c);
}

fn get_power_bi_wrap() -> String {
    "
    select
  \"rows\".\"dim33\" as \"dim33\",
  \"rows\".\"dim40\" as \"dim40\"
from
  (
    select
      \"_\".\"dim1\",
      \"_\".\"dim2\",
      \"_\".\"dim3\",
      \"_\".\"dim4\",
      \"_\".\"dim5\",
      \"_\".\"dim6\",
      \"_\".\"dim7\",
      \"_\".\"dim8\",
      \"_\".\"dim9\",
      \"_\".\"dim10\",
      \"_\".\"dim11\",
      \"_\".\"dim12\",
      \"_\".\"dim13\",
      \"_\".\"dim14\",
      \"_\".\"dim15\",
      \"_\".\"dim17\",
      \"_\".\"dim18\",
      \"_\".\"dim19\",
      \"_\".\"dim20\",
      \"_\".\"dim21\",
      \"_\".\"dim22\",
      \"_\".\"dim23\",
      \"_\".\"dim24\",
      \"_\".\"dim25\",
      \"_\".\"dim26\",
      \"_\".\"dim27\",
      \"_\".\"dim28\",
      \"_\".\"dim29\",
      \"_\".\"dim30\",
      \"_\".\"dim31\",
      \"_\".\"dim32\",
      \"_\".\"dim33\",
      \"_\".\"dim34\",
      \"_\".\"dim35\",
      \"_\".\"dim36\",
      \"_\".\"dim37\",
      \"_\".\"dim38\",
      \"_\".\"dim39\",
      \"_\".\"dim40\",
      \"_\".\"__user\",
      \"_\".\"__cubeJoinField\"
    from
      \"public\".\"WideCube\" \"_\"
    where
      cast(\"_\".\"dim33\" as decimal) = cast(2002 as decimal)
      and \"_\".\"dim49\" = 10
  ) \"rows\"
group by
  \"dim33\",
  \"dim40\"
limit
  1000001"
        .to_string()
}

pub fn power_bi_wrap(c: &mut Criterion) {
    bench_func!("power_bi_wrap", get_power_bi_wrap(), c);
}

fn get_power_bi_sum_wrap() -> String {
    r#"select
  "_"."dim1",
  "_"."a0",
  "_"."a1",
  "_"."a2",
  "_"."a3"
from
  (
    select
      "rows"."dim1" as "dim1",
      sum(cast("rows"."measure1" as decimal)) as "a0",
      sum(cast("rows"."measure2" as decimal)) as "a1",
      sum(
        cast("rows"."measure3" as decimal)
      ) as "a2",
      sum(cast("rows"."measure4" as decimal)) as "a3"
    from
      (
        select
          "_"."dim0",
          "_"."measure1",
          "_"."measure2",
          "_"."measure3",
          "_"."measure4",
          "_"."measure5",
          "_"."measure6",
          "_"."measure7",
          "_"."measure8",
          "_"."measure9",
          "_"."measure10",
          "_"."measure11",
          "_"."measure12",
          "_"."measure13",
          "_"."measure14",
          "_"."measure15",
          "_"."measure16",
          "_"."measure17",
          "_"."measure18",
          "_"."measure19",
          "_"."measure20",
          "_"."dim1",
          "_"."dim2",
          "_"."dim3",
          "_"."dim4",
          "_"."dim5",
          "_"."dim6",
          "_"."dim7",
          "_"."dim8",
          "_"."dim9",
          "_"."dim10",
          "_"."dim11",
          "_"."dim12",
          "_"."dim13",
          "_"."dim14",
          "_"."dim15",
          "_"."dim16",
          "_"."dim17",
          "_"."dim18",
          "_"."dim19",
          "_"."dim20",
          "_"."dim21",
          "_"."dim22",
          "_"."dim23",
          "_"."dim24",
          "_"."dim25",
          "_"."dim26",
          "_"."dim27",
          "_"."dim28",
          "_"."dim29",
          "_"."dim30",
          "_"."__user",
          "_"."__cubeJoinField"
        from
          "public"."WideCube" "_"
        where
          "_"."dim1" = 'Jewelry'
      ) "rows"
    group by
      "dim1"
  ) "_"
where
  (
    not "_"."a0" is null
    or not "_"."a1" is null
  )
  or (
    not "_"."a2" is null
    or not "_"."a3" is null
  )
limit
  1000001"#
        .to_string()
}

pub fn power_bi_sum_wrap(c: &mut Criterion) {
    bench_func!("power_bi_sum_wrap", get_power_bi_sum_wrap(), c);
}

fn get_simple_long_in_number_expr(set_size: usize) -> String {
    let set = (1..=set_size).join(", ");

    format!("SELECT * FROM NumberCube WHERE someNumber IN ({set})")
}

fn get_simple_long_in_str_expr(set_size: usize) -> String {
    let mut set = Vec::with_capacity(set_size);

    for i in 1..set_size {
        set.push(format!(
            "'SUPER LARGE RANDOM STRING TO TEST MEMORY CLONES ${i}'"
        ))
    }

    let set = set.join(", ");

    format!("SELECT * FROM KibanaSampleDataEcommerce WHERE customer_gender IN ({set})")
}

pub fn long_simple_in_number_expr_1k(c: &mut Criterion) {
    std::env::set_var("CUBESQL_SQL_PUSH_DOWN", "true");
    bench_func!(
        "long_simple_in_number_expr_1k",
        get_simple_long_in_number_expr(1000),
        c
    );
}

pub fn long_simple_in_str_expr_50(c: &mut Criterion) {
    std::env::set_var("CUBESQL_SQL_PUSH_DOWN", "true");
    bench_func!(
        "long_simple_in_str_expr_50",
        get_simple_long_in_str_expr(50),
        c
    );
}

pub fn long_simple_in_str_expr_1k(c: &mut Criterion) {
    std::env::set_var("CUBESQL_SQL_PUSH_DOWN", "true");
    bench_func!(
        "long_simple_in_str_expr_1k",
        get_simple_long_in_str_expr(1000),
        c
    );
}

fn get_long_in_expr() -> String {
    r#"
    SELECT
        "WideCube"."dim1" as "column1",
        "WideCube"."dim2" as "column2",
        "WideCube"."dim3" as "column3",
        "WideCube"."dim4" as "column4",
        "WideCube"."dim5" as "column5",
        "WideCube"."dim6" as "column6",
        "WideCube"."dim7" as "column7",
        "WideCube"."dim8" as "column8",
        "WideCube"."dim9" as "column9",
        "WideCube"."dim10" as "column10",
        "WideCube"."dim11" as "column11",
        "WideCube"."dim12" as "column12",
        "WideCube"."dim13" as "column13",
        "WideCube"."dim14" as "column14",
        "WideCube"."dim15" as "column15",
        SUM("WideCube"."dim16") as "some_sum"
    FROM
        "WideCube"
    WHERE
        "WideCube"."dim1" = 1
        AND "WideCube"."dim2" = 2
        AND "WideCube"."dim3" = 3
        AND "WideCube"."dim4" = 4
        AND "WideCube"."dim5" = 5
        AND "WideCube"."dim6" = 6
        AND "WideCube"."dim7" = 7
        AND "WideCube"."dim8" = 8
        AND "WideCube"."dim9" = 9
        AND "WideCube"."dim10" = 10
        AND ("WideCube"."dim11" = 42 OR "WideCube"."dim11" IS NULL)
        AND (
            "WideCube"."dim12" IN (
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
                27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50
            ) OR "WideCube"."dim12" IS NULL
        ) AND "WideCube"."dim20" = 55
    GROUP BY
        "WideCube"."dim1",
        "WideCube"."dim2",
        "WideCube"."dim3",
        "WideCube"."dim4",
        "WideCube"."dim5",
        "WideCube"."dim6",
        "WideCube"."dim7",
        "WideCube"."dim8",
        "WideCube"."dim9",
        "WideCube"."dim10",
        "WideCube"."dim11",
        "WideCube"."dim12",
        "WideCube"."dim13",
        "WideCube"."dim14",
        "WideCube"."dim15"
  "#.into()
}

pub fn long_in_expr(c: &mut Criterion) {
    std::env::set_var("CUBESQL_SQL_PUSH_DOWN", "true");
    bench_func!("long_in_expr", get_long_in_expr(), c);
}

fn get_tableau_logical_17_query() -> String {
    r#"
    SELECT LOWER(CAST("KibanaSampleDataEcommerce"."customer_gender" AS TEXT)) AS "TEMP(Test)(1234567890)(0)"
    FROM "public"."KibanaSampleDataEcommerce" "KibanaSampleDataEcommerce"
    GROUP BY 1
    "#
    .into()
}

pub fn tableau_logical_17(c: &mut Criterion) {
    std::env::set_var("CUBESQL_SQL_PUSH_DOWN", "true");
    bench_func!("tableau_logical_17", get_tableau_logical_17_query(), c);
}

fn get_ts_last_day_redshift_query() -> String {
    r#"
    WITH "qt_0" AS (
        SELECT
            DATE_TRUNC('month', "ta_1"."order_date") "ca_1",
            CASE
                WHEN sum("ta_1"."sumPrice") IS NOT NULL THEN sum("ta_1"."sumPrice")
                ELSE 0
            END "ca_2"
        FROM "db"."public"."KibanaSampleDataEcommerce" "ta_1"
        WHERE (
            "ta_1"."order_date" >= DATE '1999-12-29'
            AND "ta_1"."order_date" < DATE '1999-12-30'
        )
        GROUP BY "ca_1"
    )
    SELECT
        min("ta_2"."ca_1") "ca_3",
        max("ta_2"."ca_1") "ca_4"
    FROM "qt_0" "ta_2"
    "#
    .into()
}

pub fn ts_last_day_redshift(c: &mut Criterion) {
    std::env::set_var("CUBESQL_SQL_PUSH_DOWN", "true");
    bench_func!("ts_last_day_redshift", get_ts_last_day_redshift_query(), c);
}

fn get_tableau_bugs_b8888_query() -> String {
    r#"
    SELECT CAST(TRUNC((CASE WHEN 7 = 0 THEN NULL ELSE CAST(((6 + (1 + CAST(EXTRACT(DOW FROM (DATE_TRUNC( 'YEAR', CAST("KibanaSampleDataEcommerce"."order_date" AS TIMESTAMP) ) + (CASE WHEN (CAST(TRUNC(EXTRACT(MONTH FROM "KibanaSampleDataEcommerce"."order_date")) AS INTEGER) < 3) THEN -10 ELSE 2 END) * INTERVAL '1 MONTH')) AS INTEGER))) + (EXTRACT(EPOCH FROM (CAST("KibanaSampleDataEcommerce"."order_date" AS TIMESTAMP) - (DATE_TRUNC( 'YEAR', CAST("KibanaSampleDataEcommerce"."order_date" AS TIMESTAMP) ) + (CASE WHEN (CAST(TRUNC(EXTRACT(MONTH FROM "KibanaSampleDataEcommerce"."order_date")) AS INTEGER) < 3) THEN -10 ELSE 2 END) * INTERVAL '1 MONTH'))) / (60.0 * 60 * 24))) AS DOUBLE PRECISION) / 7 END)) AS BIGINT) AS "Week #",
        COUNT(DISTINCT "KibanaSampleDataEcommerce"."order_date") AS "ctd:order_date:ok",
        CAST(TRUNC(EXTRACT(YEAR FROM ("KibanaSampleDataEcommerce"."order_date" + 10 * INTERVAL '1 MONTH'))) AS INTEGER) AS "yr:order_date:ok"
    FROM "public"."KibanaSampleDataEcommerce" "KibanaSampleDataEcommerce"
    GROUP BY 1,
        3
    "#.into()
}

pub fn tableau_bugs_b8888(c: &mut Criterion) {
    std::env::set_var("CUBESQL_SQL_PUSH_DOWN", "true");
    bench_func!("tableau_bugs_b8888", get_tableau_bugs_b8888_query(), c);
}

fn get_quicksight_1_query() -> String {
    r#"
    SELECT
    "LocalTemp.dim_date1_tg",
    "LocalTemp.dim_str1",
    "something_2",
    "LocalTemp.measure_num1_sum",
    "$otherbucket_group_count",
    "count"
    FROM
    (
        SELECT
        "LocalTemp.dim_date1_tg",
        "$VAL_1",
        CASE
            WHEN "$VAL_2" > 25 THEN NULL
            ELSE "LocalTemp.dim_str1"
        END AS "LocalTemp.dim_str1",
        CASE
            WHEN "$VAL_2" > 25 THEN NULL
            ELSE "$VAL_2"
        END AS "$f7",
        CASE
            WHEN "$VAL_2" > 25 THEN 1
            ELSE 0
        END AS "something_2",
        SUM(
            "LocalTemp.measure_num1_sum"
        ) AS "LocalTemp.measure_num1_sum",
        COUNT(*) AS "$otherbucket_group_count",
        SUM("count") AS "count"
        FROM
        (
            SELECT
            "dim_str1" AS "LocalTemp.dim_str1",
            date_trunc('day', "dim_date1") AS "LocalTemp.dim_date1_tg",
            COUNT(*) AS "count",
            SUM("measure_num1") AS "LocalTemp.measure_num1_sum",
            DENSE_RANK() OVER (
                ORDER BY
                date_trunc('day', "dim_date1") DESC NULLS LAST
            ) AS "$VAL_1",
            DENSE_RANK() OVER (
                PARTITION BY date_trunc('day', "dim_date1")
                ORDER BY
                "dim_str1" NULLS FIRST
            ) AS "$VAL_2"
            FROM
            "public"."MultiTypeCube"
            WHERE
            (
                "dim_str2" NOT IN ('alpha', 'beta', 'gamma', 'delta')
                OR "dim_str2" IS NULL
                OR "dim_str2" IS NULL
            )
            AND "dim_str1" = 'Lima Lima Uniform'
            AND "dim_date1" >= date_trunc(
                'day',
                TO_TIMESTAMP('2022-04-17 00:00:00', 'yyyy-MM-dd HH24:mi:ss')
            )
            AND "dim_date1" < date_trunc(
                'day',
                TO_TIMESTAMP('2022-06-05 00:00:00', 'yyyy-MM-dd HH24:mi:ss')
            ) + 1 * interval '1 DAY'
            GROUP BY
            "dim_str1",
            date_trunc('day', "dim_date1")
        ) AS "t"
        WHERE
        "$VAL_1" <= 200
        GROUP BY
        "LocalTemp.dim_date1_tg",
        "$VAL_1",
        CASE
            WHEN "$VAL_2" > 25 THEN NULL
            ELSE "LocalTemp.dim_str1"
        END,
        CASE
            WHEN "$VAL_2" > 25 THEN NULL
            ELSE "$VAL_2"
        END,
        CASE
            WHEN "$VAL_2" > 25 THEN 1
            ELSE 0
        END
        ORDER BY
        "$VAL_1" NULLS FIRST,
        CASE
            WHEN "$VAL_2" > 25 THEN NULL
            ELSE "$VAL_2"
        END NULLS FIRST
    ) AS "t0"
   "#
    .into()
}

fn quicksight_1(c: &mut Criterion) {
    std::env::set_var("CUBESQL_SQL_PUSH_DOWN", "true");
    bench_func!("quicksight_1", get_quicksight_1_query(), c);
}

fn get_quicksight_2_query() -> String {
    r#"
SELECT
  "Temp-A",
  "Foo.dim_str5",
  "something_2",
  "sumof_sum_num2",
  "$count_of_groups",
  "count"
FROM
  (
    SELECT
      "Temp-A",
      "$VAL_1",
      CASE
        WHEN "$VAL_2" > 25 THEN NULL
        ELSE "Foo.dim_str5"
      END AS "Foo.dim_str5",
      CASE
        WHEN "$VAL_2" > 25 THEN NULL
        ELSE "$VAL_2"
      END AS "$f7",
      CASE
        WHEN "$VAL_2" > 25 THEN 1
        ELSE 0
      END AS "something_2",
      SUM("sum_num2") AS "sumof_sum_num2",
      COUNT(*) AS "$count_of_groups",
      SUM("count") AS "count"
    FROM
      (
        SELECT
          "dim_str5" AS "Foo.dim_str5",
          date_trunc('day', "dim_date1") AS "Temp-A",
          COUNT(*) AS "count",
          SUM("measure_num2") AS "sum_num2",
          DENSE_RANK() OVER (
            ORDER BY
              date_trunc('day', "dim_date1") DESC NULLS LAST
          ) AS "$VAL_1",
          DENSE_RANK() OVER (
            PARTITION BY date_trunc('day', "dim_date1")
            ORDER BY
              "dim_str5" NULLS FIRST
          ) AS "$VAL_2"
        FROM
          "public"."MultiTypeCube"
        WHERE
          "dim_str1" IN (
            '$0',
            '$0 - $500',
            '$1000 - $5K',
            '$100K+',
            '$10K - $25K',
            '$25K - $50K',
            '$500 - $1K',
            '$50K - $100K',
            '$5K - $10K',
            'Credit'
          )
          AND "dim_str2" IN ('Open')
          AND CAST(
            "dim_num1" AS INTEGER
          ) IN (0)
          AND "dim_str3" <> '0002146'
          AND "dim_str3" IS NOT NULL
          AND "dim_str4" = 'Tango Golf Heaviside'
          AND "dim_date1" >= date_trunc(
            'day',
            TO_TIMESTAMP('2024-01-01 00:00:00', 'yyyy-MM-dd HH24:mi:ss')
          )
          AND "dim_date1" < date_trunc(
            'day',
            TO_TIMESTAMP('2024-07-05 00:00:00', 'yyyy-MM-dd HH24:mi:ss')
          ) + 1 * interval '1 DAY'
        GROUP BY
          "dim_str5",
          date_trunc('day', "dim_date1")
      ) AS "t"
    WHERE
      "$VAL_1" <= 200
    GROUP BY
      "Temp-A",
      "$VAL_1",
      CASE
        WHEN "$VAL_2" > 25 THEN NULL
        ELSE "Foo.dim_str5"
      END,
      CASE
        WHEN "$VAL_2" > 25 THEN NULL
        ELSE "$VAL_2"
      END,
      CASE
        WHEN "$VAL_2" > 25 THEN 1
        ELSE 0
      END
    ORDER BY
      "$VAL_1" NULLS FIRST,
      CASE
        WHEN "$VAL_2" > 25 THEN NULL
        ELSE "$VAL_2"
      END NULLS FIRST
  ) AS "t0"
   "#
    .into()
}

fn quicksight_2(c: &mut Criterion) {
    std::env::set_var("CUBESQL_SQL_PUSH_DOWN", "true");
    bench_func!("quicksight_2", get_quicksight_2_query(), c);
}

criterion_group! {
    name = benches;
    config = Criterion::default().measurement_time(std::time::Duration::from_secs(15)).sample_size(10);
    targets = split_query, split_query_count_distinct, wrapped_query, power_bi_wrap, power_bi_sum_wrap, long_in_expr, long_simple_in_number_expr_1k, long_simple_in_str_expr_50, long_simple_in_str_expr_1k, tableau_logical_17,
        tableau_bugs_b8888, ts_last_day_redshift, quicksight_1, quicksight_2
}

fn simple_rules_loading(c: &mut Criterion) {
    let context = Arc::new(
        futures::executor::block_on(create_test_postgresql_cube_context(get_test_tenant_ctx()))
            .unwrap(),
    );
    // preload rules at least once
    let _rules = rewrite_rules(context.clone());

    c.bench_function("simple_rules_loading", |b| {
        b.iter(|| {
            rewrite_rules(context.clone());
        })
    });
}

criterion_group! {
    name = rules_loading;
    config = Criterion::default();
    targets = simple_rules_loading
}

criterion_main!(benches, rules_loading);
