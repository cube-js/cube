use criterion::{criterion_group, criterion_main, Criterion};
use cubesql::compile::test::rewrite_engine::{
    cube_context, query_to_logical_plan, rewrite_rules, rewrite_runner,
};
use std::sync::Arc;

macro_rules! bench_func {
    ($NAME:expr, $QUERY:expr, $CRITERION:expr) => {{
        let context = Arc::new(futures::executor::block_on(cube_context()));
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

criterion_group! {
    name = benches;
    config = Criterion::default().measurement_time(std::time::Duration::from_secs(30)).sample_size(10);
    targets = split_query, split_query_count_distinct, wrapped_query, power_bi_wrap, power_bi_sum_wrap
}
criterion_main!(benches);
