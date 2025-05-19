use pretty_assertions::assert_eq;

use crate::{
    compile::{
        test::{execute_query, init_testing_logger},
        DatabaseProtocol,
    },
    CubeError,
};

#[tokio::test]
async fn test_instr() -> Result<(), CubeError> {
    assert_eq!(
        execute_query(
            "select \
                    instr('rust is killing me', 'r') as r1,
                    instr('rust is killing me', 'e') as r2,
                    instr('Rust is killing me', 'unknown') as r3;
                "
            .to_string(),
            DatabaseProtocol::MySQL
        )
        .await?,
        "+----+----+----+\n\
            | r1 | r2 | r3 |\n\
            +----+----+----+\n\
            | 1  | 18 | 0  |\n\
            +----+----+----+"
    );

    Ok(())
}

#[tokio::test]
async fn test_timediff() -> Result<(), CubeError> {
    assert_eq!(
        execute_query(
            "select \
                    timediff('1994-11-26T13:25:00.000Z'::timestamp, '1994-11-26T13:25:00.000Z'::timestamp) as r1
                ".to_string(), DatabaseProtocol::MySQL
        )
            .await?,
        "+------------------------------------------------+\n\
            | r1                                             |\n\
            +------------------------------------------------+\n\
            | 0 years 0 mons 0 days 0 hours 0 mins 0.00 secs |\n\
            +------------------------------------------------+"
    );

    Ok(())
}

#[tokio::test]
async fn test_ends_with() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "ends_with",
        execute_query(
            "select \
                    ends_with('rust is killing me', 'me') as r1,
                    ends_with('rust is killing me', 'no') as r2
                "
            .to_string(),
            DatabaseProtocol::MySQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_locate() -> Result<(), CubeError> {
    assert_eq!(
        execute_query(
            "select \
                    locate('r', 'rust is killing me') as r1,
                    locate('e', 'rust is killing me') as r2,
                    locate('unknown', 'Rust is killing me') as r3
                "
            .to_string(),
            DatabaseProtocol::MySQL
        )
        .await?,
        "+----+----+----+\n\
            | r1 | r2 | r3 |\n\
            +----+----+----+\n\
            | 1  | 18 | 0  |\n\
            +----+----+----+"
    );

    Ok(())
}

#[tokio::test]
async fn test_if() -> Result<(), CubeError> {
    assert_eq!(
        execute_query(
            r#"select
                if(null, true, false) as r1,
                if(true, false, true) as r2,
                if(true, 'true', 'false') as r3,
                if(true, CAST(1 as int), CAST(2 as bigint)) as c1,
                if(false, CAST(1 as int), CAST(2 as bigint)) as c2,
                if(true, CAST(1 as bigint), CAST(2 as int)) as c3
            "#
            .to_string(),
            DatabaseProtocol::MySQL
        )
        .await?,
        "+-------+-------+------+----+----+----+\n\
            | r1    | r2    | r3   | c1 | c2 | c3 |\n\
            +-------+-------+------+----+----+----+\n\
            | false | false | true | 1  | 2  | 1  |\n\
            +-------+-------+------+----+----+----+"
    );

    Ok(())
}

#[tokio::test]
async fn test_least_single_row() -> Result<(), CubeError> {
    assert_eq!(
        execute_query(
            "select \
                least(100) as r0, \
                least(1, 2) as r1, \
                least(2, 1) as r2, \
                least(1.5, 2) as r3, \
                least(2, 1.5) as r4, \
                least(2.5, 2) as r5, \
                least(2, 2.5) as r6, \
                least(null, 1.5) as r7, \
                least(-1.23, 3.44, 50) as r8, \
                least(-1.23, 3.44, 4, 10, null, -5) as r9, \
                least(null, null, null) as r10
            "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?,
        "+-----+----+----+-----+-----+----+----+-----+-------+----+------+\n\
            | r0  | r1 | r2 | r3  | r4  | r5 | r6 | r7  | r8    | r9 | r10  |\n\
            +-----+----+----+-----+-----+----+----+-----+-------+----+------+\n\
            | 100 | 1  | 1  | 1.5 | 1.5 | 2  | 2  | 1.5 | -1.23 | -5 | NULL |\n\
            +-----+----+----+-----+-----+----+----+-----+-------+----+------+"
    );

    Ok(())
}

#[tokio::test]
async fn test_least_table() -> Result<(), CubeError> {
    assert_eq!(
        execute_query(
            "select \
                least(t.a, t.b, t.c) as r1 FROM ( \
                    SELECT 1 as a, 2 as b, 3 as c \
                        UNION ALL \
                    SELECT 2, 1.5, null \
                        UNION ALL \
                    SELECT 0.72, -3.14, 25.5 \
                        UNION ALL \
                    SELECT 3.14159, -5.72, -25 \
                        UNION ALL \
                    SELECT null as a, null as b, null as c \
                ) as t
            "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?,
        "+-------+\n\
            | r1    |\n\
            +-------+\n\
            | 1     |\n\
            | 1.5   |\n\
            | -3.14 |\n\
            | -25   |\n\
            | NULL  |\n\
            +-------+"
    );

    Ok(())
}

#[tokio::test]
async fn test_greatest_single_row() -> Result<(), CubeError> {
    assert_eq!(
        execute_query(
            "select \
                greatest(100) as r0, \
                greatest(1, 2) as r1, \
                greatest(2, 3) as r2, \
                greatest(1, 2.5) as r3, \
                greatest(3.2, 1) as r4, \
                greatest(2.5, 4) as r5, \
                greatest(5, 2.5) as r6, \
                greatest(null, 1.5) as r7, \
                greatest(-1.23, -3.44) as r8, \
                greatest(1, 2.0, null, 25) as r9, \
                greatest(null, 1.5, null, 2.7, null, 3.1, null, -5, null, 10) as r10, \
                greatest(null, null, null) as r11
            "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?,
        "+-----+----+----+-----+-----+----+----+-----+-------+----+-----+------+\n\
            | r0  | r1 | r2 | r3  | r4  | r5 | r6 | r7  | r8    | r9 | r10 | r11  |\n\
            +-----+----+----+-----+-----+----+----+-----+-------+----+-----+------+\n\
            | 100 | 2  | 3  | 2.5 | 3.2 | 4  | 5  | 1.5 | -1.23 | 25 | 10  | NULL |\n\
            +-----+----+----+-----+-----+----+----+-----+-------+----+-----+------+"
    );

    Ok(())
}

#[tokio::test]
async fn test_greatest_table() -> Result<(), CubeError> {
    assert_eq!(
        execute_query(
            "select \
                greatest(t.a, t.b, t.c) as r1 FROM ( \
                    SELECT 1 as a, 2 as b, 3 as c \
                        UNION ALL \
                    SELECT 1, 2.0, null \
                        UNION ALL \
                    SELECT -3.14, .72, 25.5 \
                        UNION ALL \
                    SELECT -3.14, -5.72, -25 \
                        UNION ALL \
                    SELECT null as a, null as b, null as c \
                ) as t
            "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?,
        "+-------+\n\
            | r1    |\n\
            +-------+\n\
            | 3     |\n\
            | 2     |\n\
            | 25.5  |\n\
            | -3.14 |\n\
            | NULL  |\n\
            +-------+"
    );

    Ok(())
}

#[tokio::test]
async fn test_ucase() -> Result<(), CubeError> {
    assert_eq!(
        execute_query(
            "select \
                ucase('super stroka') as r1
            "
            .to_string(),
            DatabaseProtocol::MySQL
        )
        .await?,
        "+--------------+\n\
            | r1           |\n\
            +--------------+\n\
            | SUPER STROKA |\n\
            +--------------+"
    );

    Ok(())
}

#[tokio::test]
async fn test_convert_tz() -> Result<(), CubeError> {
    assert_eq!(
        execute_query(
            "select convert_tz('2021-12-08T15:50:14.337Z'::timestamp, @@GLOBAL.time_zone, '+00:00') as r1;".to_string(), DatabaseProtocol::MySQL
        )
            .await?,
        "+-------------------------+\n\
            | r1                      |\n\
            +-------------------------+\n\
            | 2021-12-08T15:50:14.337 |\n\
            +-------------------------+"
    );

    Ok(())
}

#[tokio::test]
async fn test_pg_backend_pid() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "pg_backend_pid",
        execute_query(
            "select pg_backend_pid();".to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_to_char_udf() -> Result<(), CubeError> {
    insta::assert_snapshot!(
            "to_char_1",
            execute_query(
                "SELECT to_char(x, 'YYYY-MM-DD HH24:MI:SS.MS TZ') FROM (SELECT Str_to_date('2021-08-31 11:05:10.400000', '%Y-%m-%d %H:%i:%s.%f') x) e".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

    insta::assert_snapshot!(
        "to_char_2",
        execute_query(
            "
                SELECT to_char(x, 'YYYY-MM-DD HH24:MI:SS.MS TZ')
                FROM  (
                        SELECT Str_to_date('2021-08-31 11:05:10.400000', '%Y-%m-%d %H:%i:%s.%f') x
                    UNION ALL
                        SELECT str_to_date('2021-08-31 11:05', '%Y-%m-%d %H:%i') x
                ) e
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    insta::assert_snapshot!(
        "to_char_3",
        execute_query(
            "
                SELECT TO_CHAR(CAST(NULL AS TIMESTAMP), 'FMDay')
                UNION ALL
                SELECT TO_CHAR(CAST('2024-01-01 00:00:00' AS TIMESTAMP), 'FMDay')
                UNION ALL
                SELECT TO_CHAR(CAST('2024-01-02 00:00:00' AS TIMESTAMP), 'FMDay')
                UNION ALL
                SELECT TO_CHAR(CAST('2024-01-07 00:00:00' AS TIMESTAMP), 'FMDay')
                UNION ALL
                SELECT TO_CHAR(CAST('2024-01-01 00:00:00' AS TIMESTAMP), 'Day')
                UNION ALL
                SELECT TO_CHAR(CAST('2024-01-02 00:00:00' AS TIMESTAMP), 'Day')
                UNION ALL
                SELECT TO_CHAR(CAST('2024-01-07 00:00:00' AS TIMESTAMP), 'Day')
                UNION ALL
                SELECT TO_CHAR(CAST('2024-01-01 00:00:00' AS TIMESTAMP), 'FMMonth')
                UNION ALL
                SELECT TO_CHAR(CAST('2024-03-01 00:00:00' AS TIMESTAMP), 'FMMonth')
                UNION ALL
                SELECT TO_CHAR(CAST('2024-12-01 00:00:00' AS TIMESTAMP), 'FMMonth')
                UNION ALL
                SELECT TO_CHAR(CAST('2024-01-01 00:00:00' AS TIMESTAMP), 'Month')
                UNION ALL
                SELECT TO_CHAR(CAST('2024-03-01 00:00:00' AS TIMESTAMP), 'Month')
                UNION ALL
                SELECT TO_CHAR(CAST('2024-12-01 00:00:00' AS TIMESTAMP), 'Month')
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_regexp_substr_udf() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "regexp_substr",
        execute_query(
            "SELECT
                    regexp_substr('test@test.com', '@[^.]*') as match_dot,
                    regexp_substr('12345', '[0-9]+') as match_number,
                    regexp_substr('12345', '[0-9]+', 2) as match_number_pos_2,
                    regexp_substr(null, '@[^.]*') as source_null,
                    regexp_substr('test@test.com', null) as pattern_null,
                    regexp_substr('test@test.com', '@[^.]*', 1) as position_default,
                    regexp_substr('test@test.com', '@[^.]*', 5) as position_no_skip,
                    regexp_substr('test@test.com', '@[^.]*', 6) as position_skip,
                    regexp_substr('test@test.com', '@[^.]*', 0) as position_zero,
                    regexp_substr('test@test.com', '@[^.]*', -1) as position_negative,
                    regexp_substr('test@test.com', '@[^.]*', 100) as position_more_then_input
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    insta::assert_snapshot!(
        "regexp_substr_column",
        execute_query(
            "SELECT r.a as input, regexp_substr(r.a, '@[^.]*') as result FROM (
                    SELECT 'test@test.com' as a
                    UNION ALL
                    SELECT 'test'
                ) as r
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_metabase_to_char_query() -> Result<(), CubeError> {
    execute_query(
        "select to_char(current_timestamp, 'YYYY-MM-DD HH24:MI:SS.MS TZ')".to_string(),
        DatabaseProtocol::PostgreSQL,
    )
    .await?;

    Ok(())
}

#[tokio::test]
async fn test_quote_ident() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "quote_ident",
        execute_query(
            "SELECT quote_ident('pg_catalog') i1, quote_ident('Foo bar') i2".to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_current_setting() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "current_setting",
        execute_query(
            "SELECT current_setting('max_index_keys'), current_setting('search_path')".to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_bool_and_or() -> Result<(), CubeError> {
    insta::assert_snapshot!(
            "test_bool_and_or",
            execute_query(
                "
                SELECT
                    bool_and(ttt) and_ttt, bool_or(ttt) or_ttt,
                    bool_and(ttf) and_ttf, bool_or(ttf) or_ttf,
                    bool_and(fff) and_fff, bool_or(fff) or_fff,
                    bool_and(ttn) and_ttn, bool_or(ttn) or_ttn,
                    bool_and(tfn) and_tfn, bool_or(tfn) or_tfn,
                    bool_and(ffn) and_ffn, bool_or(ffn) or_ffn,
                    bool_and(nnn) and_nnn, bool_or(nnn) or_nnn
                FROM (
                    SELECT true ttt, true  ttf, false fff, true ttn, true  tfn, false ffn, null::bool nnn
                    UNION ALL
                    SELECT true ttt, true  ttf, false fff, true ttn, false tfn, false ffn, null       nnn
                    UNION ALL
                    SELECT true ttt, false ttf, false fff, null ttn, null  tfn, null  ffn, null       nnn
                ) tbl
                "
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

    Ok(())
}

#[tokio::test]
async fn test_pi() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "pi",
        execute_query(
            "SELECT PI() AS PI".to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_current_schemas_postgres() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "current_schemas_postgres",
        execute_query(
            "SELECT current_schemas(false)".to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    insta::assert_snapshot!(
        "current_schemas_including_implicit_postgres",
        execute_query(
            "SELECT current_schemas(true)".to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_format_type_postgres() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "format_type",
        execute_query(
            "
                SELECT
                    t.oid,
                    t.typname,
                    format_type(t.oid, 20) ft20,
                    format_type(t.oid, 5) ft5,
                    format_type(t.oid, 4) ft4,
                    format_type(t.oid, 0) ft0,
                    format_type(t.oid, -1) ftneg,
                    format_type(t.oid, NULL::bigint) ftnull,
                    format_type(cast(t.oid as text), '5') ftstr
                FROM pg_catalog.pg_type t
                ORDER BY t.oid ASC
                ;
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_pg_datetime_precision_postgres() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "pg_datetime_precision_simple",
        execute_query(
            "SELECT information_schema._pg_datetime_precision(1184, 3) p".to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    insta::assert_snapshot!(
        "pg_datetime_precision_types",
        execute_query(
            "
                SELECT t.oid, information_schema._pg_datetime_precision(t.oid, 3) p
                FROM pg_catalog.pg_type t
                ORDER BY t.oid ASC;
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_pg_numeric_precision_postgres() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "pg_numeric_precision_simple",
        execute_query(
            "SELECT information_schema._pg_numeric_precision(1700, 3);".to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    insta::assert_snapshot!(
        "pg_numeric_precision_types",
        execute_query(
            "
                SELECT t.oid, information_schema._pg_numeric_precision(t.oid, 3) p
                FROM pg_catalog.pg_type t
                ORDER BY t.oid ASC;
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_pg_numeric_scale_postgres() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "pg_numeric_scale_simple",
        execute_query(
            "SELECT information_schema._pg_numeric_scale(1700, 50);".to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    insta::assert_snapshot!(
        "pg_numeric_scale_types",
        execute_query(
            "
                SELECT t.oid, information_schema._pg_numeric_scale(t.oid, 10) s
                FROM pg_catalog.pg_type t
                ORDER BY t.oid ASC;
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
#[cfg(debug_assertions)]
async fn test_pg_get_userbyid_postgres() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "pg_get_userbyid",
        execute_query(
            "
                SELECT pg_get_userbyid(t.id)
                FROM information_schema.testing_dataset t
                WHERE t.id < 15;
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_unnest_postgres() -> Result<(), CubeError> {
    insta::assert_snapshot!(
            "unnest_i64_from_table",
            execute_query(
                "SELECT unnest(r.a) FROM (SELECT ARRAY[1,2,3,4] as a UNION ALL SELECT ARRAY[5,6,7,8] as a) as r;".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

    insta::assert_snapshot!(
            "unnest_str_from_table",
            execute_query(
                "SELECT unnest(r.a) FROM (SELECT ARRAY['1', '2'] as a UNION ALL SELECT ARRAY['3', '4'] as a) as r;".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

    insta::assert_snapshot!(
        "unnest_i64_scalar",
        execute_query(
            "SELECT unnest(ARRAY[1,2,3,4,5]);".to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_generate_series_postgres() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "generate_series_i64_1",
        execute_query(
            "SELECT generate_series(-5, 5);".to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    insta::assert_snapshot!(
        "generate_series_f64_2",
        execute_query(
            "SELECT generate_series(-5, 5, 3);".to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    insta::assert_snapshot!(
        "generate_series_f64_1",
        execute_query(
            "SELECT generate_series(-5, 5, 0.5);".to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    insta::assert_snapshot!(
        "generate_series_empty_1",
        execute_query(
            "SELECT generate_series(-5, -10, 3);".to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    insta::assert_snapshot!(
        "generate_series_empty_2",
        execute_query(
            "SELECT generate_series(1, 5, 0);".to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    insta::assert_snapshot!(
        "generate_series_date32_2_args",
        execute_query(
            "SELECT generate_series('2024-07-23'::date, '2024-07-28'::date);".to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    insta::assert_snapshot!(
        "generate_series_date32_3_args_2days_interval",
        execute_query(
            "SELECT generate_series('2024-07-23'::date, '2024-07-28'::date, '2 days'::interval);"
                .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    insta::assert_snapshot!(
        "generate_series_date32_3_args_3years_interval",
        execute_query(
            "SELECT generate_series('2016-07-23'::date, '2024-07-28'::date, '3 years'::interval);"
                .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    insta::assert_snapshot!(
            "generate_series_timestamp_2_args",
            execute_query(
                "SELECT generate_series('2024-07-23 00:00:00'::timestamp, '2024-07-28 00:00:00'::timestamp);".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

    insta::assert_snapshot!(
            "generate_series_timestamp_3_args_2years_interval",
            execute_query(
                "SELECT generate_series('2014-07-23 00:00:00'::timestamp, '2024-10-28 00:00:00'::timestamp, '2 years'::interval);".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

    insta::assert_snapshot!(
            "generate_series_timestamp_3_args_2months_interval",
            execute_query(
                "SELECT generate_series('2024-07-23 00:00:00'::timestamp, '2024-10-28 00:00:00'::timestamp, '2 months'::interval);".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

    insta::assert_snapshot!(
            "generate_series_timestamp_3_args_2days_interval",
            execute_query(
                "SELECT generate_series('2024-07-23 00:00:00'::timestamp, '2024-07-28 00:00:00'::timestamp, '2 days'::interval);".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

    insta::assert_snapshot!(
            "generate_series_timestamp_3_args_1h_30m_interval",
            execute_query(
                "SELECT generate_series('2024-07-25 00:00:00'::timestamp, '2024-07-25 12:00:00'::timestamp, '1 hours 30 minutes'::interval);".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

    insta::assert_snapshot!(
            "generate_series_timestamp_3_args_20s_interval",
            execute_query(
                "SELECT generate_series('2024-07-25 00:00:00'::timestamp, '2024-07-25 00:01:30'::timestamp, '20 seconds'::interval);".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

    insta::assert_snapshot!(
            "generate_series_timestamp_3_args_6y_5m_4d_3h_2min_1s_interval",
            execute_query(
                "SELECT generate_series('2010-01-01 00:00:00'::timestamp, '2024-07-25 00:01:30'::timestamp, '6 years 5 months 4 days 3 hours 2 minutes 1 seconds'::interval);".to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

    insta::assert_snapshot!(
        "pg_catalog_generate_series_i64",
        execute_query(
            "SELECT pg_catalog.generate_series(1, 5);".to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    insta::assert_snapshot!(
        "generate_series_from_table",
        execute_query(
            "select generate_series(1, oid) from pg_catalog.pg_type where oid in (16,17);"
                .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_generate_subscripts_postgres() -> Result<(), CubeError> {
    insta::assert_snapshot!(
            "pg_generate_subscripts_1",
            execute_query(
                "SELECT generate_subscripts(r.a, 1) FROM (SELECT ARRAY[1,2,3] as a UNION ALL SELECT ARRAY[3,4,5]) as r;"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

    insta::assert_snapshot!(
            "pg_generate_subscripts_2_forward",
            execute_query(
                "SELECT generate_subscripts(r.a, 1, false) FROM (SELECT ARRAY[1,2,3] as a UNION ALL SELECT ARRAY[3,4,5]) as r;"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

    insta::assert_snapshot!(
            "pg_generate_subscripts_2_reverse",
            execute_query(
                "SELECT generate_subscripts(r.a, 1, true) FROM (SELECT ARRAY[1,2,3] as a UNION ALL SELECT ARRAY[3,4,5]) as r;"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

    insta::assert_snapshot!(
            "pg_generate_subscripts_3",
            execute_query(
                "SELECT generate_subscripts(r.a, 2) FROM (SELECT ARRAY[1,2,3] as a UNION ALL SELECT ARRAY[3,4,5]) as r;"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

    Ok(())
}

#[tokio::test]
async fn test_pg_expandarray_postgres() -> Result<(), CubeError> {
    insta::assert_snapshot!(
            "pg_expandarray_value",
            execute_query(
                "SELECT (information_schema._pg_expandarray(t.a)).x FROM pg_catalog.pg_class c, (SELECT ARRAY[5, 10, 15] a) t;"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

    insta::assert_snapshot!(
            "pg_expandarray_index",
            execute_query(
                "SELECT (information_schema._pg_expandarray(t.a)).n FROM pg_catalog.pg_class c, (SELECT ARRAY[5, 10, 15] a) t;"
                    .to_string(),
                DatabaseProtocol::PostgreSQL
            )
            .await?
        );

    Ok(())
}

#[tokio::test]
async fn test_pg_type_is_visible_postgres() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "pg_type_is_visible",
        execute_query(
            "
                SELECT t.oid, t.typname, n.nspname, pg_catalog.pg_type_is_visible(t.oid) is_visible
                FROM pg_catalog.pg_type t, pg_catalog.pg_namespace n
                WHERE t.typnamespace = n.oid
                ORDER BY t.oid ASC;
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_pg_get_constraintdef_postgres() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "pg_get_constraintdef_1",
        execute_query(
            "select pg_catalog.pg_get_constraintdef(r.oid, true) from pg_catalog.pg_constraint r;"
                .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    insta::assert_snapshot!(
        "pg_get_constraintdef_2",
        execute_query(
            "select pg_catalog.pg_get_constraintdef(r.oid) from pg_catalog.pg_constraint r;"
                .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_pg_to_regtype_pid() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "pg_to_regtype",
        execute_query(
            "select
                    to_regtype('bool') b,
                    to_regtype('name') n,
                    to_regtype('_int4') ai,
                    to_regtype('unknown') u
                ;"
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_date_part_quarter() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "date_part_quarter",
        execute_query(
            "
                SELECT
                    t.d,
                    date_part('quarter', t.d) q
                FROM (
                    SELECT TIMESTAMP '2000-01-05 00:00:00+00:00' d UNION ALL
                    SELECT TIMESTAMP '2005-05-20 00:00:00+00:00' d UNION ALL
                    SELECT TIMESTAMP '2010-08-02 00:00:00+00:00' d UNION ALL
                    SELECT TIMESTAMP '2020-10-01 00:00:00+00:00' d
                ) t
                ORDER BY t.d ASC
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_array_lower() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "array_lower_scalar",
        execute_query(
            "
                SELECT
                    array_lower(ARRAY[1,2,3,4,5]) v1,
                    array_lower(ARRAY[5,4,3,2,1]) v2,
                    array_lower(ARRAY[5,4,3,2,1], 1) v3
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    insta::assert_snapshot!(
        "array_lower_column",
        execute_query(
            "
                SELECT
                    array_lower(t.v) q
                FROM (
                    SELECT ARRAY[1,2,3,4,5] as v UNION ALL
                    SELECT ARRAY[5,4,3,2,1] as v
                ) t
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    insta::assert_snapshot!(
        "array_lower_string",
        execute_query(
            "SELECT array_lower(ARRAY['a', 'b']) v1".to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_array_upper() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "array_upper_scalar",
        execute_query(
            "
                SELECT
                    array_upper(ARRAY[1,2,3,4,5]) v1,
                    array_upper(ARRAY[5,4,3]) v2,
                    array_upper(ARRAY[5,4], 1) v3
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    insta::assert_snapshot!(
        "array_upper_column",
        execute_query(
            "
                SELECT
                    array_upper(t.v) q
                FROM (
                    SELECT ARRAY[1,2,3,4,5] as v
                    UNION ALL
                    SELECT ARRAY[5,4,3,2] as v
                    UNION ALL
                    SELECT ARRAY[5,4,3] as v
                ) t
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    insta::assert_snapshot!(
        "array_upper_string",
        execute_query(
            "SELECT array_upper(ARRAY['a', 'b']) v1".to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_has_schema_privilege_postgres() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "has_schema_privilege",
        execute_query(
            "SELECT
                    nspname,
                    has_schema_privilege('ovr', nspname, 'CREATE') create_top,
                    has_schema_privilege('ovr', nspname, 'create') create_lower,
                    has_schema_privilege('ovr', nspname, 'USAGE') usage_top,
                    has_schema_privilege('ovr', nspname, 'usage') usage_lower
                FROM pg_namespace
                ORDER BY nspname ASC
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    insta::assert_snapshot!(
        "has_schema_privilege_default_user",
        execute_query(
            "SELECT
                    nspname,
                    has_schema_privilege(nspname, 'CREATE') create_top,
                    has_schema_privilege(nspname, 'create') create_lower,
                    has_schema_privilege(nspname, 'USAGE') usage_top,
                    has_schema_privilege(nspname, 'usage') usage_lower
                FROM pg_namespace
                ORDER BY nspname ASC
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    insta::assert_snapshot!(
        "has_schema_privilege_multiple",
        execute_query(
            "SELECT
                    nspname,
                    has_schema_privilege(nspname, 'create,usage') create_usage,
                    has_schema_privilege(nspname, 'usage,create') usage_create
                FROM pg_namespace
                ORDER BY nspname ASC
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_has_table_privilege_postgres() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "has_table_privilege",
        execute_query(
            "SELECT
                    relname,
                    has_table_privilege('ovr', relname, 'SELECT') \"select\",
                    has_table_privilege('ovr', relname, 'INSERT') \"insert\"
                FROM pg_class
                ORDER BY relname ASC
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    insta::assert_snapshot!(
        "has_table_privilege_default_user",
        // + testing priveleges in lowercase
        execute_query(
            "SELECT
                    relname,
                    has_table_privilege(relname, 'select') \"select\",
                    has_table_privilege(relname, 'insert') \"insert\",
                    has_table_privilege(relname, 'delete') \"delete\"
                FROM pg_class
                ORDER BY relname ASC
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_has_any_column_privilege_postgres() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "has_any_column_privilege",
        execute_query(
            "SELECT
                    relname,
                    has_any_column_privilege('ovr', relname, 'SELECT') \"select\",
                    has_any_column_privilege('ovr', relname, 'INSERT') \"insert\",
                    has_any_column_privilege('ovr', relname, 'DELETE') \"delete\",
                    has_any_column_privilege('ovr', relname, 'UPDATE') \"update\"
                FROM pg_class
                ORDER BY relname ASC
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    insta::assert_snapshot!(
        "has_any_column_privilege_default_user",
        // + testing priveleges in lowercase
        execute_query(
            "SELECT
                    relname,
                    has_any_column_privilege(relname, 'select') \"select\",
                    has_any_column_privilege(relname, 'insert') \"insert\",
                    has_any_column_privilege(relname, 'delete') \"delete\",
                    has_any_column_privilege(relname, 'update') \"update\"
                FROM pg_class
                ORDER BY relname ASC
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_pg_total_relation_size() -> Result<(), CubeError> {
    insta::assert_snapshot!(
        "pg_total_relation_size",
        execute_query(
            "SELECT
                    oid,
                    relname,
                    pg_total_relation_size(oid) relsize
                FROM pg_class
                ORDER BY oid ASC
                "
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_redshift_charindex() -> Result<(), CubeError> {
    init_testing_logger();

    insta::assert_snapshot!(
        "redshift_charindex",
        execute_query(
            r#"
                SELECT
                    charindex('d', 'abcdefg') d,
                    charindex('h', 'abcdefg') none
                ;"#
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_extension_udf_xirr() -> Result<(), CubeError> {
    init_testing_logger();

    insta::assert_snapshot!(
        "extension_udf_xirr",
        // XIRR result may differ between runs, so we truncate the result
        execute_query(
            r#"
            SELECT LEFT(XIRR(payment, date)::text, 10) AS xirr
            FROM (
                SELECT '2014-01-01'::date AS date, -10000.0 AS payment
                UNION ALL
                SELECT '2014-03-01'::date AS date, 2750.0 AS payment
                UNION ALL
                SELECT '2014-10-30'::date AS date, 4250.0 AS payment
                UNION ALL
                SELECT '2015-02-15'::date AS date, 3250.0 AS payment
                UNION ALL
                SELECT '2015-04-01'::date AS date, 2750.0 AS payment
            ) AS "t"
            "#
            .to_string(),
            DatabaseProtocol::PostgreSQL
        )
        .await?
    );

    Ok(())
}
