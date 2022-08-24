use std::{env, time::Duration};

use async_trait::async_trait;
use comfy_table::{Cell as TableCell, Table};
use cubesql::config::Config;
use futures::{pin_mut, TryStreamExt};
use portpicker::{pick_unused_port, Port};
use rust_decimal::prelude::*;
use tokio::time::sleep;

use super::utils::escape_snapshot_name;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use datafusion::assert_contains;
use pg_interval::Interval;
use pg_srv::{PgType, PgTypeId};
use tokio::join;
use tokio_postgres::{error::SqlState, Client, NoTls, Row, SimpleQueryMessage};

use super::basic::{AsyncTestConstructorResult, AsyncTestSuite, RunResult};

#[derive(Debug)]
pub struct PostgresIntegrationTestSuite {
    client: tokio_postgres::Client,
    port: Port,
    // connection: tokio_postgres::Connection<Socket, NoTlsStream>,
}

impl PostgresIntegrationTestSuite {
    pub(crate) async fn before_all() -> AsyncTestConstructorResult {
        let mut env_defined = false;

        if let Ok(testing_cube_token) = env::var("CUBESQL_TESTING_CUBE_TOKEN".to_string()) {
            env::set_var("CUBESQL_CUBE_TOKEN", testing_cube_token);

            env_defined = true;
        };

        if let Ok(testing_cube_url) = env::var("CUBESQL_TESTING_CUBE_URL".to_string()) {
            env::set_var("CUBESQL_CUBE_URL", testing_cube_url);
        } else {
            env_defined = false;
        };

        if !env_defined {
            return AsyncTestConstructorResult::Skipped(
                "Testing variables are not defined, passing....".to_string(),
            );
        };

        let port = pick_unused_port().expect("No ports free");
        // let random_port = 5555;
        // let random_port = 5432;

        tokio::spawn(async move {
            println!("[PostgresIntegrationTestSuite] Running SQL API");

            let config = Config::default();
            let config = config.update_config(|mut c| {
                // disable MySQL
                c.bind_address = None;
                c.postgres_bind_address = Some(format!("0.0.0.0:{}", port));

                c
            });

            let services = config.configure().await;
            services.wait_processing_loops().await.unwrap();
        });

        sleep(Duration::from_millis(1 * 1000)).await;

        let client = PostgresIntegrationTestSuite::create_client(port).await;

        AsyncTestConstructorResult::Sucess(Box::new(PostgresIntegrationTestSuite { client, port }))
    }

    async fn create_client(port: Port) -> Client {
        let (client, connection) = tokio_postgres::connect(
            format!("host=127.0.0.1 port={} user=test password=test", port).as_str(),
            NoTls,
        )
        .await
        .unwrap();

        // The connection object performs the actual communication with the database,
        // so spawn it off to run on its own.
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        client
    }

    async fn print_query_result<'a>(
        &self,
        res: Vec<Row>,
        with_description: bool,
        with_rows: bool,
    ) -> String {
        let mut table = Table::new();
        table.load_preset("||--+-++|    ++++++");

        let mut description_done = false;
        let mut description: Vec<String> = Vec::new();

        let mut header = vec![];

        if let Some(row) = res.first() {
            for column in row.columns() {
                header.push(TableCell::new(column.name()));
            }
        };

        table.set_header(header);

        for row in res.into_iter() {
            let mut values: Vec<String> = Vec::new();

            for (idx, column) in row.columns().into_iter().enumerate() {
                if !description_done {
                    description.push(format!(
                        "{} type: {} ({})",
                        column.name(),
                        column.type_().oid(),
                        PgType::get_by_tid(
                            PgTypeId::from_oid(column.type_().oid())
                                .expect(&format!("Unknown oid {}", column.type_().oid()))
                        )
                        .typname,
                    ));
                }

                // We dont need data when with_rows = false, but it's useful for testing that data type is correct
                match PgTypeId::from_oid(column.type_().oid())
                    .expect(&format!("Unknown type oid: {}", column.type_().oid()))
                {
                    PgTypeId::INT8 => {
                        let value: Option<i64> = row.get(idx);
                        values.push(value.map(|v| v.to_string()).unwrap_or("NULL".to_string()));
                    }
                    PgTypeId::INT2 => {
                        let value: Option<i16> = row.get(idx);
                        values.push(value.map(|v| v.to_string()).unwrap_or("NULL".to_string()));
                    }
                    PgTypeId::INT4 => {
                        let value: Option<i32> = row.get(idx);
                        values.push(value.map(|v| v.to_string()).unwrap_or("NULL".to_string()));
                    }
                    PgTypeId::TEXT => {
                        let value: Option<String> = row.get(idx);
                        values.push(value.map(|v| v.to_string()).unwrap_or("NULL".to_string()));
                    }
                    PgTypeId::BOOL => {
                        let value: Option<bool> = row.get(idx);
                        values.push(value.map(|v| v.to_string()).unwrap_or("NULL".to_string()));
                    }
                    PgTypeId::FLOAT4 => {
                        let value: Option<f32> = row.get(idx);
                        values.push(value.map(|v| v.to_string()).unwrap_or("NULL".to_string()));
                    }
                    PgTypeId::FLOAT8 => {
                        let value: Option<f64> = row.get(idx);
                        values.push(value.map(|v| v.to_string()).unwrap_or("NULL".to_string()));
                    }
                    PgTypeId::DATE => {
                        let value: Option<NaiveDate> = row.get(idx);
                        values.push(value.map(|v| v.to_string()).unwrap_or("NULL".to_string()));
                    }
                    PgTypeId::INTERVAL => {
                        let value: Option<Interval> = row.get(idx);
                        values.push(value.map(|v| v.to_postgres()).unwrap_or("NULL".to_string()));
                    }
                    PgTypeId::TIMESTAMP => {
                        let value: Option<NaiveDateTime> = row.get(idx);
                        values.push(value.map(|v| v.to_string()).unwrap_or("NULL".to_string()));
                    }
                    PgTypeId::TIMESTAMPTZ => {
                        let value: Option<DateTime<Utc>> = row.get(idx);
                        values.push(value.map(|v| v.to_string()).unwrap_or("NULL".to_string()));
                    }
                    PgTypeId::NUMERIC => {
                        let value: Option<Decimal> = row.get(idx);
                        values.push(value.map(|v| v.to_string()).unwrap_or("NULL".to_string()));
                    }
                    PgTypeId::ARRAYINT4 => {
                        let value: Option<Vec<i32>> = row.get(idx);
                        if let Some(v) = value {
                            values.push(
                                v.into_iter()
                                    .map(|v| v.to_string())
                                    .collect::<Vec<String>>()
                                    .join(",")
                                    .to_string(),
                            );
                        } else {
                            values.push("NULL".to_string())
                        }
                    }
                    PgTypeId::ARRAYINT8 => {
                        let value: Option<Vec<i64>> = row.get(idx);
                        if let Some(v) = value {
                            values.push(
                                v.into_iter()
                                    .map(|v| v.to_string())
                                    .collect::<Vec<String>>()
                                    .join(",")
                                    .to_string(),
                            );
                        } else {
                            values.push("NULL".to_string())
                        }
                    }
                    PgTypeId::ARRAYFLOAT8 => {
                        let value: Option<Vec<f64>> = row.get(idx);
                        if let Some(v) = value {
                            values.push(
                                v.into_iter()
                                    .map(|v| v.to_string())
                                    .collect::<Vec<String>>()
                                    .join(",")
                                    .to_string(),
                            );
                        } else {
                            values.push("NULL".to_string())
                        }
                    }
                    PgTypeId::ARRAYTEXT => {
                        let value: Option<Vec<String>> = row.get(idx);
                        if let Some(v) = value {
                            values.push(
                                v.into_iter()
                                    .map(|v| v.to_string())
                                    .collect::<Vec<String>>()
                                    .join(",")
                                    .to_string(),
                            );
                        } else {
                            values.push("NULL".to_string())
                        }
                    }
                    tid => unimplemented!("Unsupported pg_type: {:?}({})", tid, tid.to_type().oid),
                }
            }

            description_done = true;
            table.add_row(values);
        }

        if with_description {
            if with_rows {
                description.join("\r\n").to_string() + "\r\n" + &table.trim_fmt()
            } else {
                description.join("\r\n").to_string()
            }
        } else {
            if !with_rows {
                panic!("Superstrange test, which doesnt print rows and description!");
            }

            table.trim_fmt()
        }
    }

    async fn test_cancel(&self) -> RunResult<()> {
        let cancel_token = self.client.cancel_token();
        let cancel = async move {
            tokio::time::sleep(Duration::from_millis(1000)).await;

            cancel_token.cancel_query(NoTls).await
        };

        // testing_blocking tables will neven finish. It's a special testing table
        let sleep = self
            .client
            .batch_execute("SELECT * FROM information_schema.testing_blocking");

        match join!(sleep, cancel) {
            (Err(ref e), Ok(())) if e.code() == Some(&SqlState::QUERY_CANCELED) => {}
            t => panic!("unexpected return {:?}", t),
        };

        Ok(())
    }

    async fn test_snapshot_execute_query(
        &self,
        query: String,
        snapshot_name: Option<String>,
        with_description: bool,
    ) -> RunResult<()> {
        print!("test {} .. ", query);

        let res = self.client.query(&query, &[]).await.unwrap();
        insta::assert_snapshot!(
            snapshot_name.unwrap_or(escape_snapshot_name(query)),
            self.print_query_result(res, with_description, true).await
        );
        println!("ok");

        Ok(())
    }

    // This method returns only description instead of full data snapshot
    // It's useful for detecting type changes
    async fn test_snapshot_description_execute_query(
        &self,
        query: String,
        snapshot_name: Option<String>,
    ) -> RunResult<()> {
        print!("test {} .. ", query);

        let res = self.client.query(&query, &[]).await.unwrap();
        insta::assert_snapshot!(
            snapshot_name.unwrap_or(escape_snapshot_name(query)),
            self.print_query_result(res, true, false).await
        );

        println!("ok");

        Ok(())
    }

    async fn test_execute_query<AssertFn>(&self, query: String, f: AssertFn) -> RunResult<()>
    where
        AssertFn: FnOnce(Vec<Row>) -> (),
    {
        print!("test {} .. ", query);

        let res = self.client.query(&query, &[]).await.unwrap();
        f(res);

        println!("ok");

        Ok(())
    }

    async fn test_simple_query<AssertFn>(
        &self,
        query: String,
        f: AssertFn,
    ) -> Result<(), tokio_postgres::Error>
    where
        AssertFn: FnOnce(Vec<SimpleQueryMessage>) -> (),
    {
        print!("test {} .. ", query);

        let res = self.client.simple_query(&query).await?;
        f(res);

        println!("ok");

        Ok(())
    }

    async fn test_prepare(&self) -> RunResult<()> {
        // Unknown variables will be detected as TEXT
        // LIMIT has a typehint for i64
        let stmt = self
            .client
            .prepare("SELECT $1 as t1, $2 as t2 LIMIT $3")
            .await
            .unwrap();

        self.client
            .query(&stmt, &[&"test1", &"test2", &0_i64])
            .await
            .unwrap();

        Ok(())
    }

    async fn test_extended_error(&self) -> RunResult<()> {
        let actual_err = if let Err(err) = self
            .client
            .prepare("SELECT * FROM unknown_cube_will_lead_to_an_error")
            .await
        {
            err
        } else {
            panic!("Must be an error")
        };

        assert_contains!(
            actual_err.to_string(),
            "Error during planning: Table or CTE with name 'unknown_cube_will_lead_to_an_error'"
        );

        Ok(())
    }

    async fn test_prepare_empty_query(&self) -> RunResult<()> {
        let stmt = self.client.prepare("").await.unwrap();

        self.client.query(&stmt, &[]).await.unwrap();

        Ok(())
    }

    // This test tests paging on the service side which uses stream of RecordBatches to stream this query
    async fn test_stream_all(&self) -> RunResult<()> {
        let stmt = self
            .client
            .prepare("SELECT * FROM information_schema.testing_dataset WHERE id > CAST($1 as int)")
            .await
            .unwrap();

        let it = self.client.query_raw(&stmt, &["0"]).await.unwrap();

        pin_mut!(it);

        let mut total = 1;

        while let Some(_) = it.try_next().await.unwrap() {
            total += 1;
        }

        assert_eq!(total, 5000);

        Ok(())
    }

    // This test should return one row
    // TODO: Find a way how to manage Execute's return_rows to 1 instead of 100
    async fn test_stream_single(&self) -> RunResult<()> {
        let stmt = self
            .client
            .prepare("SELECT * FROM information_schema.testing_dataset WHERE id = CAST($1 as int)")
            .await
            .expect("Unable to prepare statement");

        let _ = self
            .client
            .query_one(&stmt, &[&"2000"])
            .await
            .expect("Unable to execute query");

        Ok(())
    }

    async fn test_portal_pagination(&self) -> RunResult<()> {
        let mut client = PostgresIntegrationTestSuite::create_client(self.port).await;

        let stmt = client
            .prepare("SELECT generate_series(1, 100)")
            .await
            .unwrap();

        let transaction = client.transaction().await.unwrap();

        let portal = transaction.bind(&stmt, &[]).await.unwrap();
        let r1 = transaction.query_portal(&portal, 25).await?;
        assert_eq!(r1.len(), 25);

        let r2 = transaction.query_portal(&portal, 50).await?;
        assert_eq!(r2.len(), 50);

        let r3 = transaction.query_portal(&portal, 75).await?;
        assert_eq!(r3.len(), 25);

        transaction.commit().await?;

        Ok(())
    }

    async fn test_simple_cursors(&self) -> RunResult<()> {
        self.test_simple_query(
            r#"declare test_cursor_generate_series cursor with hold for SELECT generate_series(1, 100);"#
                .to_string(),
            |messages| {
                assert_eq!(messages.len(), 1);
            }
        ).await?;

        self.test_simple_query(
            r#"fetch 1 in test_cursor_generate_series; fetch 10 in test_cursor_generate_series;"#
                .to_string(),
            |messages| {
                // Row | Selection - 2
                // Row 1 | .. | Row 10 | Selection - 11
                assert_eq!(messages.len(), 13);

                if let SimpleQueryMessage::Row(row) = &messages[0] {
                    assert_eq!(row.get(0), Some("1"));
                } else {
                    panic!("Must be Row command, 0")
                }

                if let SimpleQueryMessage::CommandComplete(rows) = messages[1] {
                    assert_eq!(rows, 1_u64);
                } else {
                    panic!("Must be CommandComplete command, 1")
                }

                if let SimpleQueryMessage::Row(row) = &messages[2] {
                    assert_eq!(row.get(0), Some("2"));
                } else {
                    panic!("Must be Row command, 2")
                }

                if let SimpleQueryMessage::Row(row) = &messages[11] {
                    assert_eq!(row.get(0), Some("11"));
                } else {
                    panic!("Must be Row command, 11")
                }

                if let SimpleQueryMessage::CommandComplete(rows) = messages[12] {
                    assert_eq!(rows, 10_u64);
                } else {
                    panic!("Must be CommandComplete command, 12")
                }
            },
        )
        .await?;

        // Read till finish
        self.test_simple_query(
            r#"fetch 1000 in test_cursor_generate_series;"#.to_string(),
            |messages| {
                // fetch 1
                // fetch 10
                // 100 - 11 = 89
                assert_eq!(messages.len(), 89 + 1);

                if let SimpleQueryMessage::CommandComplete(rows) = messages[89] {
                    assert_eq!(rows, 89_u64);
                } else {
                    panic!("Must be CommandComplete command, 89")
                }
            },
        )
        .await?;

        // Portal for Cursor was finished.
        self.test_simple_query(
            r#"fetch 1000 in test_cursor_generate_series; fetch 10 in test_cursor_generate_series;"#
                .to_string(),
            |messages| {
                assert_eq!(messages.len(), 2);
            }
        ).await?;

        Ok(())
    }

    // Tableau Desktop uses it
    async fn test_simple_cursors_without_hold(&self) -> RunResult<()> {
        // without hold is default behaviour
        self.test_simple_query(
            r#"begin; declare test_without_hold cursor for SELECT generate_series(1, 100);"#
                .to_string(),
            |_| {},
        )
        .await?;

        self.test_simple_query(
            r#"fetch 5 in test_without_hold; commit;"#.to_string(),
            |_| {},
        )
        .await?;

        // Assert that cursor was closed
        let err = self
            .test_simple_query(r#"fetch 5 in test_without_hold;"#.to_string(), |_| {})
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "db error: ERROR: cursor \"test_without_hold\" does not exist"
        );

        Ok(())
    }

    // Tableau Desktop uses it
    async fn test_simple_cursors_close_specific(&self) -> RunResult<()> {
        // without hold is default behaviour
        self.test_simple_query(
            r#"DECLARE test_with_hold CURSOR WITH HOLD FOR SELECT generate_series(1, 100);"#
                .to_string(),
            |_| {},
        )
        .await?;

        self.test_simple_query(r#"FETCH 5 IN test_with_hold;"#.to_string(), |_| {})
            .await?;

        self.test_simple_query(r#"CLOSE test_with_hold;"#.to_string(), |_| {})
            .await?;

        // Assert that cursor was closed
        let err = self
            .test_simple_query(r#"fetch 5 in test_with_hold;"#.to_string(), |_| {})
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "db error: ERROR: cursor \"test_with_hold\" does not exist"
        );

        Ok(())
    }

    // Tableau Desktop uses it
    async fn test_simple_cursors_close_all(&self) -> RunResult<()> {
        // without hold is default behaviour
        self.test_simple_query(
            r#"DECLARE cursor_1 CURSOR WITH HOLD for SELECT generate_series(1, 100); DECLARE cursor_2 CURSOR WITH HOLD for SELECT generate_series(1, 100);"#
                .to_string(),
            |_| {},
        )
            .await?;

        self.test_simple_query(
            r#"FETCH 5 IN cursor_1; FETCH 5 IN cursor_2;"#.to_string(),
            |_| {},
        )
        .await?;

        self.test_simple_query(r#"CLOSE ALL;"#.to_string(), |_| {})
            .await?;

        // Assert that cursor1 was closed
        let err = self
            .test_simple_query(r#"FETCH 5 IN cursor_1;"#.to_string(), |_| {})
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "db error: ERROR: cursor \"cursor_1\" does not exist"
        );

        // Assert that cursor2 was closed
        let err = self
            .test_simple_query(r#"FETCH 5 IN cursor_2;"#.to_string(), |_| {})
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "db error: ERROR: cursor \"cursor_2\" does not exist"
        );

        Ok(())
    }

    // Hightouch uses it
    async fn test_simple_query_prepare(&self) -> RunResult<()> {
        self.test_simple_query("PREPARE simple_query AS SELECT 1".to_string(), |_| {})
            .await?;

        self.test_simple_query(
            "PREPARE simple_query_parens AS (SELECT 1)".to_string(),
            |_| {},
        )
        .await?;

        Ok(())
    }

    async fn test_simple_query_deallocate_specific(&self) -> RunResult<()> {
        self.test_simple_query("PREPARE simple_query AS SELECT 1".to_string(), |_| {})
            .await?;

        self.test_simple_query(
            "select * from pg_catalog.pg_prepared_statements WHERE name = 'simple_query'"
                .to_string(),
            |rows| {
                assert_eq!(rows.len(), 1 + 1, "prepared statement must be defined");
            },
        )
        .await?;

        self.test_simple_query("DEALLOCATE simple_query".to_string(), |_| {})
            .await?;

        self.test_simple_query(
            "select * from pg_catalog.pg_prepared_statements WHERE name = 'simple_query'"
                .to_string(),
            |rows| {
                assert_eq!(
                    rows.len(),
                    1,
                    "prepared statement called simple_query must be undefined (was deallocated)"
                );
            },
        )
        .await?;

        Ok(())
    }

    async fn test_simple_query_deallocate_all(&self) -> RunResult<()> {
        self.test_simple_query("PREPARE simple_query AS SELECT 1".to_string(), |_| {})
            .await?;

        self.test_simple_query(
            "select * from pg_catalog.pg_prepared_statements WHERE name = 'simple_query'"
                .to_string(),
            |rows| {
                assert_eq!(rows.len(), 1 + 1, "prepared statement must be defined");
            },
        )
        .await?;

        self.test_simple_query("DEALLOCATE ALL".to_string(), |_| {})
            .await?;

        self.test_simple_query(
            "select * from pg_catalog.pg_prepared_statements".to_string(),
            |rows| {
                assert_eq!(
                    rows.len(),
                    1,
                    "all prepared statement must be cleared after deallocate all"
                );
            },
        )
        .await?;

        Ok(())
    }

    async fn test_simple_query_discard_all(&self) -> RunResult<()> {
        self.test_simple_query("PREPARE simple_query AS SELECT 1".to_string(), |_| {})
            .await?;
        self.test_simple_query(
            r#"DECLARE cursor_1 CURSOR WITH HOLD for SELECT generate_series(1, 100); DECLARE cursor_2 CURSOR WITH HOLD for SELECT generate_series(1, 100);"#
                .to_string(),
            |_| {},
        )
            .await?;

        self.test_simple_query("DISCARD ALL".to_string(), |_| {})
            .await?;

        self.test_simple_query(
            "select * from pg_catalog.pg_prepared_statements".to_string(),
            |rows| {
                assert_eq!(
                    rows.len(),
                    1,
                    "all prepared statement must be cleared after DISCARD ALL"
                );
            },
        )
        .await?;

        // TODO: Check portals/cursors

        Ok(())
    }

    async fn test_df_panic_handle(&self) -> RunResult<()> {
        // This test only stream call with panic on the Portal
        let err = self
            .test_simple_query(
                "SELECT TIMESTAMP '9999-12-31 00:00:00';".to_string(),
                |_| {},
            )
            .await
            .unwrap_err();

        assert_eq!(
            err.to_string(),
            "db error: ERROR: Internal: Unexpected panic. Reason: attempt to multiply with overflow"
        );

        Ok(())
    }
}

#[async_trait]
impl AsyncTestSuite for PostgresIntegrationTestSuite {
    async fn after_all(&mut self) -> RunResult<()> {
        // TODO: Close SQL API?
        Ok(())
    }

    async fn run(&mut self) -> RunResult<()> {
        self.test_cancel().await?;
        self.test_prepare().await?;
        self.test_extended_error().await?;
        self.test_prepare_empty_query().await?;
        self.test_stream_all().await?;
        self.test_stream_single().await?;
        self.test_portal_pagination().await?;
        self.test_simple_cursors().await?;
        self.test_simple_cursors_without_hold().await?;
        self.test_simple_cursors_close_specific().await?;
        self.test_simple_cursors_close_all().await?;
        self.test_simple_query_prepare().await?;
        self.test_snapshot_execute_query(
            "SELECT COUNT(*) count, status FROM Orders GROUP BY status".to_string(),
            None,
            false,
        )
        .await?;
        self.test_simple_query_deallocate_specific().await?;
        self.test_simple_query_deallocate_all().await?;
        self.test_df_panic_handle().await?;
        self.test_simple_query_discard_all().await?;

        // PostgreSQL doesn't support unsigned integers in the protocol, it's a constraint only
        self.test_snapshot_execute_query(
            r#"SELECT
                NULL,
                1.234::float as f32,
                1.234::double as f64,
                1::smallint as i16,
                CAST(1 as SMALLINT UNSIGNED) as u16,
                1::integer as i32,
                CAST(1 as INTEGER UNSIGNED) as u32,
                1::bigint as i64,
                CAST(1 as BIGINT UNSIGNED) as u64,
                true as bool_true,
                false as bool_false,
                'test' as str,
                CAST(1.25 as DECIMAL(15, 0)) as d0,
                CAST(1.25 as DECIMAL(15, 2)) as d2,
                CAST(1.25 as DECIMAL(15, 5)) as d5,
                CAST(1.25 as DECIMAL(15, 10)) as d10,
                CAST('2022-04-25 16:25:01.164774 +00:00' as timestamp)::date as date,
                '2022-04-25 16:25:01.164774 +00:00'::timestamp as tsmp,
                interval '13 month' as interval_year_month,
                interval '1 hour 30 minutes' as interval_day_time,
                interval '13 month 1 day 1 hour 30 minutes' as interval_month_day_nano,
                ARRAY['test1', 'test2'] as str_arr,
                ARRAY[1,2,3] as i64_arr,
                ARRAY[1.2,2.3,3.4] as f64_arr
            "#
            .to_string(),
            Some("pg_test_types".to_string()),
            true,
        )
        .await?;

        let system_tables_do_review = vec![
            "pg_catalog.pg_type",
            "pg_catalog.pg_proc",
            "pg_catalog.pg_tables",
            "pg_catalog.pg_class",
            "information_schema.tables",
            "information_schema.columns",
        ];

        for tbl_name in system_tables_do_review {
            self.test_snapshot_description_execute_query(
                format!("SELECT * FROM {}", tbl_name),
                Some(format!("system_{}", tbl_name)),
            )
            .await?;
        }

        self.test_snapshot_execute_query(
            r#"
                SELECT CAST(TRUNC(EXTRACT(QUARTER FROM "Orders"."completedAt")) AS INTEGER) AS q,
                    SUM("Orders"."count") AS s
                    FROM "public"."Orders" "Orders"
                GROUP BY 1
                ORDER BY q ASC
            "#
            .to_string(),
            Some("datepart_quarter".to_string()),
            false,
        )
        .await?;

        self.test_execute_query(
            r#"SELECT
                now() as tsmp_tz,
                now()::timestamp as tsmp
            "#
            .to_string(),
            |rows| {
                assert_eq!(rows.len(), 1);

                let columns = rows.get(0).unwrap().columns();
                assert_eq!(
                    columns
                        .into_iter()
                        .map(|col| col.type_().oid())
                        .collect::<Vec<u32>>(),
                    vec![1184, 1114]
                );
            },
        )
        .await?;

        self.test_simple_query(r#"SET DateStyle = 'ISO'"#.to_string(), |messages| {
            assert_eq!(messages.len(), 1);

            // SET
            if let SimpleQueryMessage::Row(_) = messages[0] {
                panic!("Must be CommandComplete command, (SET is used)")
            }
        })
        .await?;

        // Tableau Desktop
        self.test_simple_query(
            r#"SET DateStyle = 'ISO';SET extra_float_digits = 2;show transaction_isolation"#
                .to_string(),
            |messages| {
                assert_eq!(messages.len(), 4);

                // SET
                if let SimpleQueryMessage::Row(_) = messages[0] {
                    panic!("Must be CommandComplete command, 1")
                }

                // SET
                if let SimpleQueryMessage::Row(_) = messages[1] {
                    panic!("Must be CommandComplete command, 2")
                }

                // SELECT
                if let SimpleQueryMessage::CommandComplete(_) = messages[2] {
                    panic!("Must be Row command, 3")
                }

                // DATA
                if let SimpleQueryMessage::Row(_) = messages[3] {
                    panic!("Must be CommandComplete command, 4")
                }
            },
        )
        .await?;

        Ok(())
    }
}
