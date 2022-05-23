use std::{env, time::Duration};

use async_trait::async_trait;
use comfy_table::{Cell as TableCell, Table};
use cubesql::config::Config;
use futures::{pin_mut, TryStreamExt};
use portpicker::pick_unused_port;
use rust_decimal::prelude::*;
use tokio::time::sleep;

use super::utils::escape_snapshot_name;
use chrono::{DateTime, NaiveDateTime, Utc};
use pg_srv::{PgType, PgTypeId};
use tokio_postgres::{NoTls, Row};

use super::basic::{AsyncTestConstructorResult, AsyncTestSuite, RunResult};

#[derive(Debug)]
pub struct PostgresIntegrationTestSuite {
    client: tokio_postgres::Client,
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

        let random_port = pick_unused_port().expect("No ports free");
        // let random_port = 5555;
        // let random_port = 5432;

        tokio::spawn(async move {
            println!("[PostgresIntegrationTestSuite] Running SQL API");

            let config = Config::default();
            let config = config.update_config(|mut c| {
                // disable MySQL
                c.bind_address = None;
                c.postgres_bind_address = Some(format!("0.0.0.0:{}", random_port));

                c
            });

            let services = config.configure().await;
            services.wait_processing_loops().await.unwrap();
        });

        sleep(Duration::from_millis(1 * 1000)).await;

        let (client, connection) = tokio_postgres::connect(
            format!(
                "host=127.0.0.1 port={} user=test password=test",
                random_port
            )
            .as_str(),
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

        AsyncTestConstructorResult::Sucess(Box::new(PostgresIntegrationTestSuite { client }))
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
                    oid => unimplemented!("Unsupported pg_type: {:?}", oid),
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

    async fn test_prepare(&self) -> RunResult<()> {
        let stmt = self
            .client
            .prepare("SELECT $1 as t1, $2 as t2")
            .await
            .unwrap();

        self.client
            .query(&stmt, &[&"test1", &"test2"])
            .await
            .unwrap();

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
}

#[async_trait]
impl AsyncTestSuite for PostgresIntegrationTestSuite {
    async fn after_all(&mut self) -> RunResult<()> {
        todo!()
    }

    async fn run(&mut self) -> RunResult<()> {
        self.test_prepare().await?;
        self.test_prepare_empty_query().await?;
        self.test_stream_all().await?;
        self.test_stream_single().await?;
        self.test_snapshot_execute_query(
            "SELECT COUNT(*) count, status FROM Orders GROUP BY status".to_string(),
            None,
            false,
        )
        .await?;

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
                ARRAY['test1', 'test2'] as str_arr,
                ARRAY[1,2,3] as i64_arr,
                ARRAY[1.2,2.3,3.4] as f64_arr,
                '2022-04-25 16:25:01.164774 +00:00'::timestamp as tsmp
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

        Ok(())
    }
}
