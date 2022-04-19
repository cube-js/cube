use std::{env, time::Duration};

use async_trait::async_trait;
use comfy_table::{Cell as TableCell, Table};
use cubesql::config::Config;
use futures::{pin_mut, TryStreamExt};
use portpicker::pick_unused_port;
use tokio::time::sleep;

use super::utils::escape_snapshot_name;
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

    async fn print_query_result<'a>(&self, res: Vec<Row>) -> String {
        let mut table = Table::new();
        table.load_preset("||--+-++|    ++++++");

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
                match column.type_().oid() {
                    20 => {
                        let value: i64 = row.get(idx);
                        values.push(value.to_string());
                    }
                    25 => {
                        let value: String = row.get(idx);
                        values.push(value);
                    }
                    16 => {
                        let value: bool = row.get(idx);
                        values.push(value.to_string());
                    }
                    701 => {
                        let value: f64 = row.get(idx);
                        values.push(value.to_string());
                    }
                    // _int4
                    1007 => {
                        let value: Vec<i32> = row.get(idx);
                        values.push(
                            value
                                .into_iter()
                                .map(|v| v.to_string())
                                .collect::<Vec<String>>()
                                .join(",")
                                .to_string(),
                        );
                    }
                    // _int8
                    1016 => {
                        let value: Vec<i64> = row.get(idx);
                        values.push(
                            value
                                .into_iter()
                                .map(|v| v.to_string())
                                .collect::<Vec<String>>()
                                .join(",")
                                .to_string(),
                        );
                    }
                    // _text
                    1009 => {
                        let value: Vec<String> = row.get(idx);
                        values.push(value.join(",").to_string());
                    }
                    oid => unimplemented!("Unsupported pg_type: {}", oid),
                }
            }

            table.add_row(values);
        }

        table.trim_fmt()
    }

    #[allow(unused)]
    async fn test_execute_query(&self, query: String, snapshot_name: Option<String>) -> RunResult {
        print!("test {} .. ", query);

        let res = self.client.query(&query, &[]).await.unwrap();
        insta::assert_snapshot!(
            snapshot_name.unwrap_or(escape_snapshot_name(query)),
            self.print_query_result(res).await
        );

        println!("ok");

        Ok(())
    }

    async fn test_prepare(&self) -> RunResult {
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

    async fn test_prepare_empty_query(&self) -> RunResult {
        let stmt = self.client.prepare("").await.unwrap();

        self.client.query(&stmt, &[]).await.unwrap();

        Ok(())
    }

    // This test tests paging on the service side which uses stream of RecordBatches to stream this query
    async fn test_stream_all(&self) -> RunResult {
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

        assert_eq!(total, 3000);

        Ok(())
    }

    // This test should return one row
    // TODO: Find a way how to manage Execute's return_rows to 1 instead of 100
    async fn test_stream_single(&self) -> RunResult {
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
    async fn after_all(&mut self) -> RunResult {
        todo!()
    }

    async fn run(&mut self) -> RunResult {
        self.test_prepare().await?;
        self.test_prepare_empty_query().await?;
        self.test_stream_all().await?;
        self.test_stream_single().await?;
        self.test_execute_query(
            "SELECT COUNT(*) count, status FROM Orders GROUP BY status".to_string(),
            None,
        )
        .await?;
        self.test_execute_query(
            r#"SELECT
                true as bool_true,
                false as bool_false,
                'test',
                1.0,
                1,
                ARRAY['test1', 'test2'] as str_arr,
                ARRAY[1,2,3] as int8_arr
            "#
            .to_string(),
            Some("pg_test_types".to_string()),
        )
        .await?;

        Ok(())
    }
}
