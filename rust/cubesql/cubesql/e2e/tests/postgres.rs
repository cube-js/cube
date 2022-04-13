use std::{env, time::Duration};

use async_trait::async_trait;
use comfy_table::{Cell as TableCell, Table};
use cubesql::config::Config;
use portpicker::pick_unused_port;
use tokio::time::sleep;
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

        for (idx, row) in res.into_iter().enumerate() {
            let mut values = Vec::new();

            for _column in row.columns() {
                let value: String = row.get(idx);
                values.push(value);
            }

            table.add_row(values);
        }

        table.trim_fmt()
    }

    fn escape_snapshot_name(&self, name: String) -> String {
        name.to_lowercase()
            // @todo Real escape?
            .replace(" ", "_")
            .replace("*", "asterisk")
    }

    async fn assert_query(&self, res: Vec<Row>, query: String) {
        insta::assert_snapshot!(
            self.escape_snapshot_name(query),
            self.print_query_result(res).await
        );
    }

    #[allow(unused)]
    async fn test_execute_query(&self, query: String) -> RunResult {
        print!("test {} .. ", query);

        let res = self.client.query(&query, &[]).await.unwrap();
        self.assert_query(res, query).await;

        println!("ok");

        Ok(())
    }

    async fn test_prepare(&self) -> RunResult {
        let stmt = self.client.prepare("SELECT $1").await.unwrap();
        self.client.query(&stmt, &[&"test"]).await.unwrap();

        // let stmt = self.client.prepare("SELECT $1").await.unwrap();
        // self.client.query(&stmt, &[&true]).await.unwrap();

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
        // self.test_execute_query(
        //     "SELECT COUNT(*) count, status FROM Orders GROUP BY status".to_string(),
        // ).await?;

        Ok(())
    }
}
