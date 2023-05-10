use std::env;

use super::utils::escape_snapshot_name;
use async_trait::async_trait;
use comfy_table::{Cell, Table};
use cubesql::config::Config;
use mysql_async::{prelude::*, Conn, Opts, Pool, QueryResult, TextProtocol};

use portpicker::pick_unused_port;
use pretty_assertions::assert_eq;

use super::basic::{AsyncTestConstructorResult, AsyncTestSuite, RunResult};

#[derive(Debug)]
pub struct MySqlIntegrationTestSuite {
    pool: Pool,
}

impl MySqlIntegrationTestSuite {
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
        // let random_port = 3306;

        tokio::spawn(async move {
            println!("[MySqlIntegrationTestSuite] Running SQL API");

            let config = Config::default();
            let config = config.update_config(|mut c| {
                c.bind_address = Some(format!("0.0.0.0:{}", random_port));

                c
            });

            let services = config.configure().await;
            services.wait_processing_loops().await.unwrap();
        });

        let url = format!("mysql://root:password@localhost:{}/db", random_port);
        let pool = Pool::new(Opts::from_url(&url).unwrap());

        AsyncTestConstructorResult::Sucess(Box::new(MySqlIntegrationTestSuite { pool }))
    }

    async fn print_query_result<'a>(
        &self,
        res: &mut QueryResult<'a, 'static, TextProtocol>,
    ) -> String {
        let mut table = Table::new();
        table.load_preset("||--+-++|    ++++++");

        let mut header = Vec::with_capacity(res.columns_ref().len());
        let mut description: Vec<String> = Vec::with_capacity(res.columns_ref().len());

        for column in res.columns_ref().into_iter() {
            header.push(Cell::new(column.name_str()));
            description.push(format!(
                "{} type: ({:?}:{}) flags: {:?}",
                column.name_str(),
                column.column_type(),
                column.column_length(),
                column.flags(),
            ));
        }

        table.set_header(header);

        res.for_each(|row| {
            let values: Vec<String> = row
                .unwrap()
                .into_iter()
                .map(|v| match v {
                    mysql_async::Value::NULL => "NULL".to_string(),
                    mysql_async::Value::Int(n) => n.to_string(),
                    mysql_async::Value::UInt(n) => n.to_string(),
                    mysql_async::Value::Float(n) => n.to_string(),
                    mysql_async::Value::Double(n) => n.to_string(),
                    mysql_async::Value::Bytes(n) => String::from_utf8(n).unwrap(),
                    _ => unimplemented!(),
                })
                .collect();
            table.add_row(values);
        })
        .await
        .unwrap();

        description.join("\r\n").to_string() + "\r\n" + &table.trim_fmt()
    }

    async fn test_use(&self) -> RunResult<()> {
        let mut conn = self.pool.get_conn().await.unwrap();

        {
            let mut response = conn
                .query_iter("SELECT database()".to_string())
                .await
                .unwrap();
            assert_eq!(response.collect::<String>().await.unwrap(), vec!["db"]);
        }

        {
            conn.query_iter("USE `information_schema`").await.unwrap();
        }

        let mut response = conn
            .query_iter("SELECT database()".to_string())
            .await
            .unwrap();
        assert_eq!(
            response.collect::<String>().await.unwrap(),
            vec!["information_schema"]
        );

        Ok(())
    }

    async fn test_prepared_reset(&self) -> RunResult<()> {
        let mut conn = self.pool.get_conn().await.unwrap();

        // Server should deallocate statement on execution
        let statement = conn.prep("SELECT ?".to_string()).await.unwrap();
        conn.exec_iter(&statement, ("test",)).await.unwrap();
        conn.exec_iter(&statement, ("test",)).await.unwrap();

        // Close statement
        let statement = conn.prep("SELECT ?".to_string()).await.unwrap();
        conn.exec_iter(&statement, ("test",)).await.unwrap();
        conn.close(statement);

        // Client will allocate a new one
        let statement = conn.prep("SELECT ?".to_string()).await.unwrap();
        conn.exec_iter(&statement, ("test",)).await.unwrap();

        Ok(())
    }

    async fn test_prepared(&self) -> RunResult<()> {
        let mut conn = self.pool.get_conn().await.unwrap();

        {
            let statement = conn.prep("/** 1 */ SELECT ?".to_string()).await.unwrap();
            conn.exec_iter(&statement, ("test",)).await.unwrap();
        }

        // @todo Not working, because deserialization on the server?
        // {
        //     let statement = conn.prep("/** 2 */ SELECT ?".to_string()).await.unwrap();
        //     conn.exec_iter(&statement, (true,)).await.unwrap();
        // }

        // {
        //     let statement = conn.prep("/** 3 */ SELECT ?".to_string()).await.unwrap();
        //     conn.exec_iter(&statement, (false,)).await.unwrap();
        // }

        Ok(())
    }

    async fn assert_query(&self, conn: &mut Conn, query: String) {
        let mut response = conn.query_iter(query.clone()).await.unwrap();
        insta::assert_snapshot!(
            escape_snapshot_name(query),
            self.print_query_result(&mut response).await
        );
    }

    async fn test_execute_query(&self, query: String) -> RunResult<()> {
        print!("test {} .. ", query);

        let mut conn = self.pool.get_conn().await.unwrap();
        self.assert_query(&mut conn, query).await;

        println!("ok");

        Ok(())
    }
}

#[async_trait]
impl AsyncTestSuite for MySqlIntegrationTestSuite {
    async fn after_all(&mut self) -> RunResult<()> {
        // TODO: Close SQL API?
        Ok(())
    }

    async fn run(&mut self) -> RunResult<()> {
        self.test_use().await?;
        self.test_prepared().await?;
        self.test_prepared_reset().await?;
        self.test_execute_query(
            "SELECT COUNT(*) count, status FROM Orders GROUP BY status".to_string(),
        )
        .await?;
        self.test_execute_query(
            "SELECT COUNT(*) count, status, createdAt FROM Orders GROUP BY status, createdAt ORDER BY createdAt".to_string(),
        )
        .await?;
        self.test_execute_query(
            "SELECT COUNT(*) count, status, DATE_TRUNC('month', createdAt) date FROM Orders GROUP BY status, DATE_TRUNC('month', createdAt) ORDER BY date".to_string(),
        )
        .await?;
        self.test_execute_query(
            "SELECT COUNT(*) count, status, DATE_TRUNC('quarter', createdAt) date FROM Orders GROUP BY status, DATE_TRUNC('quarter', createdAt) ORDER BY date".to_string(),
        )
        .await?;
        self.test_execute_query(
            r#"SELECT
                CAST(true as boolean) as bool_true,
                CAST(false as boolean) as bool_false,
                1::int as int,
                'str' as str
            "#
            .to_string(),
        )
        .await?;

        Ok(())
    }
}
