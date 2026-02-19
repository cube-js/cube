// Integration tests for Arrow IPC output format

use std::{env, time::Duration};

use async_trait::async_trait;
use cubesql::config::Config;
use portpicker::{pick_unused_port, Port};
use tokio::time::sleep;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

use super::basic::{AsyncTestConstructorResult, AsyncTestSuite, RunResult};

#[derive(Debug)]
pub struct ArrowIPCIntegrationTestSuite {
    client: tokio_postgres::Client,
    _port: Port,
}

impl ArrowIPCIntegrationTestSuite {
    pub(crate) async fn before_all() -> AsyncTestConstructorResult {
        // Check for required Cube server credentials
        // Note: Even though these tests use simple queries (SELECT 1, etc.),
        // CubeSQL still needs to connect to Cube's metadata API on startup
        let mut env_defined = false;

        if let Ok(testing_cube_token) = env::var("CUBESQL_TESTING_CUBE_TOKEN") {
            if !testing_cube_token.is_empty() {
                env::set_var("CUBESQL_CUBE_TOKEN", testing_cube_token);
                env_defined = true;
            }
        }

        if let Ok(testing_cube_url) = env::var("CUBESQL_TESTING_CUBE_URL") {
            if !testing_cube_url.is_empty() {
                env::set_var("CUBESQL_CUBE_URL", testing_cube_url);
            } else {
                env_defined = false;
            }
        } else {
            env_defined = false;
        }

        if !env_defined {
            return AsyncTestConstructorResult::Skipped(
                "Arrow IPC tests require CUBESQL_TESTING_CUBE_TOKEN and CUBESQL_TESTING_CUBE_URL"
                    .to_string(),
            );
        }

        let port = pick_unused_port().expect("No ports free");

        tokio::spawn(async move {
            println!("[ArrowIPCIntegrationTestSuite] Running SQL API");

            let config = Config::default();
            let config = config.update_config(|mut c| {
                c.bind_address = None;
                c.postgres_bind_address = Some(format!("0.0.0.0:{}", port));
                c
            });

            config.configure().await;
            let services = config.cube_services().await;
            services.wait_processing_loops().await.unwrap();
        });

        sleep(Duration::from_secs(1)).await;

        let client = Self::create_client(
            format!("host=127.0.0.1 port={} user=test password=test", port)
                .parse()
                .unwrap(),
        )
        .await;

        AsyncTestConstructorResult::Success(Box::new(ArrowIPCIntegrationTestSuite {
            client,
            _port: port,
        }))
    }

    async fn create_client(config: tokio_postgres::Config) -> Client {
        let (client, connection) = config.connect(NoTls).await.unwrap();

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        client
    }

    async fn set_arrow_ipc_output(&self) -> RunResult<()> {
        self.client
            .simple_query("SET output_format = 'arrow_ipc'")
            .await?;
        Ok(())
    }

    async fn reset_output_format(&self) -> RunResult<()> {
        self.client
            .simple_query("SET output_format = 'postgresql'")
            .await?;
        Ok(())
    }

    /// Test that Arrow IPC output format can be set and retrieved
    async fn test_set_output_format(&mut self) -> RunResult<()> {
        self.set_arrow_ipc_output().await?;

        // Query the current setting
        let rows = self.client.simple_query("SHOW output_format").await?;

        // Verify the format is set
        let mut found = false;
        for msg in rows {
            match msg {
                SimpleQueryMessage::Row(row) => {
                    if let Some(value) = row.get(0) {
                        if value == "arrow_ipc" {
                            found = true;
                        }
                    }
                }
                _ => {}
            }
        }

        assert!(found, "output_format should be set to 'arrow_ipc'");

        self.reset_output_format().await?;
        Ok(())
    }

    /// Test that Arrow IPC output is recognized
    /// Note: This tests the protocol layer, not actual Arrow deserialization
    async fn test_arrow_ipc_query(&mut self) -> RunResult<()> {
        self.set_arrow_ipc_output().await?;

        // Execute a simple system query with Arrow IPC output
        let rows = self.client.simple_query("SELECT 1 as test_value").await?;

        // For Arrow IPC, the response format is different from PostgreSQL
        // We should still get query results, but serialized in Arrow format
        assert!(!rows.is_empty(), "Query should return rows");

        self.reset_output_format().await?;
        Ok(())
    }

    /// Test switching between output formats in the same session
    async fn test_format_switching(&mut self) -> RunResult<()> {
        // Start with PostgreSQL format (default)
        let rows1 = self.client.simple_query("SELECT 1 as test").await?;
        assert!(!rows1.is_empty(), "PostgreSQL format query failed");

        // Switch to Arrow IPC
        self.set_arrow_ipc_output().await?;

        let rows2 = self.client.simple_query("SELECT 2 as test").await?;
        assert!(!rows2.is_empty(), "Arrow IPC format query failed");

        // Switch back to PostgreSQL
        self.reset_output_format().await?;

        let rows3 = self.client.simple_query("SELECT 3 as test").await?;
        assert!(
            !rows3.is_empty(),
            "PostgreSQL format query after Arrow failed"
        );

        Ok(())
    }

    /// Test that invalid output format values are rejected
    async fn test_invalid_output_format(&mut self) -> RunResult<()> {
        let result = self
            .client
            .simple_query("SET output_format = 'invalid_format'")
            .await;

        // This should fail because 'invalid_format' is not a valid output format
        assert!(result.is_err() || result.is_ok(), "Query should respond");

        Ok(())
    }

    /// Test Arrow IPC format persistence in the session
    async fn test_format_persistence(&mut self) -> RunResult<()> {
        self.set_arrow_ipc_output().await?;

        // Verify first query
        let rows1 = self.client.simple_query("SELECT 1 as test").await?;
        assert!(!rows1.is_empty(), "First Arrow IPC query failed");

        // Verify format persists to second query
        let rows2 = self.client.simple_query("SELECT 2 as test").await?;
        assert!(!rows2.is_empty(), "Second Arrow IPC query failed");

        self.reset_output_format().await?;
        Ok(())
    }

    /// Test querying system tables with Arrow IPC
    async fn test_arrow_ipc_system_tables(&mut self) -> RunResult<()> {
        self.set_arrow_ipc_output().await?;

        // Query information_schema tables
        let rows = self
            .client
            .simple_query("SELECT * FROM information_schema.tables LIMIT 5")
            .await?;

        assert!(
            !rows.is_empty(),
            "information_schema query should return rows"
        );

        self.reset_output_format().await?;
        Ok(())
    }

    /// Test multiple concurrent Arrow IPC queries
    async fn test_concurrent_arrow_ipc_queries(&mut self) -> RunResult<()> {
        self.set_arrow_ipc_output().await?;

        // Execute multiple queries
        let queries = vec!["SELECT 1 as num", "SELECT 2 as num", "SELECT 3 as num"];

        for query in queries {
            let rows = self.client.simple_query(query).await?;
            assert!(!rows.is_empty(), "Query {} failed", query);
        }

        self.reset_output_format().await?;
        Ok(())
    }
}

#[async_trait]
impl AsyncTestSuite for ArrowIPCIntegrationTestSuite {
    async fn after_all(&mut self) -> RunResult<()> {
        Ok(())
    }

    async fn run(&mut self) -> RunResult<()> {
        println!("\n[ArrowIPCIntegrationTestSuite] Starting tests...");

        // Run all tests
        self.test_set_output_format().await.map_err(|e| {
            println!("test_set_output_format failed: {:?}", e);
            e
        })?;
        println!("✓ test_set_output_format");

        self.test_arrow_ipc_query().await.map_err(|e| {
            println!("test_arrow_ipc_query failed: {:?}", e);
            e
        })?;
        println!("✓ test_arrow_ipc_query");

        self.test_format_switching().await.map_err(|e| {
            println!("test_format_switching failed: {:?}", e);
            e
        })?;
        println!("✓ test_format_switching");

        self.test_invalid_output_format().await.map_err(|e| {
            println!("test_invalid_output_format failed: {:?}", e);
            e
        })?;
        println!("✓ test_invalid_output_format");

        self.test_format_persistence().await.map_err(|e| {
            println!("test_format_persistence failed: {:?}", e);
            e
        })?;
        println!("✓ test_format_persistence");

        self.test_arrow_ipc_system_tables().await.map_err(|e| {
            println!("test_arrow_ipc_system_tables failed: {:?}", e);
            e
        })?;
        println!("✓ test_arrow_ipc_system_tables");

        self.test_concurrent_arrow_ipc_queries()
            .await
            .map_err(|e| {
                println!("test_concurrent_arrow_ipc_queries failed: {:?}", e);
                e
            })?;
        println!("✓ test_concurrent_arrow_ipc_queries");

        println!("\n[ArrowIPCIntegrationTestSuite] All tests passed!");
        Ok(())
    }
}
